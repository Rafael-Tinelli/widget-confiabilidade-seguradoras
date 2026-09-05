<?php

declare(strict_types=1);

/*
 * Prepared-only HostGator bridge for v2 unknown market queries.
 *
 * This file is NOT installed or activated by the repository workflow. It stores only
 * an aggregated normalized query plus timestamps/counters. It intentionally does not
 * read or persist IP address, cookies, user identifiers, e-mail, CPF or session IDs.
 *
 * Required environment:
 *   V2_UNKNOWN_QUERY_DB_PATH=/absolute/path/outside/public_html/unknown-market.sqlite
 *   V2_UNKNOWN_QUERY_SNAPSHOT_TOKEN=<random secret used only by the admin GET snapshot>
 */

const V2_NOISE_QUERIES = [
    'seguro',
    'seguros',
    'seguradora',
    'seguradoras',
    'ranking',
    'ranking seguradoras',
    'melhor seguradora',
    'melhores seguradoras',
    'confiabilidade',
];

function json_response(int $status, array $payload): never
{
    http_response_code($status);
    header('Content-Type: application/json; charset=utf-8');
    header('Cache-Control: no-store');
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
}

function looks_like_forbidden_personal_query(string $value): bool
{
    $value = trim($value);
    if ($value === '') {
        return false;
    }
    if (str_contains($value, '@')) {
        return true;
    }
    return preg_match('/[^\s@]+@[^\s@]+\.[^\s@]+/', $value) === 1;
}

function normalize_market_query(string $value): string
{
    $value = trim($value);
    if ($value === '' || strlen($value) > 160) {
        return '';
    }

    $digits = preg_replace('/\D+/', '', $value) ?? '';
    $nonCnpj = preg_replace('/[\d.\/\-\s]+/', '', $value) ?? '';
    if (strlen($digits) === 14 && $nonCnpj === '') {
        return $digits;
    }

    if (class_exists('Transliterator')) {
        $transliterator = Transliterator::create('NFD; [:Nonspacing Mark:] Remove; NFC');
        if ($transliterator !== null) {
            $value = $transliterator->transliterate($value);
        }
    } elseif (function_exists('iconv')) {
        $converted = iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $value);
        if ($converted !== false) {
            $value = $converted;
        }
    }

    $value = strtolower($value);
    $value = preg_replace('/[^a-z0-9]+/', ' ', $value) ?? '';
    return trim(preg_replace('/\s+/', ' ', $value) ?? '');
}

function is_eligible_market_query(string $normalized): bool
{
    if ($normalized === '' || in_array($normalized, V2_NOISE_QUERIES, true)) {
        return false;
    }
    if (ctype_digit($normalized)) {
        return strlen($normalized) === 14;
    }
    return strlen($normalized) >= 3;
}

function database(): PDO
{
    $path = getenv('V2_UNKNOWN_QUERY_DB_PATH') ?: '';
    if ($path === '') {
        json_response(503, ['status' => 'unavailable', 'reason' => 'storage_not_configured']);
    }

    $directory = dirname($path);
    if (!is_dir($directory) || !is_writable($directory)) {
        json_response(503, ['status' => 'unavailable', 'reason' => 'storage_directory_unavailable']);
    }

    $pdo = new PDO('sqlite:' . $path, null, null, [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]);
    $pdo->exec('PRAGMA journal_mode=WAL');
    $pdo->exec('PRAGMA busy_timeout=5000');
    $pdo->exec(
        'CREATE TABLE IF NOT EXISTS unknown_market_queries (' .
        'normalized_query TEXT PRIMARY KEY,' .
        'first_seen TEXT NOT NULL,' .
        'last_seen TEXT NOT NULL,' .
        'last_seen_day TEXT NOT NULL,' .
        'count INTEGER NOT NULL,' .
        'distinct_day_count INTEGER NOT NULL' .
        ')'
    );
    return $pdo;
}

function require_snapshot_token(): void
{
    $expected = getenv('V2_UNKNOWN_QUERY_SNAPSHOT_TOKEN') ?: '';
    $authorization = $_SERVER['HTTP_AUTHORIZATION'] ?? '';
    $provided = str_starts_with($authorization, 'Bearer ')
        ? substr($authorization, 7)
        : '';
    if ($expected === '' || $provided === '' || !hash_equals($expected, $provided)) {
        json_response(401, ['status' => 'unauthorized']);
    }
}

$method = strtoupper($_SERVER['REQUEST_METHOD'] ?? 'GET');

if ($method === 'POST') {
    $contentType = strtolower($_SERVER['CONTENT_TYPE'] ?? '');
    if (!str_contains($contentType, 'application/json')) {
        json_response(415, ['status' => 'ignored', 'reason' => 'json_required']);
    }
    $body = file_get_contents('php://input');
    if ($body === false || strlen($body) > 2048) {
        json_response(413, ['status' => 'ignored', 'reason' => 'payload_too_large']);
    }
    $payload = json_decode($body, true);
    if (!is_array($payload) || array_keys($payload) !== ['query'] || !is_string($payload['query'])) {
        json_response(400, ['status' => 'ignored', 'reason' => 'query_only_contract_required']);
    }
    if (looks_like_forbidden_personal_query($payload['query'])) {
        json_response(202, ['status' => 'ignored', 'reason' => 'personal_query_rejected']);
    }

    $normalized = normalize_market_query($payload['query']);
    if (!is_eligible_market_query($normalized)) {
        json_response(202, ['status' => 'ignored', 'reason' => 'query_not_eligible']);
    }

    $now = new DateTimeImmutable('now', new DateTimeZone('UTC'));
    $timestamp = $now->format('Y-m-d\TH:i:s\Z');
    $day = $now->format('Y-m-d');
    $pdo = database();
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare(
            'SELECT last_seen_day, count, distinct_day_count ' .
            'FROM unknown_market_queries WHERE normalized_query = :query'
        );
        $select->execute([':query' => $normalized]);
        $existing = $select->fetch();
        if ($existing === false) {
            $insert = $pdo->prepare(
                'INSERT INTO unknown_market_queries ' .
                '(normalized_query, first_seen, last_seen, last_seen_day, count, distinct_day_count) ' .
                'VALUES (:query, :first_seen, :last_seen, :day, 1, 1)'
            );
            $insert->execute([
                ':query' => $normalized,
                ':first_seen' => $timestamp,
                ':last_seen' => $timestamp,
                ':day' => $day,
            ]);
        } else {
            $distinctDays = (int) $existing['distinct_day_count'];
            if ((string) $existing['last_seen_day'] !== $day) {
                $distinctDays++;
            }
            $update = $pdo->prepare(
                'UPDATE unknown_market_queries SET last_seen = :last_seen, last_seen_day = :day, ' .
                'count = :count, distinct_day_count = :distinct_days WHERE normalized_query = :query'
            );
            $update->execute([
                ':last_seen' => $timestamp,
                ':day' => $day,
                ':count' => ((int) $existing['count']) + 1,
                ':distinct_days' => $distinctDays,
                ':query' => $normalized,
            ]);
        }
        $pdo->commit();
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        json_response(503, ['status' => 'unavailable', 'reason' => 'aggregate_write_failed']);
    }
    json_response(202, ['status' => 'observed']);
}

if ($method === 'GET') {
    require_snapshot_token();
    $pdo = database();
    $rows = $pdo->query(
        'SELECT normalized_query, first_seen, last_seen, count, distinct_day_count ' .
        'FROM unknown_market_queries ORDER BY count DESC, normalized_query ASC'
    )->fetchAll();
    json_response(200, [
        'artifact' => 'v2_widget_unknown_market_query_snapshot',
        'privacy_contract' => 'aggregated_query_only_no_personal_or_session_identifiers',
        'queries' => $rows,
    ]);
}

header('Allow: GET, POST');
json_response(405, ['status' => 'method_not_allowed']);
