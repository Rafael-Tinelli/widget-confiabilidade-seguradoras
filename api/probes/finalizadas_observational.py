"""Aquisição e agregação OBSERVACIONAL dos ZIPs mensais finalizadas.

Isolado de data/raw, data/derived, Gate 4, Receita e publicação. O catálogo
contém somente URLs diretas oficiais; não há scraping, Selenium ou descoberta
de identificadores por suposição. --feed-url aceita um futuro catálogo JSON
estruturado, quando seu endpoint oficial estiver comprovado.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import tempfile
import unicodedata
import zipfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

COLUMNS = (
    "regiao", "uf", "cidade", "sexo", "faixa_etaria", "data_finalizacao",
    "tempo_resposta", "nome_fantasia", "segmento_de_mercado", "area",
    "assunto", "grupo_problema", "problema", "como_comprou_contratou",
    "procurou_empresa", "respondida", "situacao", "avaliacao_reclamacao",
    "nota_do_consumidor",
)
LEGACY_COLUMNS = (
    "Região", "UF", "Cidade", "Sexo", "Faixa Etária", "Data Finalização",
    "Tempo Resposta", "Nome Fantasia", "Segmento de Mercado", "Área",
    "Assunto", "Grupo Problema", "Problema", "Como Comprou Contratou",
    "Procurou Empresa", "Respondida", "Situação", "Avaliação Reclamação",
    "Nota do Consumidor",
)
TARGET = "Seguros, Capitalização e Previdência"
MAX_ZIP_BYTES = 120 * 1024 * 1024
MIN_CSV_BYTES = 20_000_000
MAX_CSV_BYTES = 900 * 1024 * 1024
DOWNLOAD_PATH = re.compile(r"/pages/publicacao/externo/\d{10,24}/download\Z")
MONTH_PATTERN = re.compile(r"20\d{2}-(0[1-9]|1[0-2])\Z")
OFFICIAL_CATALOG_URL = (
    "https://www.consumidor.gov.br/pages/publicacao/externo/"
    "publicacoes.json?indicadorTipoPublicacao=2"
)
OFFICIAL_CATALOG_PATH = "/pages/publicacao/externo/publicacoes.json"
FINALIZADAS_NAME = re.compile(r"finalizadas_(20\d{2}-(?:0[1-9]|1[0-2]))\.zip\Z")
MAX_CATALOG_BYTES = 2_000_000


class ObservationalError(RuntimeError):
    """Não promover resultados quando não há evidência suficiente."""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_name(name: str) -> str:
    text = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in text if not unicodedata.combining(c)).lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()


def simple_norm(text: str) -> str:
    return " ".join(text.strip().lower().split())


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def official_url(url: str) -> str:
    parsed = urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.consumidor.gov.br"
        or parsed.port not in (None, 443)
        or parsed.username or parsed.password or parsed.query or parsed.fragment
        or not DOWNLOAD_PATH.fullmatch(parsed.path)
    ):
        raise ObservationalError("URL não é download direto oficial permitido")
    return url


def _checked_feed_url(url: str) -> str:
    """Apenas o endpoint JSON oficial identificado no tráfego do portal."""
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)
    if (
        parsed.scheme != "https"
        or parsed.hostname != "www.consumidor.gov.br"
        or parsed.port not in (None, 443)
        or parsed.username or parsed.password or parsed.fragment
        or parsed.path != OFFICIAL_CATALOG_PATH
        or query != {"indicadorTipoPublicacao": ["2"]}
    ):
        raise ObservationalError("Feed deve ser o JSON oficial de publicações (tipo 2)")
    return url


def _session() -> requests.Session:
    retry = Retry(total=2, connect=2, read=2, status=2, backoff_factor=0.7,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=frozenset({"GET"}), respect_retry_after_header=True)
    http = requests.Session()
    http.mount("https://", HTTPAdapter(max_retries=retry))
    http.headers.update({"User-Agent": "sanida-finalizadas-observational/1.0",
                         "Accept": "application/zip,application/json,*/*;q=0.1"})
    return http


def _official_publications(payload: object) -> list[dict]:
    """Converte a LISTA JSON oficial, sem parsing de HTML ou suposição de IDs."""
    if not isinstance(payload, list):
        raise ObservationalError("Feed oficial não retornou uma lista JSON")
    publications: list[dict] = []
    for item in payload:
        if not isinstance(item, dict):
            raise ObservationalError("Publicação oficial não é um objeto")
        filename = item.get("nomeArquivo")
        if not isinstance(filename, str):
            continue
        match = FINALIZADAS_NAME.fullmatch(filename)
        if not match:
            continue
        if item.get("indicadorExclusaoLogica") is not False:
            publications.append({
                "month": match.group(1), "withdrawn": True,
                "discovery": "official_publicacoes_json",
            })
            continue
        code = str(item.get("codigo") or "")
        if not re.fullmatch(r"\d{10,24}", code):
            raise ObservationalError(f"Código inválido para {filename}")
        size = item.get("tamanhoArquivo")
        if type(size) is not int or not 1024 <= size <= MAX_ZIP_BYTES:
            raise ObservationalError(f"Tamanho anunciado inválido para {filename}")
        url = official_url(
            f"https://www.consumidor.gov.br/pages/publicacao/externo/{code}/download"
        )
        publications.append({
            "month": match.group(1), "url": url, "expected_sha256": "",
            "published_at": str(item.get("dataPublicacao") or ""),
            "modified_at": str(item.get("dataAlteracao") or ""),
            "publication_code": code, "announced_bytes": size,
            "discovery": "official_publicacoes_json",
        })
    if not publications:
        raise ObservationalError("Feed oficial não contém arquivos finalizadas")
    return publications


def _fetch_official_feed(url: str, http: requests.Session) -> list[dict]:
    # A consulta não usa cookies enviados pelo usuário ou cabeçalhos de navegador.
    with http.get(
        _checked_feed_url(url), timeout=(12, 50), stream=True,
        allow_redirects=False,
        params={"_": int(datetime.now(timezone.utc).timestamp() * 1000)},
        headers={
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.consumidor.gov.br/pages/dadosabertos/externo/",
        },
    ) as response:
        if response.is_redirect:
            raise ObservationalError("Redirect proibido no catálogo oficial")
        response.raise_for_status()
        if "application/json" not in response.headers.get("Content-Type", "").lower():
            raise ObservationalError("Feed oficial não retornou Content-Type JSON")
        chunks: list[bytes] = []
        consumed = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            consumed += len(chunk)
            if consumed > MAX_CATALOG_BYTES:
                raise ObservationalError("JSON oficial excedeu limite de bytes")
            chunks.append(chunk)
        if not consumed:
            raise ObservationalError("Feed oficial vazio")
        encoding = response.encoding or "utf-8"
        try:
            payload = json.loads(b"".join(chunks).decode(encoding))
        except (LookupError, UnicodeError, ValueError) as exc:
            raise ObservationalError("Feed oficial não contém JSON válido") from exc
    return _official_publications(payload)


def read_catalog(path: Path, feed_url: str | None = None,
                 http: requests.Session | None = None) -> list[dict]:
    """Mescla catálogo local fixado e descobertas oficiais; conflito falha fechado."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("publications"), list):
        raise ObservationalError("Catálogo local inválido")

    known: dict[str, dict] = {}
    for item in data["publications"]:
        if not isinstance(item, dict):
            raise ObservationalError("Entrada local não é objeto")
        month = item.get("month")
        if not isinstance(month, str) or not MONTH_PATTERN.fullmatch(month):
            raise ObservationalError("Competência inválida no catálogo local")
        url = official_url(str(item.get("url") or ""))
        checksum = str(item.get("expected_sha256") or "").lower()
        if checksum and not re.fullmatch(r"[0-9a-f]{64}", checksum):
            raise ObservationalError("SHA-256 esperado inválido no catálogo local")
        candidate = {"month": month, "url": url, "expected_sha256": checksum,
                     "discovery": "pinned_official_url"}
        if month in known and known[month] != candidate:
            raise ObservationalError(f"Mesma competência local com URLs diferentes: {month}")
        known[month] = candidate

    discovered: dict[str, dict] = {}
    if feed_url:
        for entry in _fetch_official_feed(feed_url, http or _session()):
            month = entry["month"]
            if month in discovered and discovered[month] != entry:
                if discovered[month].get("url") != entry.get("url"):
                    raise ObservationalError(
                        f"Mais de uma publicação oficial para {month}"
                    )
                raise ObservationalError(
                    f"Metadados oficiais conflitantes para a mesma competência: {month}"
                )
            discovered[month] = entry
        for month, entry in discovered.items():
            pinned = known.get(month)
            if entry.get("withdrawn"):
                if not pinned:
                    continue
                known[month] = {
                    **(pinned or {"month": month, "expected_sha256": ""}),
                    "catalog_status": "withdrawn",
                    "discovery": "official_publicacoes_json",
                }
                continue
            if pinned and pinned["url"] != entry["url"]:
                raise ObservationalError(
                    f"Publicação oficial diverge da origem fixada para {month}"
                )
            if pinned:
                entry["expected_sha256"] = pinned["expected_sha256"]
            entry["catalog_status"] = "listed"
            known[month] = entry
        for month in known.keys() - discovered.keys():
            known[month]["catalog_status"] = "absent_from_feed"

    if not known:
        raise ObservationalError("Nenhuma publicação oficial conhecida")
    return [known[m] for m in sorted(known)]

def download_direct(url: str, destination: Path,
                    http: requests.Session | None = None) -> str:
    """Baixa em arquivo temporário, limita bytes e valida URL após redirect."""
    official_url(url)
    client = http or _session()
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".download-", suffix=".part",
                              dir=destination.parent)
    os.close(fd)
    try:
        # Redirect não pode levar os dados a outro host.
        with client.get(url, stream=True, timeout=(12, 45),
                        allow_redirects=False) as response:
            if response.is_redirect:
                raise ObservationalError("Redirect não permitido no download oficial")
            response.raise_for_status()
            length = response.headers.get("Content-Length")
            if length is not None and int(length) > MAX_ZIP_BYTES:
                raise ObservationalError("Content-Length excede limite")
            digest = hashlib.sha256()
            received = 0
            with open(tmp, "wb") as target:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    if not chunk:
                        continue
                    received += len(chunk)
                    if received > MAX_ZIP_BYTES:
                        raise ObservationalError("Download excedeu limite em bytes")
                    target.write(chunk)
                    digest.update(chunk)
                target.flush()
                os.fsync(target.fileno())
            if length is not None and int(length) != received:
                raise ObservationalError("Download incompleto: Content-Length divergente")
            if received < 1024:
                raise ObservationalError("Download vazio ou inesperadamente pequeno")
            with open(tmp, "rb") as target:
                if target.read(4) != b"PK\x03\x04":
                    raise ObservationalError("Resposta não contém arquivo ZIP")
        os.replace(tmp, destination)
        return digest.hexdigest()
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def empty_stats() -> dict:
    return {
        "complaintsCount": 0, "respondedCount": 0, "resolvedCount": 0,
        "finalizedCount": 0, "scoreSum": 0.0, "satisfactionCount": 0,
        "total_claims": 0, "responded_claims": 0, "resolved_claims": 0,
        "finalized_claims": 0,
    }


def aggregate_zip(source: Path, month: str, url: str) -> tuple[dict, dict]:
    """Porta o processador homologado de outubro; nenhum dado é publicado."""
    if not MONTH_PATTERN.fullmatch(month):
        raise ObservationalError("Competência inválida")
    if source.stat().st_size > MAX_ZIP_BYTES:
        raise ObservationalError("ZIP acima do limite")
    zip_digest = sha256(source)
    with zipfile.ZipFile(source) as archive:
        members = [m for m in archive.infolist() if not m.is_dir()]
        if len(members) != 1 or members[0].filename != f"finalizadas_{month}.csv":
            raise ObservationalError("ZIP deve conter um único CSV do mês declarado")
        member = members[0]
        if (member.flag_bits & 1 or member.file_size > MAX_CSV_BYTES
                or member.file_size < MIN_CSV_BYTES or archive.testzip() is not None):
            raise ObservationalError("ZIP criptografado, corrompido ou fora dos limites")
        names: dict[str, dict] = {}
        displays: dict[str, Counter] = defaultdict(Counter)
        total = segment_rows = replied = evaluated = resolved = 0
        unresolved = unknown_resolution = 0
        csv_digest = hashlib.sha256()
        # Hash CSV feito independentemente dos registros com stream descompactado.
        with archive.open(member) as raw:
            for chunk in iter(lambda: raw.read(1 << 20), b""):
                csv_digest.update(chunk)
        with (archive.open(member) as raw,
              io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text):
            reader = csv.DictReader(text, delimiter=";", strict=True)
            if reader.fieldnames == list(COLUMNS):
                schema = "canonical_snake_19"
                incoming = COLUMNS
            elif reader.fieldnames == list(LEGACY_COLUMNS):
                schema = "legacy_portuguese_19"
                incoming = LEGACY_COLUMNS
            else:
                raise ObservationalError("Schema finalizadas não corresponde aos dois formatos oficiais conhecidos")
            for row in reader:
                total += 1
                if (None in row or len(row) != len(COLUMNS)
                        or any(v is None for v in row.values())):
                    raise ObservationalError(f"Linha {total} com estrutura inválida")
                row = {field: row[incoming[i]] for i, field in enumerate(COLUMNS)}
                raw_date = row["data_finalizacao"].strip()
                date_format = "%Y-%m-%d" if re.fullmatch(r"20\d{2}-\d{2}-\d{2}", raw_date) else "%d/%m/%Y"
                try:
                    date = datetime.strptime(raw_date, date_format).replace(tzinfo=timezone.utc)
                except ValueError as exc:
                    raise ObservationalError(f"Data inválida na linha {total}") from exc
                if date.strftime("%Y-%m") != month:
                    raise ObservationalError(f"Competência inesperada na linha {total}")
                if simple_norm(row["segmento_de_mercado"]) != simple_norm(TARGET):
                    continue
                segment_rows += 1
                state = row["situacao"].strip()
                reply = row["respondida"].strip()
                evaluation = row["avaliacao_reclamacao"].strip()
                score_raw = row["nota_do_consumidor"].strip()
                if (state not in ("Finalizada avaliada", "Finalizada não avaliada")
                        or reply not in (("Sim", "Não") if schema == "canonical_snake_19" else ("S", "N"))
                        or evaluation not in ("Não Avaliada", "Resolvida",
                                              "Não Resolvida", "")):
                    raise ObservationalError(f"Enum inválido na linha {total}")
                is_evaluated = state == "Finalizada avaliada"
                if ((is_evaluated and evaluation == "Não Avaliada")
                        or (not is_evaluated and evaluation != "Não Avaliada")):
                    raise ObservationalError(f"Avaliação inconsistente na linha {total}")
                if is_evaluated and evaluation == "":
                    # Fonte oficial traz nota sem resposta sobre resolução:
                    # registrar a ausência; nunca imputar 'resolvida'.
                    unknown_resolution += 1
                score = 0.0
                if is_evaluated:
                    try:
                        score = float(score_raw.replace(",", "."))
                    except ValueError as exc:
                        raise ObservationalError(f"Nota inválida na linha {total}") from exc
                    if not math.isfinite(score) or score not in (1, 2, 3, 4, 5):
                        raise ObservationalError(f"Nota fora do domínio na linha {total}")
                    evaluated += 1
                elif score_raw:
                    raise ObservationalError(f"Nota não avaliada na linha {total}")
                name = row["nome_fantasia"].strip()
                key = normalize_name(name)
                if not key:
                    raise ObservationalError(f"Nome vazio na linha {total}")
                if key not in names:
                    names[key] = {"name": name, "display_name": name,
                                  "cnpj": "", "statistics": empty_stats()}
                displays[key][name] += 1
                current_display = names[key]["display_name"]
                if displays[key][name] > displays[key][current_display]:
                    names[key]["name"] = names[key]["display_name"] = name
                stats = names[key]["statistics"]
                stats["complaintsCount"] += 1
                stats["total_claims"] += 1
                stats["finalizedCount"] += 1
                stats["finalized_claims"] += 1
                if reply in ("Sim", "S"):
                    replied += 1
                    stats["respondedCount"] += 1
                    stats["responded_claims"] += 1
                if evaluation == "Resolvida":
                    resolved += 1
                    stats["resolvedCount"] += 1
                    stats["resolved_claims"] += 1
                if evaluation == "Não Resolvida":
                    unresolved += 1
                if score > 0:
                    stats["scoreSum"] += score
                    stats["satisfactionCount"] += 1

    if total < 1 or segment_rows < 1 or (evaluated != resolved + unresolved + unknown_resolution):
        raise ObservationalError("Contagens de origem insuficientes ou contraditórias")
    if segment_rows != sum(v["statistics"]["complaintsCount"] for v in names.values()):
        raise ObservationalError("Reconciliacão do segmento por fornecedor falhou")
    if replied != sum(v["statistics"]["respondedCount"] for v in names.values()):
        raise ObservationalError("Reconciliacão de respostas por fornecedor falhou")
    for item in names.values():
        stats = item["statistics"]
        count = stats["satisfactionCount"]
        stats["averageScore"] = round(stats["scoreSum"] / count, 4) if count else 0.0
    meta = {
        "status": "experimental", "publication_status": "isolated_not_for_publication",
        "source_family": "finalizadas", "reference_event": "finalizacao",
        "month": month, "source_url": url, "source_member": member.filename,
        "source_zip_sha256": zip_digest, "source_csv_sha256": csv_digest.hexdigest(),
        "zip_bytes": source.stat().st_size, "csv_bytes": member.file_size,
        "filter_segment": TARGET, "lines_total": total, "lines_kept": segment_rows,
        "companies": len(names),
        "source_schema": schema, "resolution_unknown_count": unknown_resolution,
        "unmapped_identity_status": "raw_provider_names_only_no_susep_matching",
        "consumidor_gov_base_completa_equivalence": "not_demonstrated",
        "methodology_note": "Competência por finalização, não por abertura.",
    }
    aggregate = {"meta": meta, "by_name_key_raw": dict(sorted(names.items())),
                 "by_cnpj_key_raw": {}}
    evidence = {"month": month, "rows_total": total, "rows_segment": segment_rows,
                "providers": len(names), "responded": replied, "evaluated": evaluated,
                "source_schema": schema, "resolution_unknown": unknown_resolution,
                "resolved": resolved, "unresolved": unresolved, "unassessed": segment_rows - evaluated,
                "zip_sha256": zip_digest, "csv_sha256": csv_digest.hexdigest(),
                "zip_bytes": source.stat().st_size, "csv_bytes": member.file_size,
                "source_url": url,
                "status": ("requires_manual_review" if unknown_resolution else "validated_experimental"),
                "reference_event": "finalizacao"}
    return aggregate, evidence


def _safe_output(out: Path) -> Path:
    destination = out.resolve()
    root = Path.cwd().resolve()
    protected = ("data/raw", "data/derived", "data/cache", "public",
                 "site", "web", ".github")
    for name in protected:
        path = (root / name).resolve()
        if destination == path or path in destination.parents:
            raise ObservationalError("Destino pertence à área de produção/protegida")
    return destination


def _write_json(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".observational-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as output:
            json.dump(content, output, sort_keys=True, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--feed-url", default=None)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--previous-manifest", type=Path, default=None)
    parser.add_argument("--recovery-report", type=Path, default=None)
    parser.add_argument("--latest-months", type=int, default=2)
    args = parser.parse_args(argv)
    if args.latest_months < 1 or args.latest_months > 12:
        raise ObservationalError("latest-months deve estar entre 1 e 12")
    destination = _safe_output(args.output_dir)
    publications = read_catalog(args.catalog, feed_url=args.feed_url)
    if not publications:
        raise ObservationalError("Nenhuma publicação oficial conhecida")
    recovery_status = None
    if args.recovery_report:
        recovery = json.loads(args.recovery_report.read_text(encoding="utf-8"))
        recovery_status = recovery.get("status") if isinstance(recovery, dict) else None
        if recovery_status not in ("bootstrap", "restored", "restored_with_gap"):
            raise ObservationalError("Relatório de recuperação inválido")
        if (recovery_status == "bootstrap") == bool(args.previous_manifest):
            raise ObservationalError("Manifesto e recuperação não correspondem")
        if args.previous_manifest:
            from api.probes.finalizadas_history import digest
            if recovery.get("manifest_sha256") != digest(args.previous_manifest):
                raise ObservationalError("Hash do manifesto restaurado divergente")
    previous: dict = {}
    previous_quality_alerts: set[str] = set()
    if args.previous_manifest:
        if not args.previous_manifest.is_file():
            raise ObservationalError("Manifesto anterior informado está indisponível")
        root = json.loads(args.previous_manifest.read_text(encoding="utf-8"))
        if (not isinstance(root, dict)
                or root.get("schema") != "finalizadas-observational-1"
                or not isinstance(root.get("months"), dict)):
            raise ObservationalError("Manifesto anterior inválido ou incompatível")
        previous = root["months"]
        previous_quality_alerts = set(root.get("quality_alerts", []))
        for month, prior in previous.items():
            if (not isinstance(month, str) or not MONTH_PATTERN.fullmatch(month)
                    or not isinstance(prior, dict)
                    or (prior.get("zip_sha256") is not None
                        and not re.fullmatch(
                            r"[0-9a-f]{64}", str(prior["zip_sha256"])
                        ))):
                raise ObservationalError("Histórico de revisão anterior inválido")
    manifest = {"schema": "finalizadas-observational-1",
                "status": "experimental_no_publication",
                "generated_at": utcnow(),
                "baseline_state": recovery_status or (
                    "restored" if args.previous_manifest else "unavailable"
                ),
                "months": {m: dict(p) for m, p in previous.items()},
                "revisions": [], "quality_alerts": []}
    if recovery_status == "restored_with_gap":
        # The missing successful run may have touched ANY inherited month,
        # including months outside the current two-month reprocessing window.
        for state in manifest["months"].values():
            state["status"] = "requires_manual_review"
            state["history_gap"] = True
    client = _session()
    for item in publications[-args.latest_months:]:
        month = item["month"]
        if item.get("catalog_status") in ("withdrawn", "absent_from_feed"):
            prior = dict(previous.get(month, {}))
            prior["month"] = month
            prior["catalog_status"] = item["catalog_status"]
            prior["status"] = "requires_manual_review"
            prior["revision"] = True
            manifest["months"][month] = prior
            manifest["revisions"].append(month)
            continue
        url = item["url"]
        month_dir = destination / month
        month_dir.mkdir(parents=True, exist_ok=True)
        zip_path = month_dir / f"finalizadas_{month}.zip"
        download_digest = download_direct(url, zip_path, http=client)
        aggregate, evidence = aggregate_zip(zip_path, month, url)
        if evidence["zip_sha256"] != download_digest:
            raise ObservationalError("Hash mudou entre aquisição e processamento")
        prior = previous.get(month, {})
        previous_hash = prior.get("zip_sha256") if isinstance(prior, dict) else None
        known_hash = item["expected_sha256"]
        previous_code = prior.get("publication_code") if isinstance(prior, dict) else None
        previous_url = prior.get("source_url") if isinstance(prior, dict) else None
        announced = item.get("announced_bytes")
        # A revisão pendente permanece pendente mesmo se os mesmos bytes
        # forem observados novamente; observar não equivale a aprovar.
        prior_pending = prior.get("status") == "requires_manual_review"
        history_gap = bool(prior.get("history_gap")
                           or recovery_status == "restored_with_gap")
        # A source revision and a pending manual review are not synonyms:
        # e.g. the official September data has an unknown resolution but
        # no ZIP revision. Missing history is a third, distinct reason.
        metadata_changed = any(
            prior.get(field) not in (None, "")
            and item.get(field) not in (None, "")
            and prior[field] != item[field]
            for field in ("published_at", "modified_at", "announced_bytes")
        )
        is_revision = bool(
            prior.get("revision")
            or (known_hash and known_hash != download_digest)
            or (previous_hash and previous_hash != download_digest)
            or (previous_code and previous_code != item.get("publication_code"))
            or (previous_url and previous_url != url)
            or (announced and announced != evidence["zip_bytes"])
            or metadata_changed
        )
        requires_review = bool(prior_pending or history_gap or is_revision
                               or evidence.get("resolution_unknown"))
        if prior_pending or is_revision:
            origin = prior.get("review_origin_sha256")
            if origin or (is_revision and previous_hash):
                evidence["review_origin_sha256"] = origin or previous_hash
        evidence["publication_code"] = item.get("publication_code")
        evidence["published_at"] = item.get("published_at")
        evidence["modified_at"] = item.get("modified_at")
        evidence["announced_bytes"] = announced
        evidence["discovery"] = item.get("discovery")
        evidence["catalog_status"] = item.get("catalog_status", "pinned_only")
        evidence["revision"] = is_revision
        evidence["history_gap"] = history_gap
        evidence["observed_at"] = utcnow()
        if evidence.get("resolution_unknown"):
            manifest["quality_alerts"].append(month)
        if is_revision:
            manifest["revisions"].append(month)
        if requires_review:
            evidence["status"] = "requires_manual_review"
        _write_json(month_dir / "agregado-experimental.json", aggregate)
        _write_json(month_dir / "validacao.json", evidence)
        manifest["months"][month] = evidence
    manifest["revisions"] = sorted(set(manifest["revisions"]) | {
        month for month, state in manifest["months"].items()
        if state.get("revision") is True
    })
    manifest["quality_alerts"] = sorted(
        set(manifest["quality_alerts"]) | previous_quality_alerts | {
            month for month, state in manifest["months"].items()
            if state.get("resolution_unknown")
        }
    )
    _write_json(destination / "manifest.json", manifest)
    print(json.dumps({"status": manifest["status"], "months": sorted(manifest["months"]),
                      "revisions": manifest["revisions"], "output_dir": str(destination)},
                     ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ObservationalError, zipfile.BadZipFile, csv.Error, UnicodeError,
            requests.RequestException, OSError, ValueError) as exc:
        print(f"OBSERVATIONAL_FAILURE: {type(exc).__name__}: {exc}", file=__import__("sys").stderr)
        raise SystemExit(1) from exc
