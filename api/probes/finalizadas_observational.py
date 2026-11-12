#!/usr/bin/env python3
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
from urllib.parse import urlparse

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
TARGET = "Seguros, Capitalização e Previdência"
MAX_ZIP_BYTES = 120 * 1024 * 1024
MIN_CSV_BYTES = 20_000_000
MAX_CSV_BYTES = 900 * 1024 * 1024
DOWNLOAD_PATH = re.compile(r"/pages/publicacao/externo/\d{10,24}/download\Z")
MONTH_PATTERN = re.compile(r"20\d{2}-(0[1-9]|1[0-2])\Z")


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
    parsed = urlparse(url)
    if (parsed.scheme != "https"
            or parsed.hostname != "www.consumidor.gov.br"
            or parsed.port not in (None, 443)
            or parsed.username or parsed.password
            or parsed.fragment):
        raise ObservationalError("Catálogo remoto deve ser HTTPS oficial")
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


def read_catalog(path: Path, feed_url: str | None = None,
                 http: requests.Session | None = None) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("publications"), list):
        raise ObservationalError("Catálogo local inválido")
    publications = list(data["publications"])
    if feed_url:
        url = _checked_feed_url(feed_url)
        with (http or _session()).get(url, timeout=(10, 20)) as response:
            response.raise_for_status()
            if len(response.content) > 2_000_000:
                raise ObservationalError("Catálogo JSON remoto excessivo")
            feed = response.json()
        if not isinstance(feed, dict) or not isinstance(feed.get("publications"), list):
            raise ObservationalError("Catálogo remoto não segue contrato JSON")
        publications += feed["publications"]

    selected: dict[str, dict] = {}
    for item in publications:
        if not isinstance(item, dict):
            raise ObservationalError("Entrada de catálogo não é objeto")
        month = item.get("month")
        if not isinstance(month, str) or not MONTH_PATTERN.fullmatch(month):
            raise ObservationalError("Competência inválida no catálogo")
        url = official_url(str(item.get("url") or ""))
        known = str(item.get("expected_sha256") or "").lower()
        if known and not re.fullmatch(r"[0-9a-f]{64}", known):
            raise ObservationalError("SHA-256 esperado inválido no catálogo")
        entry = {"month": month, "url": url, "expected_sha256": known}
        if month in selected and selected[month] != entry:
            raise ObservationalError(f"Publicações conflitantes para {month}")
        selected[month] = entry
    return [selected[m] for m in sorted(selected)]


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
        unresolved = 0
        csv_digest = hashlib.sha256()
        # Hash CSV feito independentemente dos registros com stream descompactado.
        with archive.open(member) as raw:
            for chunk in iter(lambda: raw.read(1 << 20), b""):
                csv_digest.update(chunk)
        with archive.open(member) as raw:
            with io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text:
                reader = csv.DictReader(text, delimiter=";", strict=True)
                if reader.fieldnames != list(COLUMNS):
                    raise ObservationalError("Schema finalizadas diferente dos 19 campos")
                for row in reader:
                    total += 1
                    if (None in row or len(row) != len(COLUMNS)
                            or any(v is None for v in row.values())):
                        raise ObservationalError(f"Linha {total} com estrutura inválida")
                    try:
                        date = datetime.strptime(row["data_finalizacao"].strip(),
                                                 "%d/%m/%Y")
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
                            or reply not in ("Sim", "Não")
                            or evaluation not in ("Não Avaliada", "Resolvida",
                                                  "Não Resolvida")):
                        raise ObservationalError(f"Enum inválido na linha {total}")
                    is_evaluated = state == "Finalizada avaliada"
                    if is_evaluated != (evaluation != "Não Avaliada"):
                        raise ObservationalError(f"Avaliação inconsistente na linha {total}")
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
                    if reply == "Sim":
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

    if total < 1 or segment_rows < 1 or (evaluated != resolved + unresolved):
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
        "unmapped_identity_status": "raw_provider_names_only_no_susep_matching",
        "consumidor_gov_base_completa_equivalence": "not_demonstrated",
        "methodology_note": "Competência por finalização, não por abertura.",
    }
    aggregate = {"meta": meta, "by_name_key_raw": dict(sorted(names.items())),
                 "by_cnpj_key_raw": {}}
    evidence = {"month": month, "rows_total": total, "rows_segment": segment_rows,
                "providers": len(names), "responded": replied, "evaluated": evaluated,
                "resolved": resolved, "unresolved": unresolved, "unassessed": segment_rows - evaluated,
                "zip_sha256": zip_digest, "csv_sha256": csv_digest.hexdigest(),
                "zip_bytes": source.stat().st_size, "csv_bytes": member.file_size,
                "source_url": url, "status": "validated_experimental",
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
    parser.add_argument("--latest-months", type=int, default=2)
    args = parser.parse_args(argv)
    if args.latest_months < 1 or args.latest_months > 12:
        raise ObservationalError("latest-months deve estar entre 1 e 12")
    destination = _safe_output(args.output_dir)
    publications = read_catalog(args.catalog, feed_url=args.feed_url)
    if not publications:
        raise ObservationalError("Nenhuma publicação oficial conhecida")
    previous: dict = {}
    if args.previous_manifest and args.previous_manifest.exists():
        root = json.loads(args.previous_manifest.read_text(encoding="utf-8"))
        previous = root.get("months", {}) if isinstance(root, dict) else {}
    manifest = {"schema": "finalizadas-observational-1",
                "status": "experimental_no_publication",
                "generated_at": utcnow(), "months": {}, "revisions": []}
    client = _session()
    for item in publications[-args.latest_months:]:
        month, url = item["month"], item["url"]
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
        is_revision = bool((known_hash and known_hash != download_digest) or
                           (previous_hash and previous_hash != download_digest))
        evidence["revision"] = is_revision
        evidence["observed_at"] = utcnow()
        if is_revision:
            manifest["revisions"].append(month)
            evidence["status"] = "requires_manual_review"
        _write_json(month_dir / "agregado-experimental.json", aggregate)
        _write_json(month_dir / "validacao.json", evidence)
        manifest["months"][month] = evidence
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
