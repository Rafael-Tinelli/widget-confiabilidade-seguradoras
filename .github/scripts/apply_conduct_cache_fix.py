from __future__ import annotations

from pathlib import Path


BUILDER = Path("api/v2/build_consumer_gov_conduct_evidence.py")
TESTS = Path("tests/test_v2_consumer_gov_conduct.py")
WORKFLOW = Path(".github/workflows/v2-consumer-gov-conduct-evidence.yml")


def patch_builder() -> None:
    text = BUILDER.read_text(encoding="utf-8")

    old_import = "from typing import Any\n\nfrom api.build_consumidor_gov import ("
    new_import = "from typing import Any\n\nimport requests\n\nfrom api.build_consumidor_gov import ("
    if old_import not in text:
        raise SystemExit("builder import anchor not found")
    text = text.replace(old_import, new_import, 1)

    old_block = '''def _ensure_raw_csvs(months: list[str]) -> dict[str, Any]:
    resources = _list_basecompleta_resources()
    missing_resources = [month for month in months if month not in resources]
    if missing_resources:
        raise RuntimeError(
            f"Consumer.gov resources missing for months: {missing_resources}"
        )

    result: dict[str, Any] = {}
    for month in months:
        resource = resources[month]
        raw_csv = RAW_DIR / f"basecompleta_{month}.csv"
        if not raw_csv.exists() or raw_csv.stat().st_size < CG_MIN_MONTH_BYTES:
            _download(resource.url, raw_csv)
        if not raw_csv.exists() or raw_csv.stat().st_size < CG_MIN_MONTH_BYTES:
            raise RuntimeError(f"invalid Consumer.gov raw CSV for {month}: {raw_csv}")
        result[month] = {
            "path": raw_csv,
            "resource_url": resource.url,
            "resource_name": resource.name,
            "bytes": raw_csv.stat().st_size,
        }
    return result
'''

    new_block = '''class TaxonomyRawSourceUnavailable(RuntimeError):
    """Required Consumer.gov Base Completa rows are unavailable for taxonomy."""


def _valid_raw_csv(path: Path) -> bool:
    return path.exists() and path.stat().st_size >= CG_MIN_MONTH_BYTES


def _ensure_raw_csvs(months: list[str]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    missing_months: list[str] = []

    for month in months:
        raw_csv = RAW_DIR / f"basecompleta_{month}.csv"
        if _valid_raw_csv(raw_csv):
            result[month] = {
                "path": raw_csv,
                "resource_url": None,
                "resource_name": None,
                "bytes": raw_csv.stat().st_size,
                "acquisition": "cache",
            }
        else:
            missing_months.append(month)

    if not missing_months:
        return result

    try:
        resources = _list_basecompleta_resources()
    except (
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        requests.RequestException,
    ) as exc:
        raise TaxonomyRawSourceUnavailable(
            "taxonomy_raw_source_unavailable: valid cached Consumer.gov Base Completa "
            f"CSVs are missing for months {missing_months}, and CKAN discovery failed: {exc}"
        ) from exc

    missing_resources = [month for month in missing_months if month not in resources]
    if missing_resources:
        raise TaxonomyRawSourceUnavailable(
            "taxonomy_raw_source_unavailable: Consumer.gov Base Completa resources "
            f"are missing for months {missing_resources}"
        )

    for month in missing_months:
        resource = resources[month]
        raw_csv = RAW_DIR / f"basecompleta_{month}.csv"
        try:
            _download(resource.url, raw_csv)
        except (OSError, RuntimeError, requests.RequestException) as exc:
            raise TaxonomyRawSourceUnavailable(
                "taxonomy_raw_source_unavailable: failed to download Consumer.gov "
                f"Base Completa for {month}: {exc}"
            ) from exc

        if not _valid_raw_csv(raw_csv):
            raise TaxonomyRawSourceUnavailable(
                "taxonomy_raw_source_unavailable: downloaded Consumer.gov Base Completa "
                f"for {month} is invalid or smaller than {CG_MIN_MONTH_BYTES} bytes"
            )

        result[month] = {
            "path": raw_csv,
            "resource_url": resource.url,
            "resource_name": resource.name,
            "bytes": raw_csv.stat().st_size,
            "acquisition": "download",
        }

    for month, metadata in result.items():
        resource = resources.get(month)
        if resource is not None:
            metadata["resource_url"] = resource.url
            metadata["resource_name"] = resource.name

    return result
'''

    if old_block not in text:
        raise SystemExit("old _ensure_raw_csvs block not found")
    BUILDER.write_text(text.replace(old_block, new_block, 1), encoding="utf-8")


def patch_tests() -> None:
    text = TESTS.read_text(encoding="utf-8")
    import_anchor = "import pytest\n\nfrom api.v2.consumer_gov_conduct import (\n"
    import_replacement = (
        "import pytest\n\n"
        "import api.v2.build_consumer_gov_conduct_evidence as conduct_builder\n"
        "from api.v2.consumer_gov_conduct import (\n"
    )
    if import_anchor not in text:
        raise SystemExit("test import anchor not found")
    text = text.replace(import_anchor, import_replacement, 1)
    text += '''


def test_raw_csv_cache_is_used_without_ckan_lookup(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(conduct_builder, "RAW_DIR", tmp_path)
    monkeypatch.setattr(conduct_builder, "CG_MIN_MONTH_BYTES", 10)
    for month in ("2026-05", "2026-06"):
        (tmp_path / f"basecompleta_{month}.csv").write_bytes(b"x" * 20)

    def fail_if_called():
        raise AssertionError("CKAN discovery must not run when every raw month is cached")

    monkeypatch.setattr(conduct_builder, "_list_basecompleta_resources", fail_if_called)

    raw = conduct_builder._ensure_raw_csvs(["2026-05", "2026-06"])

    assert set(raw) == {"2026-05", "2026-06"}
    assert raw["2026-05"]["acquisition"] == "cache"
    assert raw["2026-06"]["acquisition"] == "cache"
    assert raw["2026-05"]["resource_url"] is None


def test_missing_raw_csv_reports_taxonomy_source_unavailable(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(conduct_builder, "RAW_DIR", tmp_path)
    monkeypatch.setattr(conduct_builder, "CG_MIN_MONTH_BYTES", 10)
    (tmp_path / "basecompleta_2026-05.csv").write_bytes(b"x" * 20)

    def fail_ckan():
        raise RuntimeError("temporary CKAN failure")

    monkeypatch.setattr(conduct_builder, "_list_basecompleta_resources", fail_ckan)

    with pytest.raises(
        conduct_builder.TaxonomyRawSourceUnavailable,
        match="taxonomy_raw_source_unavailable",
    ) as exc_info:
        conduct_builder._ensure_raw_csvs(["2026-05", "2026-06"])

    assert "2026-06" in str(exc_info.value)
'''
    TESTS.write_text(text, encoding="utf-8")


def patch_workflow() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    old_lint = '''      - name: Show Ruff normalization for conduct builder
        run: |
          set -euo pipefail
          ruff check --fix api/v2/build_consumer_gov_conduct_evidence.py
          git diff -- api/v2/build_consumer_gov_conduct_evidence.py
'''
    new_lint = '''      - name: Lint conduct modules
        run: |
          set -euo pipefail
          ruff check api/v2/consumer_gov_conduct.py api/v2/build_consumer_gov_conduct_evidence.py tests/test_v2_consumer_gov_conduct.py
'''
    if old_lint not in text:
        raise SystemExit("temporary Ruff step not found")
    text = text.replace(old_lint, new_lint, 1)

    old_save = '''      - name: Save Consumer.gov raw cache
        uses: actions/cache/save@v4
        with:
          path: data/raw/consumidor_gov
          key: v2-conduct-consumer-raw-${{ runner.os }}-${{ github.run_id }}

'''
    if old_save not in text:
        raise SystemExit("early raw cache save step not found")
    text = text.replace(old_save, "", 1)

    old_eligibility = '''      - name: Build formal regulatory inventory
        run: |
          set -euo pipefail
          python -m api.v2.build_eligibility_inventory
'''
    new_eligibility = '''      - name: Build formal regulatory inventory
        run: |
          set -euo pipefail
          for attempt in 1 2 3; do
            if python -m api.v2.build_eligibility_inventory; then
              exit 0
            fi
            if [ "$attempt" -eq 3 ]; then
              echo "SUSEP inventory build failed after 3 attempts."
              exit 1
            fi
            echo "SUSEP inventory attempt $attempt failed; retrying."
            sleep $((attempt * 15))
          done
'''
    if old_eligibility not in text:
        raise SystemExit("eligibility step not found")
    text = text.replace(old_eligibility, new_eligibility, 1)

    conduct_step = '''      - name: Build Consumer.gov conduct evidence
        run: |
          set -euo pipefail
          python -m api.v2.build_consumer_gov_conduct_evidence

'''
    save_after_conduct = conduct_step + '''      - name: Save Consumer.gov raw cache after conduct acquisition
        if: always()
        uses: actions/cache/save@v4
        with:
          path: data/raw/consumidor_gov
          key: v2-conduct-consumer-raw-${{ runner.os }}-${{ github.run_id }}

'''
    if conduct_step not in text:
        raise SystemExit("conduct build step not found")
    WORKFLOW.write_text(
        text.replace(conduct_step, save_after_conduct, 1),
        encoding="utf-8",
    )


def main() -> None:
    patch_builder()
    patch_tests()
    patch_workflow()
    Path(".github/workflows/v2-conduct-ruff-debug.yml").unlink(missing_ok=True)


if __name__ == "__main__":
    main()
