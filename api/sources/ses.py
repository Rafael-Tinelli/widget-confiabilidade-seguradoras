python - <<'PY'
import json
from pathlib import Path

ins = json.loads(Path("api/v1/insurers.json").read_text(encoding="utf-8"))
items = ins.get("insurers", [])

# B1
total = len(items)
cnpj_present = sum(1 for it in items if it.get("cnpj"))

# B2
b2_block = 0
b2_matched = 0
for it in items:
    cg = (((it.get("data") or {}).get("reputation") or {}).get("consumidorGov"))
    if isinstance(cg, dict):
        b2_block += 1
        if isinstance(cg.get("match"), dict):
            b2_matched += 1

# B3
opin_true = sum(1 for it in items if (it.get("flags") or {}).get("openInsuranceParticipant") is True)

print("TOTAL:", total)
print("CNPJ preenchido:", cnpj_present)
print("B2 bloco presente:", b2_block, "| B2 matched:", b2_matched)
print("B3 OPIN true:", opin_true)

# Extra: match report
mr = Path("data/derived/consumidor_gov/match_report_insurers.json")
if mr.exists():
    rep = json.loads(mr.read_text(encoding="utf-8"))
    print("MATCH REPORT stats:", rep.get("stats"))
else:
    print("MATCH REPORT: não encontrado")
PY
