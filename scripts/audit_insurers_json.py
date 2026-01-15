import json
import math
import sys
from pathlib import Path

REQUIRED_FLOATS = [
    ("data", "premiums"),
    ("data", "claims"),
    ("data", "net_worth"),
]

def get_nested(d, *keys):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def is_finite_number(x):
    return isinstance(x, (int, float)) and math.isfinite(float(x))

def main():
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("api/v1/insurers.json")
    if not path.exists():
        path = Path("insurers.json")
    data = json.loads(path.read_text(encoding="utf-8"))

    insurers = data.get("insurers") or []
    if not insurers:
        raise SystemExit("ERRO: insurers vazio.")

    missing = []
    weird = []

    n_fin_nonzero = 0
    for ins in insurers:
        # contrato mínimo
        for a, b in REQUIRED_FLOATS:
            v = get_nested(ins, a, b)
            if not is_finite_number(v):
                missing.append((ins.get("id"), f"{a}.{b}", v))

        premiums = float(get_nested(ins, "data", "premiums") or 0.0)
        claims = float(get_nested(ins, "data", "claims") or 0.0)

        if premiums > 0 or claims > 0:
            n_fin_nonzero += 1

        # anomalias
        if premiums < 0 or claims < 0:
            weird.append((ins.get("id"), "negativo", premiums, claims))

        if premiums > 0:
            lr = claims / premiums
            if lr < 0 or lr > 5:  # limiar de auditoria, não “regra de negócio”
                weird.append((ins.get("id"), "loss_ratio_extremo", lr, premiums, claims))

    print(f"OK: arquivo={path}")
    print(f"insurers={len(insurers)} | financial_nonzero={n_fin_nonzero}")
    print(f"missing_required={len(missing)} | weird={len(weird)}")

    if missing[:20]:
        print("\n-- Missing (amostra) --")
        for row in missing[:20]:
            print(row)

    if weird[:20]:
        print("\n-- Weird (amostra) --")
        for row in weird[:20]:
            print(row)

    # Falha CI se contrato mínimo quebrar
    if missing:
        raise SystemExit(2)

if __name__ == "__main__":
    main()
