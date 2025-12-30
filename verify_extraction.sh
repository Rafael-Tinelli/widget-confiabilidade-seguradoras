#!/bin/bash
set -e  # Encerra se houver erro

echo "========================================================"
echo "🧪 INICIANDO SMOKE TEST: EXTRAÇÃO E INTEGRAÇÃO"
echo "========================================================"

# 1. Limpeza de Artefatos Anteriores (para garantir que estamos testando o código novo)
echo "[1/5] Limpando dados antigos de teste..."
rm -f data/raw/opin/*.json.gz
rm -f api/v1/insurers.json
echo "      Limpeza concluída."

# 2. Verificar Dependências
echo "[2/5] Verificando ambiente Python..."
if ! python -c "import curl_cffi" &> /dev/null; then
    echo "❌ Erro: curl_cffi não instalado. Rode: pip install curl_cffi"
    exit 1
fi
echo "      Dependências OK."

# 3. Teste Unitário da Extração de Produtos (Isolado)
echo "[3/5] Rodando api.sources.opin_products (Download Produtos)..."
# Isso vai baixar os JSONs de Auto, Vida e Residencial
python -m api.sources.opin_products

# Verificação: Arquivos foram criados?
echo "      Verificando arquivos gerados em data/raw/opin/..."
COUNT_GZ=$(ls data/raw/opin/*.json.gz 2>/dev/null | wc -l)

if [ "$COUNT_GZ" -eq "0" ]; then
    echo "❌ FALHA: Nenhum arquivo .json.gz gerado em data/raw/opin/"
    exit 1
else
    echo "✅ SUCESSO: $COUNT_GZ arquivos de produtos gerados."
    ls -lh data/raw/opin/*.json.gz
fi

# 4. Teste de Integração (Build Completo)
echo "[4/5] Rodando api.build_insurers (Integração SUSEP + OPIN)..."
# Isso vai processar SES, Consumidor.gov e agora o OPIN Products
python -m api.build_insurers

# 5. Validação Profunda do JSON Final
echo "[5/5] Auditando o JSON final (insurers.json)..."

python - <<EOF
import json
import sys
from pathlib import Path

try:
    p = Path("api/v1/insurers.json")
    if not p.exists():
        print("❌ Arquivo insurers.json não encontrado!")
        sys.exit(1)

    data = json.loads(p.read_text(encoding="utf-8"))
    
    # 1. Verifica Metadados da Fonte
    sources = data.get("sources", {})
    opin_prod = sources.get("open_insurance_products")
    
    if not opin_prod:
        print("❌ FALHA: Chave 'open_insurance_products' não encontrada em 'sources'.")
        sys.exit(1)
        
    files = opin_prod.get("files", [])
    if len(files) < 3:
         print(f"⚠️ AVISO: Esperava 3 arquivos de produtos, encontrou {len(files)}: {files}")
    else:
         print(f"✅ Metadados OK: Referência para {len(files)} arquivos de produtos encontrada.")

    # 2. Verifica se a flag 'openInsuranceParticipant' está popular
    insurers = data.get("insurers", [])
    opin_count = sum(1 for i in insurers if i.get("flags", {}).get("openInsuranceParticipant"))
    
    print(f"✅ Estatística: {len(insurers)} seguradoras processadas.")
    print(f"✅ Integração OPIN: {opin_count} seguradoras marcadas como participantes do Open Insurance.")
    
    if opin_count == 0:
        print("❌ FALHA: Nenhuma seguradora foi marcada como participante do Open Insurance (flag=False para todas).")
        sys.exit(1)

    print("\n🎉 TUDO CERTO! O pipeline está gerando dados consistentes.")

except Exception as e:
    print(f"❌ Erro durante a validação Python: {e}")
    sys.exit(1)
EOF
