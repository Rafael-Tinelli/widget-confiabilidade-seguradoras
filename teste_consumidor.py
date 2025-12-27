from api.sources.consumidor_gov import discover_basecompleta_urls, download_csv_to_gz, aggregate_month
import json

def test():
    # 1. Descobrir URLs (Pega só 1 mês para teste rápido)
    urls = discover_basecompleta_urls(months=1)
    if not urls:
        print("Nenhuma URL encontrada.")
        return

    ym, url = list(urls.items())[0]
    print(f"Testando mês: {ym} -> {url}")

    # 2. Baixar
    gz_path = f"test_consumidor_{ym}.csv.gz"
    download_csv_to_gz(url, gz_path)

    # 3. Agregar
    aggs = aggregate_month(gz_path)

    # 4. Mostrar TOP 5 empresas com mais reclamações
    sorted_aggs = sorted(aggs.values(), key=lambda x: x.total, reverse=True)[:5]
    
    print("\n--- TOP 5 EMPRESAS (TESTE) ---")
    for a in sorted_aggs:
        print(json.dumps(a.to_public(), indent=2, ensure_ascii=False))

if __name__ == "__main__":
    test()
