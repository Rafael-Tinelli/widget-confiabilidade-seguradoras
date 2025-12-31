# api/sources/opin_products.py
from __future__ import annotations

import os
import time
import re
import json
import gzip
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, List, Dict

import requests
import urllib3

# Desabilita warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações
OPIN_PARTICIPANTS_URL = os.getenv(
    "OPIN_PARTICIPANTS_URL", 
    "https://data.directory.opinbrasil.com.br/participants"
)

# Diretório para salvar os arquivos que o build_insurers.py exige
CACHE_DIR = Path("data/raw/opin")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

FAMILY_KEYWORDS = {
    "auto": ["products-auto", "auto-insurance", "automovel", "vehicle"],
    "home": ["products-residential", "residential-insurance", "housing", "residencial"],
    "life": ["products-life", "life-pension", "life-insurance", "vida", "person"],
    "patrimonial": ["products-patrimonial", "patrimonial"],
    "travel": ["products-travel", "travel"],
}

@dataclass
class OpinMeta:
    source: str = "Open Insurance Brasil"
    as_of: str = ""
    products_count: int = 0
    families_scanned: List[str] = None
    # Campos exigidos pelo build_insurers.py
    products_auto_file: str = ""
    products_life_file: str = ""
    products_home_file: str = ""

class OpinResult(OpinMeta):
    """
    Objeto híbrido que satisfaz TODAS as exigências do script legado.
    """
    def __init__(self, meta: OpinMeta, data: dict, stats: dict):
        super().__init__(
            source=meta.source,
            as_of=meta.as_of,
            products_count=meta.products_count,
            families_scanned=meta.families_scanned,
            products_auto_file=meta.products_auto_file,
            products_life_file=meta.products_life_file,
            products_home_file=meta.products_home_file
        )
        self._data = data
        self._stats = stats

    def __iter__(self):
        yield self
        yield self._data
        
    @property
    def data(self):
        return self._data

    @property
    def stats(self) -> dict:
        return self._stats

def _recursive_find_endpoints(data: Any, target_keywords: List[str]) -> str | None:
    if isinstance(data, dict):
        if "ApiDiscoveryEndpoints" in data:
            endpoints = data["ApiDiscoveryEndpoints"]
            if isinstance(endpoints, list):
                for ep in endpoints:
                    if isinstance(ep, dict) and "ApiDiscoveryId" in ep:
                        url = ep.get("ApiDiscoveryId", "")
                        if any(k in url.lower() for k in target_keywords):
                            return url
        
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                found = _recursive_find_endpoints(value, target_keywords)
                if found:
                    return found
            
    elif isinstance(data, list):
        for item in data:
            found = _recursive_find_endpoints(item, target_keywords)
            if found:
                return found
            
    return None

def _get_api_base(discovery_url: str) -> str:
    return re.sub(r'/(open-insurance-)?discovery.*$', '', discovery_url)

def _crawl_products(discovery_url: str, family_key: str) -> List[Dict]:
    products = []
    base_url = _get_api_base(discovery_url)
    target_url = base_url 
    
    page = 1
    # Limite conservador para garantir execução rápida
    while page <= 5: 
        try:
            resp = requests.get(
                target_url, 
                params={"page": page, "page-size": 80},
                timeout=10,
                verify=False
            )
            
            if resp.status_code != 200:
                break

            data = resp.json()
            payload = data.get("data") or data.get("Data") or {}
            brand = payload.get("brand") or {}
            companies = brand.get("companies") or []
            
            items_found = 0
            for comp in companies:
                prods = comp.get("products") or []
                for p in prods:
                    products.append({
                        "name": p.get("name", "Produto Sem Nome"),
                        "code": p.get("code", "000"),
                        "company_cnpj": comp.get("cnpjNumber", ""),
                        "company_name": comp.get("name", ""),
                        "family": family_key,
                        "coverages": [c.get("coverage") for c in p.get("coverages", [])] if "coverages" in p else []
                    })
                    items_found += 1

            meta = data.get("meta") or data.get("Meta") or {}
            total_pages = meta.get("totalPages") or 1
            
            if page >= total_pages or items_found == 0:
                break
            
            page += 1
            time.sleep(0.05)

        except Exception:
            break
            
    return products

def _save_category_file(products: List[Dict], category: str) -> str:
    """Salva lista de produtos em JSON.GZ e retorna o nome do arquivo."""
    filename = f"products_{category}.json.gz"
    filepath = CACHE_DIR / filename
    
    try:
        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False)
        return filename
    except Exception as e:
        print(f"OPIN: Erro ao salvar {filename}: {e}")
        return ""

def extract_open_insurance_products() -> OpinResult:
    print("OPIN: Baixando lista de participantes...")
    
    # 1. Busca Participantes
    try:
        resp = requests.get(OPIN_PARTICIPANTS_URL, timeout=30, verify=False)
        resp.raise_for_status()
        participants = resp.json()
    except Exception as e:
        print(f"OPIN: Falha fatal ao baixar participantes: {e}")
        # Retorna estrutura vazia válida
        empty_meta = OpinMeta(
            source="Open Insurance Brasil", products_auto_file="", products_life_file="", products_home_file=""
        )
        return OpinResult(empty_meta, {}, {"auto": 0, "life": 0, "home": 0})

    active_parts = [
        p for p in participants 
        if str(p.get("Status", "")).lower() == "active" or str(p.get("status", "")).lower() == "active"
    ]
    
    print(f"OPIN: {len(active_parts)} participantes ativos encontrados.")
    
    products_by_cnpj = {}
    all_products_flat = [] # Lista plana para separar por categoria depois
    
    # 2. Crawling
    for p in active_parts:
        # Extração CNPJ
        cnpj = None
        candidates = [
            p.get("RegistrationNumber"), 
            p.get("OrganisationId"), 
            p.get("CnpjNumber"),
            next((o.get("RegistrationId") for o in p.get("OrgDomainClaims", []) if o.get("RegistrationId")), None)
        ]
        
        for val in candidates:
            if val and isinstance(val, str):
                nums = re.sub(r"\D", "", val)
                if len(nums) == 14:
                    cnpj = nums
                    break
        
        if not cnpj:
            continue
        
        participant_products = []
        
        for family, keywords in FAMILY_KEYWORDS.items():
            discovery_url = _recursive_find_endpoints(p, keywords)
            
            if discovery_url:
                prods = _crawl_products(discovery_url, family)
                if prods:
                    participant_products.extend(prods)
        
        if participant_products:
            if cnpj not in products_by_cnpj:
                products_by_cnpj[cnpj] = []
            products_by_cnpj[cnpj].extend(participant_products)
            all_products_flat.extend(participant_products)
            # print(f"OPIN: +{len(participant_products)} produtos de {cnpj}")

    # 3. Separação e Salvamento de Arquivos (Exigência do build_insurers.py)
    stats = {}
    files_map = {}
    
    for cat in ["auto", "life", "home"]:
        # Filtra produtos da categoria
        cat_prods = [p for p in all_products_flat if p.get("family") == cat]
        
        # Salva no disco
        fname = _save_category_file(cat_prods, cat)
        
        # Registra estatísticas e nomes de arquivo
        stats[cat] = len(cat_prods)
        files_map[f"products_{cat}_file"] = fname
        
        if len(cat_prods) > 0:
            print(f"OPIN: {len(cat_prods)} produtos de {cat} salvos em {fname}.")

    total_products = len(all_products_flat)

    # 4. Construção do Retorno
    meta = OpinMeta(
        as_of=datetime.now().strftime("%Y-%m-%d"),
        products_count=total_products,
        families_scanned=list(FAMILY_KEYWORDS.keys()),
        products_auto_file=files_map.get("products_auto_file", ""),
        products_life_file=files_map.get("products_life_file", ""),
        products_home_file=files_map.get("products_home_file", "")
    )
    
    print(f"OPIN: Total final -> {total_products} produtos coletados.")
    return OpinResult(meta, products_by_cnpj, stats)
