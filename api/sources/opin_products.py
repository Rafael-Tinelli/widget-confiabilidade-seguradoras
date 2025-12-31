# api/sources/opin_products.py
from __future__ import annotations

import os
import time
import re
from dataclasses import dataclass
from datetime import datetime
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

class OpinResult(OpinMeta):
    """
    Objeto híbrido para compatibilidade total com build_insurers.py.
    """
    def __init__(self, meta: OpinMeta, data: dict):
        super().__init__(
            source=meta.source,
            as_of=meta.as_of,
            products_count=meta.products_count,
            families_scanned=meta.families_scanned
        )
        self._data = data

    def __iter__(self):
        yield self
        yield self._data
        
    @property
    def data(self):
        return self._data

    # --- A CORREÇÃO ESTÁ AQUI ---
    @property
    def stats(self) -> dict:
        """Retorna estatísticas para satisfazer o build_insurers.py"""
        return {
            "products_count": self.products_count,
            "families_scanned": self.families_scanned
        }

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
    
    # print(f"    -> Crawling {family_key} em: {target_url}")
    
    page = 1
    # Reduzi para 5 páginas para ser mais rápido neste teste
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

def extract_open_insurance_products() -> OpinResult:
    print("OPIN: Baixando lista de participantes...")
    
    try:
        resp = requests.get(OPIN_PARTICIPANTS_URL, timeout=30, verify=False)
        resp.raise_for_status()
        participants = resp.json()
    except Exception as e:
        print(f"OPIN: Falha fatal ao baixar participantes: {e}")
        return OpinResult(OpinMeta(warning="Falha Download Participantes"), {})

    active_parts = [
        p for p in participants 
        if str(p.get("Status", "")).lower() == "active" or str(p.get("status", "")).lower() == "active"
    ]
    
    print(f"OPIN: {len(active_parts)} participantes ativos encontrados.")
    
    products_by_cnpj = {}
    total_products = 0
    
    for p in active_parts:
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
        
        name = next((n.get("OrganisationName") for n in p.get("AuthorisationServers", []) if "OrganisationName" in n), p.get("OrganisationName", "Unknown"))

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
            total_products += len(participant_products)
            print(f"OPIN: +{len(participant_products)} produtos de {name} ({cnpj})")

    meta = OpinMeta(
        as_of=datetime.now().strftime("%Y-%m-%d"),
        products_count=total_products,
        families_scanned=list(FAMILY_KEYWORDS.keys())
    )
    
    print(f"OPIN: Total final -> {total_products} produtos coletados de {len(products_by_cnpj)} seguradoras.")
    return OpinResult(meta, products_by_cnpj)
