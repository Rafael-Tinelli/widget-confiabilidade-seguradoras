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

# Desabilita warnings de SSL inseguro (necessário para OPIN muitas vezes)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configurações
OPIN_PARTICIPANTS_URL = os.getenv(
    "OPIN_PARTICIPANTS_URL", 
    "https://data.directory.opinbrasil.com.br/participants"
)

# Mapeamento: Chave interna -> Trecho da URL ou FamilyType que identifica o produto
FAMILY_KEYWORDS = {
    "auto": ["products-auto", "auto-insurance"],
    "home": ["products-residential", "residential-insurance", "housing"],
    "life": ["products-life", "life-pension", "life-insurance"],
    "patrimonial": ["products-patrimonial"],
    "travel": ["products-travel"],
}

@dataclass
class OpinMeta:
    source: str = "Open Insurance Brasil"
    as_of: str = ""
    products_count: int = 0
    families_scanned: List[str] = None

def _recursive_find_endpoints(data: Any, target_keywords: List[str]) -> str | None:
    """
    Busca forense: varre recursivamente o JSON do participante procurando 
    uma URL em 'ApiDiscoveryEndpoints' que contenha uma das palavras-chave.
    """
    if isinstance(data, dict):
        # Se achou o campo de endpoints, verifica se a URL bate
        if "ApiDiscoveryEndpoints" in data:
            endpoints = data["ApiDiscoveryEndpoints"]
            if isinstance(endpoints, list):
                for ep in endpoints:
                    if isinstance(ep, dict) and "ApiDiscoveryId" in ep:
                        url = ep.get("ApiDiscoveryId", "")
                        # Verifica se alguma keyword está na URL
                        if any(k in url.lower() for k in target_keywords):
                            return url
        
        # Continua descendo na árvore
        for key, value in data.items():
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
    """Limpa a URL de discovery para pegar a base da API."""
    # Ex: .../products-auto/v1/personal/discovery -> .../products-auto/v1/personal
    # Remove /discovery ou /open-insurance-discovery
    return re.sub(r'/(open-insurance-)?discovery$', '', discovery_url)

def _crawl_products(discovery_url: str, family_key: str) -> List[Dict]:
    """Baixa os produtos paginados de uma URL base."""
    products = []
    base_url = _get_api_base(discovery_url)
    
    # Endpoint padrão de listagem (pode variar, mas geralmente é apenas GET na base)
    target_url = base_url 
    
    print(f"    -> Crawling {family_key} em: {target_url} ...")
    
    page = 1
    total_pages = 1
    
    while page <= total_pages and page <= 20: # Limite de segurança
        try:
            # Tenta pegar a página
            resp = requests.get(
                target_url, 
                params={"page": page, "page-size": 100},
                timeout=15,
                verify=False # SSL do OPIN costuma falhar
            )
            
            if resp.status_code != 200:
                if page == 1: 
                    print(f"    -> Falha {resp.status_code} na pág 1. Ignorando.")
                break

            data = resp.json()
            
            # Normaliza estrutura de resposta (Data vs data)
            payload = data.get("data") or data.get("Data") or {}
            brand = payload.get("brand") or {}
            companies = brand.get("companies") or []
            
            for comp in companies:
                # Extrai produtos de cada empresa listada
                prods = comp.get("products") or []
                for p in prods:
                    # Salva dados essenciais
                    products.append({
                        "name": p.get("name"),
                        "code": p.get("code"),
                        "company_cnpj": comp.get("cnpjNumber"),
                        "company_name": comp.get("name"),
                        "family": family_key,
                        "coverages": [c.get("coverage") for c in p.get("coverages", [])] if "coverages" in p else []
                    })

            # Paginação
            meta = data.get("meta") or data.get("Meta") or {}
            total_pages = meta.get("totalPages") or 1
            
            if total_pages > 1:
                print(f"       Pág {page}/{total_pages} - {len(products)} produtos acumulados...")
            
            page += 1
            time.sleep(0.1) # Politeness

        except Exception as e:
            print(f"    -> Erro crawling {family_key}: {str(e)[:100]}")
            break
            
    return products

def extract_opin_products() -> tuple[OpinMeta, dict[str, list[dict]]]:
    print("OPIN: Baixando lista de participantes...")
    
    try:
        resp = requests.get(OPIN_PARTICIPANTS_URL, timeout=30, verify=False)
        resp.raise_for_status()
        participants = resp.json()
    except Exception as e:
        print(f"OPIN: Falha fatal ao baixar participantes: {e}")
        # Retorna vazio mas estruturado para não quebrar o pipeline
        return OpinMeta(warning="Falha Download Participantes"), {}

    # Filtra apenas ativos
    active_parts = [
        p for p in participants 
        if p.get("Status") == "Active" or p.get("status") == "Active"
    ]
    
    print(f"OPIN: {len(active_parts)} participantes ativos encontrados.")
    
    products_by_cnpj = {}
    total_products = 0
    
    # Para cada participante, busca URLs de cada família
    for p in active_parts:
        # Tenta achar CNPJ em vários campos
        cnpj = None
        for field in ["RegistrationNumber", "OrganisationId", "CnpjNumber"]:
            val = p.get(field)
            if val and isinstance(val, str):
                nums = re.sub(r"\D", "", val)
                if len(nums) == 14:
                    cnpj = nums
                    break
        
        if not cnpj:
            continue
        
        # Nome da empresa para log
        name = next((n.get("OrganisationName") for n in p.get("AuthorisationServers", []) if "OrganisationName" in n), p.get("OrganisationName", "Unknown"))

        # Busca produtos para este participante
        participant_products = []
        
        for family, keywords in FAMILY_KEYWORDS.items():
            # 1. Encontra a URL de Discovery para essa família
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
    return meta, products_by_cnpj
