def extract_ses_master_and_financials(
    zip_url: Optional[str] = None,
) -> Tuple[SesExtractionMeta, Dict[str, Dict[str, Any]]]:
    if zip_url:
        print(f"SES: zip_url={zip_url} informado, mas ignorado em favor do crawler Playwright.")

    zip_path, used_url = _download_zip_via_browser()

    try:
        with zipfile.ZipFile(zip_path) as z:
            all_files = z.namelist()
            print(f"DEBUG: ZIP Contents: {all_files}")
            
            csvs = [n for n in all_files if n.lower().endswith(".csv")]

            cias_candidates = [n for n in csvs if "cias" in n.lower() or "cadast" in n.lower()]
            seg_candidates = [n for n in csvs if "seguros" in n.lower() or "finan" in n.lower()]

            if not cias_candidates or not seg_candidates:
                print("WARNING: Nomes de arquivo padrão não encontrados. Testando todos os CSVs.")
                cias_candidates = csvs
                seg_candidates = csvs

            # Seleção inteligente baseada no header
            cias = _pick_best_csv(
                z,
                cias_candidates,
                required_groups=[
                    # Grupo 1: ID da empresa
                    ["cod_enti", "coenti", "cod_cia", "co_enti", "codigo"],
                    # Grupo 2: Nome da empresa
                    ["noenti", "nome", "nome_cia", "razao_social"],
                ],
            )

            seguros = _pick_best_csv(
                z,
                seg_candidates,
                required_groups=[
                    # Grupo 1: ID da empresa
                    ["cod_enti", "coenti", "cod_cia", "co_enti"],
                    # Grupo 2: Data (Ano/Mês)
                    ["damesano", "anomes", "competencia", "damesaano"],
                    # Grupo 3: Prêmios (Adicionados variantes descobertas no log)
                    ["premio", "premio_emitido", "premios", "premio_direto", "premio_de_seguros"],
                ],
            )

            if not cias or not seguros:
                raise RuntimeError(f"CSVs válidos não identificados. Conteúdo: {all_files}")

            print(f"SELECTED CIAS: {cias}")
            print(f"SELECTED SEGUROS: {seguros}")

            rows_cias = _read_csv(z, cias)
            rows_seguros = _read_csv(z, seguros)

            h_cias = [_norm(x) for x in rows_cias[0]]
            h_seg = [_norm(x) for x in rows_seguros[0]]

            print(f"DEBUG HEADERS CIAS: {h_cias}")
            print(f"DEBUG HEADERS SEGUROS: {h_seg}")

            def g_idx(h: list[str], keys: list[str]) -> Optional[int]:
                for k in keys:
                    nk = _norm(k)
                    if nk in h:
                        return h.index(nk)
                return None

            id_i = g_idx(h_cias, ["cod_enti", "coenti", "cod_cia", "co_enti", "codigo"])
            nm_i = g_idx(h_cias, ["noenti", "nome", "nome_cia", "no_enti", "razao_social"])
            cn_i = g_idx(h_cias, ["cnpj", "numcnpj", "nu_cnpj"])

            sid_i = g_idx(h_seg, ["cod_enti", "coenti", "cod_cia", "co_enti", "codigo"])
            ym_i = g_idx(h_seg, ["damesano", "anomes", "competencia", "damesaano"])
            
            # Atualiza busca de colunas de Prêmios com os novos nomes
            pr_i = g_idx(h_seg, ["premio", "premio_emitido", "premios", "premio_direto", "premio_de_seguros"])
            
            # Sinistros (mantém variantes comuns)
            sn_i = g_idx(h_seg, ["sinistros", "sinistro", "sinistros_ocorridos", "sinistro_direto", "sinistro_ocorrido"])
            
            # Validação crítica para não retornar 0 silenciosamente
            if sid_i is None or ym_i is None or pr_i is None:
                raise RuntimeError(
                    f"Colunas obrigatórias ausentes em '{seguros}'. "
                    f"Indices encontrados: id={sid_i}, date={ym_i}, prem={pr_i}. "
                    f"Headers disponíveis: {h_seg}"
                )

            companies: Dict[str, Dict[str, Any]] = {}
            if id_i is not None and nm_i is not None:
                for row in rows_cias[1:]:
                    if len(row) <= max(id_i, nm_i):
                        continue
                    sid = _digits(row[id_i])
                    if not sid:
                        continue
                    cn = None
                    if cn_i is not None and len(row) > cn_i:
                        cn = _digits(row[cn_i])
                    companies[sid.zfill(6)] = {"name": row[nm_i].strip(), "cnpj": cn}

            agg: Dict[str, Dict[str, float]] = {}
            max_ym = 0

            for row in rows_seguros[1:]:
                # Check de segurança por linha
                if len(row) <= max(sid_i, ym_i, pr_i):
                    continue

                ym = _parse_ym(row[ym_i])
                if not ym:
                    continue
                max_ym = max(max_ym, ym)

                sid = _digits(row[sid_i])
                if not sid:
                    continue
                sid = sid.zfill(6)

                prem = _parse_brl_number(row[pr_i])
                sin = 0.0
                if sn_i is not None and len(row) > sn_i:
                    sin = _parse_brl_number(row[sn_i])

                bucket = agg.setdefault(sid, {"p": 0.0, "c": 0.0})
                bucket["p"] += prem
                bucket["c"] += sin

            start_ym = (max_ym // 100 * 12 + max_ym % 100 - 1 - 11)
            start_ym = (start_ym // 12) * 100 + (start_ym % 12 + 1)

            out: Dict[str, Dict[str, Any]] = {}
            for sid, val in agg.items():
                if val["p"] <= 0:
                    continue
                base = companies.get(sid) or {"name": f"SES_{sid}", "cnpj": None}
                out[sid] = {
                    "name": base["name"],
                    "cnpj": base["cnpj"],
                    "premiums": round(val["p"], 2),
                    "claims": round(val["c"], 2),
                }

            meta = SesExtractionMeta(used_url, cias, seguros, _ym_to_iso_01(start_ym), _ym_to_iso_01(max_ym))
            return meta, out

    finally:
        try:
            zip_path.unlink()
        except Exception:
            pass
