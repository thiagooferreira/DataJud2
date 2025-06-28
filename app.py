import streamlit as st
import requests
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100
MAX_PAGES = 100

UF_ENDPOINTS = {
    "AC": "tjac", "AL": "tjal", "AP": "tjap", "AM": "tjam", "BA": "tjba",
    "CE": "tjce", "DF": "tjdft", "ES": "tjes", "GO": "tjgo", "MA": "tjma",
    "MT": "tjmt", "MS": "tjms", "MG": "tjmg", "PA": "tjpa", "PB": "tjpb",
    "PR": "tjpr", "PE": "tjpe", "PI": "tjpi", "RJ": "tjrj", "RN": "tjrn",
    "RS": "tjrs", "RO": "tjro", "RR": "tjrr", "SC": "tjsc", "SP": "tjsp",
    "SE": "tjse", "TO": "tjto"
}

TERM = "prescrição"

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search" if code else None

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def fetch_filtered_by_term(api_url, term):
    hits, last_sort = [], None
    for _ in range(MAX_PAGES):
        body = {
            "size": QUERY_SIZE,
            "query": {
                "bool": {
                    "should": [
                        {"match_phrase": {"movimentos.nome": term}},
                        {"match_phrase": {"movimentos.descricao": term}},
                        {"match_phrase": {"status.descricao": term}}
                    ],
                    "minimum_should_match": 1
                }
            },
            "sort": [{"@timestamp": {"order": "asc"}}]
        }
        if last_sort:
            body["search_after"] = last_sort

        resp = requests.post(api_url, headers=headers(), json=body, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("hits", {}).get("hits", [])
        if not batch:
            break
        hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch) < QUERY_SIZE:
            break
        time.sleep(0.1)
    logging.info(f"Total hits contendo '{term}': {len(hits)}")
    return hits

def main():
    st.title("DataJud – Buscar qualquer 'prescrição' por UF")

    uf = st.sidebar.selectbox("Selecione UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida!")
        return

    st.sidebar.write(f"Buscando termos relacionados à **prescrição** em múltiplos campos")

    if st.button("Buscar processos relacionados"):
        st.info(f"Consultando UF: {uf}, buscando 'prescrição'...")
        try:
            hits = fetch_filtered_by_term(api_url, TERM)
        except Exception as e:
            st.error(f"Erro na consulta: {e}")
            return

        st.success(f"{len(hits)} processos encontrados com referência a '{TERM}'")

        if hits:
            dados_brutos = [h["_source"] for h in hits]
            dados_simples = [{
                "numeroProcesso": d.get("numeroProcesso"),
                "classe": d.get("classe", {}).get("nome"),
                "tribunal": d.get("tribunal"),
                "sistema": d.get("sistema", {}).get("nome"),
                "dataAjuizamento": d.get("dataAjuizamento"),
                "grau": d.get("grau")
            } for d in dados_brutos]

            df_visual = pd.DataFrame(dados_simples)
            st.dataframe(df_visual)

            df_completo = pd.json_normalize(dados_brutos, sep="_")
            csv = df_completo.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Baixar resultado completo (CSV)", csv, f"{uf}_prescricao_completo.csv", "text/csv")

if __name__ == "__main__":
    main()
