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

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search" if code else None

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def fetch_all_by_uf_and_area(api_url, area_keyword="Criminal"):
    all_hits, last_sort = [], None
    for _ in range(MAX_PAGES):
        body = {
            "size": QUERY_SIZE,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"classe.nome": area_keyword}}
                    ]
                }
            },
            "sort": [{"@timestamp": {"order": "asc"}}]
        }
        if last_sort:
            body["search_after"] = last_sort
        resp = requests.post(api_url, headers=headers(), json=body, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("hits", {}).get("hits", [])
        if not batch:
            break
        all_hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch) < QUERY_SIZE:
            break
        time.sleep(0.1)
    return all_hits

def main():
    st.title("Consulta DataJud – UF + Área Criminal")

    uf = st.sidebar.selectbox("Selecione a UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida!")
        return

    st.sidebar.info("Área sendo filtrada: Criminal (classe.nome contém 'Criminal')")

    if st.button("Executar consulta"):
        with st.spinner(f"Buscando processos da Área Criminal em {uf}..."):
            try:
                hits = fetch_all_by_uf_and_area(api_url, area_keyword="Criminal")
            except Exception as e:
                st.error(f"Erro na consulta: {e}")
                return

        st.success(f"{len(hits)} processos encontrados na Área Criminal — UF: {uf}.")
        if hits:
            df = pd.json_normalize([h["_source"] for h in hits])
            st.dataframe(df)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Baixar CSV", csv, f"{uf}_criminal.csv", "text/csv")

if __name__ == "__main__":
    main()
