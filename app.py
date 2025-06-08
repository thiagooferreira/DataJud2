import streamlit as st
import requests
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100  # número por página
MAX_PAGES = 100   # limite de páginas a tentar

UF_ENDPOINTS = {
    "AC":"tjac","AL":"tjal","AP":"tjap","AM":"tjam","BA":"tjba",
    "CE":"tjce","DF":"tjdft","ES":"tjes","GO":"tjgo","MA":"tjma",
    "MT":"tjmt","MS":"tjms","MG":"tjmg","PA":"tjpa","PB":"tjpb",
    "PR":"tjpr","PE":"tjpe","PI":"tjpi","RJ":"tjrj","RN":"tjrn",
    "RS":"tjrs","RO":"tjro","RR":"tjrr","SC":"tjsc","SP":"tjsp",
    "SE":"tjse","TO":"tjto"
}

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search" if code else None

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def fetch_all_by_uf(api_url):
    all_hits, last_sort = [], None
    for page in range(MAX_PAGES):
        query = {
            "size": QUERY_SIZE,
            "query": {"match_all": {}},
            "sort":[{"@timestamp":{"order":"asc"}}]
        }
        if last_sort:
            query["search_after"] = last_sort

        resp = requests.post(api_url, headers=headers(), json=query, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("hits", {}).get("hits", [])
        if not batch:
            break

        all_hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch) < QUERY_SIZE:
            break
        time.sleep(0.1)  # desacelera um pouco para evitar sobrecarga

    logging.info(f"Total hits: {len(all_hits)}")
    return all_hits

def main():
    st.title("Consulta DATAJUD – Retorno Completo por UF")

    uf = st.sidebar.selectbox("Selecione a UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida!")
        return

    if st.button("Executar consulta (match_all)"):
        with st.spinner(f"Buscando todos os processos da UF {uf} ..."):
            try:
                hits = fetch_all_by_uf(api_url)
            except Exception as e:
                st.error(f"Erro na consulta: {e}")
                return

        st.success(f"{len(hits)} resultados encontrados na UF {uf}.")
        if hits:
            df = pd.json_normalize([h["_source"] for h in hits])
            st.dataframe(df)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Baixar CSV completo", csv, f"{uf}_todos.csv", "text/csv")

if __name__ == "__main__":
    main()
