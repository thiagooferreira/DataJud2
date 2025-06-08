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
    "AC":"tjac","AL":"tjal","AP":"tjap","AM":"tjam","BA":"tjba",
    "CE":"tjce","DF":"tjdft","ES":"tjes","GO":"tjgo","MA":"tjma",
    "MT":"tjmt","MS":"tjms","MG":"tjmg","PA":"tjpa","PB":"tjpb",
    "PR":"tjpr","PE":"tjpe","PI":"tjpi","RJ":"tjrj","RN":"tjrn",
    "RS":"tjrs","RO":"tjro","RR":"tjrr","SC":"tjsc","SP":"tjsp",
    "SE":"tjse","TO":"tjto"
}

SEARCH_PHRASE = "Arquivado – Extinção da Punibilidade – Prescrição"

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search" if code else None

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def fetch_filtered(api_url, phrase):
    hits, last_sort = [], None
    for _ in range(MAX_PAGES):
        query = {
            "size": QUERY_SIZE,
            "query": {
                "multi_match": {
                    "query": phrase,
                    "type": "phrase",
                    "fields": [
                        "status.descricao",
                        "movimentos.nome",
                        "movimentos.descricao",
                        "movimentacoes.texto",
                        "descricao",      # fallback generic
                        "movimentos.*.nome"
                    ],
                    "operator": "AND"
                }
            },
            "sort": [{"@timestamp": {"order": "asc"}}]
        }
        if last_sort:
            query["search_after"] = last_sort

        resp = requests.post(api_url, headers=headers(), json=query, timeout=60)
        resp.raise_for_status()
        batch = resp.json().get("hits", {}).get("hits", [])
        if not batch:
            break

        hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch) < QUERY_SIZE:
            break
        time.sleep(0.1)
    return hits

def main():
    st.title("DataJud – Filtragem Avançada por Prescrição")

    uf = st.sidebar.selectbox("Selecione UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida!")
        return

    st.sidebar.write("🔎 Filtro sendo aplicado em múltiplos campos:")

    if st.button("Buscar processos prescritos"):
        with st.spinner(f"Buscando ‘{SEARCH_PHRASE}’ em qualquer campo..."):
            try:
                hits = fetch_filtered(api_url, SEARCH_PHRASE)
            except Exception as e:
                st.error(f"Erro na consulta: {e}")
                return

        st.success(f"{len(hits)} processos encontrados na UF {uf}")
        if hits:
            df = pd.json_normalize([h["_source"] for h in hits])
            st.dataframe(df)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Baixar CSV", csv, f"{uf}_prescricao.csv", "text/csv")

if __name__ == "__main__":
    main()
