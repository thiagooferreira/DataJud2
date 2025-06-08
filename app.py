import streamlit as st
import requests
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
UF_ENDPOINTS = {
    "GO": "tjgo",  # Exemplo UF, inclua as outras conforme necessário
    "SP": "tjsp",
    # ...
}

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search"

def fetch_status_options(api_url):
    payload = {
        "size": 0,
        "aggs": {
            "status_terms": {
                "terms": {
                    "field": "status.descricao.keyword",
                    "size": 1000
                }
            }
        }
    }
    r = requests.post(api_url, headers=headers(), json=payload, timeout=30)
    r.raise_for_status()
    buckets = r.json().get("aggregations", {}).get("status_terms", {}).get("buckets", [])
    return [b["key"] for b in buckets]

st.title("DataJud – Identificação de termos em status.descricao")

uf = st.sidebar.selectbox("Selecione a UF", list(UF_ENDPOINTS.keys()))
api_url = get_api_url(uf)
if not api_url:
    st.error("UF inválida!")
    st.stop()

if st.button("Listar termos de status.descricao"):
    try:
        terms = fetch_status_options(api_url)
    except Exception as e:
        st.error(f"Erro na consulta: {e}")
        st.stop()

    if terms:
        st.success(f"{len(terms)} termos retornados.")
        df = pd.DataFrame(terms, columns=["status.descricao"])
        st.dataframe(df)
        st.write("Use esses termos para configurar o filtro exato posteriormente.")
    else:
        st.warning("Nenhum status.descricao encontrado nesta UF.")
