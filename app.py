import streamlit as st
import requests
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)

API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100
MAX_PAGES = 20
STATUS_PRESCRICAO = "Arquivado – Extinção da Punibilidade – Prescrição"

UF_ENDPOINTS = {
    "AC": "tjac","AL": "tjal","AP": "tjap","AM": "tjam",
    "BA": "tjba","CE": "tjce","DF": "tjdft","ES": "tjes",
    "GO": "tjgo","MA": "tjma","MT": "tjmt","MS": "tjms",
    "MG": "tjmg","PA": "tjpa","PB": "tjpb","PR": "tjpr",
    "PE": "tjpe","PI": "tjpi","RJ": "tjrj","RN": "tjrn",
    "RS": "tjrs","RO": "tjro","RR": "tjrr","SC": "tjsc",
    "SP": "tjsp","SE": "tjse","TO": "tjto"
}

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    if not code:
        return None
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search"

def headers():
    return {
        "Authorization": f"APIKey {API_KEY}",
        "Content-Type": "application/json"
    }

def fetch_status_options(api_url):
    q = {
        "size": 0,
        "aggs": {
            "options": {
                "terms": {
                    "field": "status.descricao.keyword",
                    "size": 500
                }
            }
        }
    }
    r = requests.post(api_url, headers=headers(), json=q, timeout=30)
    r.raise_for_status()
    data = r.json()
    buckets = data.get("aggregations", {}).get("options", {}).get("buckets", [])
    return [b["key"] for b in buckets]

def search_with_status(api_url, status):
    hits = []
    last_sort = None
    for _ in range(MAX_PAGES):
        body = {
            "size": QUERY_SIZE,
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"status.descricao": status}}
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
        hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch) < QUERY_SIZE:
            break
        time.sleep(0.1)
    return hits

def main():
    st.title("Consulta DataJud – Status por UF")

    uf = st.sidebar.selectbox("Selecione UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida.")
        return

    st.sidebar.info("Carregando opções de status…")
    try:
        options = fetch_status_options(api_url)
    except Exception as e:
        st.sidebar.error(f"Falha ao buscar status: {e}")
        options = []

    if STATUS_PRESCRICAO not in options:
        st.sidebar.warning(
            f"Status padrão indisponível em {uf}. Opções incluem: {options[:5]}"
        )
    else:
        st.sidebar.success("Status padrão disponível ✅")

    st.write(f"Status padrão: **{STATUS_PRESCRICAO}**")

    if st.button("Consultar"):
        st.info(f"Buscando processos arquivados por prescrição em {uf}...")
        try:
            hits = search_with_status(api_url, STATUS_PRESCRICAO)
        except Exception as e:
            st.error(f"Erro durante consulta: {e}")
            return
        st.success(f"{len(hits)} processos encontrados.")
        if hits:
            df = pd.json_normalize([h["_source"] for h in hits])
            st.dataframe(df)
            csv = df.to_csv(index=False).encode("utf-8")
            st.download_button("📥 Baixar CSV", csv, "resultados.csv", "text/csv")

if __name__ == "__main__":
    main()
