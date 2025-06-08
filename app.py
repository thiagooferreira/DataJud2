import streamlit as st
import requests
import pandas as pd
import time
import logging

logging.basicConfig(level=logging.INFO)
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100
MAX_PAGES = 20
STATUS_PRESCRICAO = "Arquivado - Extinção da Punibilidade - Prescrição"

UF_ENDPOINTS = {
    "AC":"tjac","AL":"tjal","AP":"tjap","AM":"tjam","BA":"tjba","CE":"tjce",
    "DF":"tjdft","ES":"tjes","GO":"tjgo","MA":"tjma","MT":"tjmt","MS":"tjms",
    "MG":"tjmg","PA":"tjpa","PB":"tjpb","PR":"tjpr","PE":"tjpe","PI":"tjpi",
    "RJ":"tjrj","RN":"tjrn","RS":"tjrs","RO":"tjro","RR":"tjrr","SC":"tjsc",
    "SP":"tjsp","SE":"tjse","TO":"tjto"
}

def get_api_url(uf):
    code = UF_ENDPOINTS.get(uf)
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{code}/_search" if code else None

def fetch_status_options(api_url):
    q = {"size":0,"aggs":{"options":{"terms":{"field":"status.descricao.keyword","size":500}}}}
    r = requests.post(api_url, headers=headers(), json=q, timeout=30).json()
    return [b["key"] for b in r.get("aggregations",{}).get("options",{}).get("buckets",[])]

def headers():
    return {'Authorization':f'APIKey {API_KEY}','Content-Type':'application/json'}

def search_with_status(api_url, status):
    hits, last_sort = [], None
    for page in range(MAX_PAGES):
        body = {"size": QUERY_SIZE, "query":{"bool":{"must":[{"match":{"status.descricao":status}}]}},
                "sort":[{"@timestamp":{"order":"asc"}}]}
        if last_sort: body["search_after"]=last_sort
        r = requests.post(api_url, headers=headers(), json=body, timeout=60).json()
        batch = r.get("hits",{}).get("hits",[])
        if not batch: break
        hits.extend(batch)
        last_sort = batch[-1].get("sort")
        if len(batch)<QUERY_SIZE: break
        time.sleep(0.1)
    return hits

def main():
    st.title("Consulta DataJud – Prescrição por UF")
    uf = st.sidebar.selectbox("Selecione UF", list(UF_ENDPOINTS.keys()))
    api_url = get_api_url(uf)
    if not api_url:
        st.error("UF inválida."); return

    try:
        st.sidebar.info("Buscando statuses disponíveis…")
        options = fetch_status_options(api_url)
    except:
        options = []

    if STATUS_PRESCRICAO not in options:
        st.warning(f"Status padrão não está disponível em {uf}. Disponíveis: {options[:5]}…")
    else:
        st.success("Status de prescrição disponível ✅")
    status = STATUS_PRESCRICAO

    if st.sidebar.button("Consultar"):
        st.info(f"Consultando UF: {uf}, status: {status}")
        hits = search_with_status(api_url, status)
        st.success(f"{len(hits)} processos encontrados")
        df = pd.json_normalize([h["_source"] for h in hits])
        st.dataframe(df)
