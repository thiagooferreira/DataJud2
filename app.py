import streamlit as st
import requests
import pandas as pd

# Dicionário com UF e seus respectivos endpoints
uf_endpoints = {
    "Acre": "tjac", "Alagoas": "tjal", "Amapá": "tjap", "Amazonas": "tjam",
    "Bahia": "tjba", "Ceará": "tjce", "Distrito Federal": "tjdf", "Espírito Santo": "tjes",
    "Goiás": "tjgo", "Maranhão": "tjma", "Mato Grosso": "tjmt", "Mato Grosso do Sul": "tjms",
    "Minas Gerais": "tjmg", "Pará": "tjpa", "Paraíba": "tjpb", "Paraná": "tjpr",
    "Pernambuco": "tjpe", "Piauí": "tjpi", "Rio de Janeiro": "tjrj", "Rio Grande do Norte": "tjrn",
    "Rio Grande do Sul": "tjrs", "Rondônia": "tjro", "Roraima": "tjrr", "Santa Catarina": "tjsc",
    "São Paulo": "tjsp", "Sergipe": "tjse", "Tocantins": "tjto"
}

st.title("Consulta DataJud por UF e Status")

# Seleção da UF
selected_uf = st.selectbox("Selecione a UF", list(uf_endpoints.keys()))

# Status fixo com opção de editar
default_status = "Arquivado - Extinção da Punibilidade - Prescrição"
status_input = st.text_input("Status (status.descricao)", value=default_status)

# Botão para iniciar consulta
if st.button("Consultar"):
    with st.spinner("Consultando..."):
        endpoint = f"https://api-publica.datajud.cnj.jus.br/api_publica_{uf_endpoints[selected_uf]}/_search"
        query = {
            "size": 100,
            "query": {
                "bool": {
                    "must": [
                        {"match_phrase": {"status.descricao": status_input}}
                    ]
                }
            }
        }

        try:
            response = requests.post(endpoint, json=query)
            response.raise_for_status()
            data = response.json()

            results = [hit["_source"] for hit in data.get("hits", {}).get("hits", [])]
            if results:
                df = pd.DataFrame(results)
                st.success(f"{len(df)} resultados encontrados.")
                st.dataframe(df)
                csv = df.to_csv(index=False).encode("utf-8")
                st.download_button("Baixar CSV", csv, "resultados.csv", "text/csv")
            else:
                st.warning("Nenhum resultado encontrado.")
        except Exception as e:
            st.error(f"Erro ao consultar API: {e}")
