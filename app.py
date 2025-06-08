import streamlit as st
import requests
import pandas as pd
import json
import os
import datetime
import time
from urllib.parse import quote
import logging
import concurrent.futures

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Configurações da API
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100  # Define quantos resultados buscar por página
MAX_PAGES = 20    # Limite máximo de páginas para evitar sobrecarga

# Status específico a ser buscado
STATUS_PRESCRICAO = "Arquivado - Extinção da Punibilidade - Prescrição"

# Dicionário de UF para endpoint da API
UF_ENDPOINTS = {
    "AC": "tjac",
    "AL": "tjal",
    "AP": "tjap",
    "AM": "tjam",
    "BA": "tjba",
    "CE": "tjce",
    "DF": "tjdft",
    "ES": "tjes",
    "GO": "tjgo",
    "MA": "tjma",
    "MT": "tjmt",
    "MS": "tjms",
    "MG": "tjmg",
    "PA": "tjpa",
    "PB": "tjpb",
    "PR": "tjpr",
    "PE": "tjpe",
    "PI": "tjpi",
    "RJ": "tjrj",
    "RN": "tjrn",
    "RS": "tjrs",
    "RO": "tjro",
    "RR": "tjrr",
    "SC": "tjsc",
    "SP": "tjsp",
    "SE": "tjse",
    "TO": "tjto",
    # Tribunais Superiores
    "STF": "stf",
    "STJ": "stj",
    "TST": "tst",
    "TSE": "tse",
    "STM": "stm",
}

def get_api_url(uf):
    """Retorna a URL da API para a UF selecionada."""
    endpoint = UF_ENDPOINTS.get(uf)
    if not endpoint:
        logging.error(f"UF {uf} não encontrada no dicionário de endpoints.")
        return None
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{endpoint}/_search"

def fetch_status_descricao_options(api_url):
    """
    Busca todas as opções distintas de status.descricao disponíveis para uma UF.
    Usa agregação (terms aggregation) para obter valores únicos.
    """
    # Query para buscar apenas os status distintos
    aggs_query = {
        "size": 0,  # Não precisamos dos documentos, apenas da agregação
        "aggs": {
            "status_options": {
                "terms": {
                    "field": "status.descricao.keyword",  # Usando .keyword para campos de texto
                    "size": 1000  # Buscar até 1000 status distintos
                }
            }
        }
    }
    
    # Query alternativa caso o campo .keyword não exista
    aggs_query_alt = {
        "size": 0,
        "aggs": {
            "status_options": {
                "terms": {
                    "field": "status.descricao",
                    "size": 1000
                }
            }
        }
    }
    
    headers = {
        'Authorization': f'APIKey {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        # Primeira tentativa com .keyword
        logging.info(f"Tentando buscar status com .keyword para URL: {api_url}")
        response = requests.post(api_url, headers=headers, json=aggs_query, timeout=120)
        
        # Registrar detalhes da resposta para depuração
        logging.info(f"Resposta da API (status: {response.status_code}): {response.text[:500]}...")
        
        if response.status_code == 200:
            data = response.json()
            
            # Verificar se a resposta contém agregações
            if 'aggregations' in data and 'status_options' in data['aggregations'] and 'buckets' in data['aggregations']['status_options']:
                buckets = data['aggregations']['status_options']['buckets']
                if buckets:  # Se encontrou resultados, retorna
                    status_options = [bucket['key'] for bucket in buckets]
                    logging.info(f"Encontrados {len(status_options)} status usando .keyword")
                    return sorted(status_options)
                else:
                    logging.warning("Agregação retornou buckets vazios com .keyword")
            else:
                logging.warning(f"Resposta não contém agregações esperadas com .keyword: {data.keys()}")
        else:
            logging.warning(f"Erro na requisição com .keyword: {response.status_code} - {response.text[:200]}")
        
        # Se não encontrou resultados com .keyword, tenta sem .keyword
        logging.info("Tentando buscar status sem .keyword")
        response = requests.post(api_url, headers=headers, json=aggs_query_alt, timeout=120)
        
        # Registrar detalhes da resposta para depuração
        logging.info(f"Resposta da API sem .keyword (status: {response.status_code}): {response.text[:500]}...")
        
        if response.status_code == 200:
            data = response.json()
            
            # Extrair os status da resposta
            if 'aggregations' in data and 'status_options' in data['aggregations'] and 'buckets' in data['aggregations']['status_options']:
                buckets = data['aggregations']['status_options']['buckets']
                if buckets:
                    status_options = [bucket['key'] for bucket in buckets]
                    logging.info(f"Encontrados {len(status_options)} status sem usar .keyword")
                    return sorted(status_options)
                else:
                    logging.warning("Agregação retornou buckets vazios sem .keyword")
            else:
                logging.warning(f"Resposta não contém agregações esperadas sem .keyword: {data.keys()}")
        else:
            logging.warning(f"Erro na requisição sem .keyword: {response.status_code} - {response.text[:200]}")
        
        # Se ainda não encontrou, tenta uma amostragem de documentos
        logging.info("Tentando buscar status via amostragem de documentos")
        sample_query = {
            "size": 100,  # Buscar 100 documentos para extrair status
            "query": {
                "match_all": {}
            },
            "_source": ["status.descricao"]  # Buscar apenas o campo status.descricao
        }
        
        response = requests.post(api_url, headers=headers, json=sample_query, timeout=120)
        
        # Registrar detalhes da resposta para depuração
        logging.info(f"Resposta da API para amostragem (status: {response.status_code}): {response.text[:500]}...")
        
        if response.status_code == 200:
            data = response.json()
            
            status_set = set()
            if 'hits' in data and 'hits' in data['hits']:
                for hit in data['hits']['hits']:
                    source = hit.get('_source', {})
                    status = source.get('status', {})
                    if isinstance(status, dict) and 'descricao' in status:
                        status_set.add(status['descricao'])
            
            if status_set:
                logging.info(f"Encontrados {len(status_set)} status via amostragem de documentos")
                return sorted(list(status_set))
            else:
                logging.warning("Não foram encontrados status via amostragem de documentos")
        else:
            logging.warning(f"Erro na requisição de amostragem: {response.status_code} - {response.text[:200]}")
        
        # Verificar mapeamento do índice para diagnóstico
        logging.info("Verificando mapeamento do índice para diagnóstico")
        mapping_url = api_url.replace("/_search", "/_mapping")
        
        try:
            mapping_response = requests.get(mapping_url, headers=headers, timeout=60)
            logging.info(f"Resposta do mapeamento (status: {mapping_response.status_code}): {mapping_response.text[:1000]}...")
            
            if mapping_response.status_code == 200:
                mapping_data = mapping_response.json()
                logging.info(f"Mapeamento do índice obtido: {json.dumps(mapping_data)[:1000]}...")
                
                # Analisar mapeamento para verificar se status.descricao existe e seu tipo
                # Esta é uma análise simplificada, o mapeamento real pode ser mais complexo
                for index_name, index_data in mapping_data.items():
                    if 'mappings' in index_data:
                        mappings = index_data['mappings']
                        if 'properties' in mappings:
                            properties = mappings['properties']
                            if 'status' in properties and 'properties' in properties['status']:
                                status_props = properties['status']['properties']
                                if 'descricao' in status_props:
                                    descricao_mapping = status_props['descricao']
                                    logging.info(f"Mapeamento de status.descricao encontrado: {descricao_mapping}")
                                else:
                                    logging.warning("Campo 'descricao' não encontrado no mapeamento de 'status'")
                            else:
                                logging.warning("Propriedades de 'status' não encontradas no mapeamento")
                        else:
                            logging.warning("Propriedades não encontradas no mapeamento")
                    else:
                        logging.warning("Mapeamento não encontrado no índice")
            else:
                logging.warning(f"Não foi possível obter o mapeamento do índice: {mapping_response.status_code}")
        except Exception as e:
            logging.error(f"Erro ao verificar mapeamento do índice: {e}")
            
        logging.warning(f"Não foi possível encontrar status.descricao na resposta após todas as tentativas")
        return []
    except Exception as e:
        logging.error(f"Erro ao buscar opções de status.descricao: {e}")
        return []

def create_query_status_descricao(status_descricao, classe_processual=None, search_after=None):
    """
    Cria uma query DSL para a API do DataJud usando exclusivamente o campo status.descricao.
    Opcionalmente inclui filtro por classe processual.
    """
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "status.descricao": status_descricao
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "@timestamp": {
                    "order": "asc"
                }
            }
        ]
    }
    
    # Adiciona search_after se fornecido (para paginação)
    if search_after:
        query["search_after"] = search_after
    
    # Adiciona filtro de classe processual se fornecido
    if classe_processual:
        query["query"]["bool"]["must"].append({
            "match": {
                "classe.nome": classe_processual
            }
        })
    
    return query

def create_query_fallback(status_descricao, classe_processual=None, search_after=None):
    """
    Cria uma query DSL de fallback usando match_phrase em status.descricao.
    Usado quando a busca exata não retorna resultados.
    """
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "status.descricao": status_descricao
                        }
                    }
                ]
            }
        },
        "sort": [
            {
                "@timestamp": {
                    "order": "asc"
                }
            }
        ]
    }
    
    # Adiciona search_after se fornecido (para paginação)
    if search_after:
        query["search_after"] = search_after
    
    # Adiciona filtro de classe processual se fornecido
    if classe_processual:
        query["query"]["bool"]["must"].append({
            "match": {
                "classe.nome": classe_processual
            }
        })
    
    return query

def fetch_datajud_data(api_url, query):
    """Realiza a consulta à API do DataJud e retorna os dados."""
    headers = {
        'Authorization': f'APIKey {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(api_url, headers=headers, json=query, timeout=120)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        error_msg = f"Erro ao conectar à API DataJud: {e}"
        if hasattr(e, 'response') and e.response is not None:
            error_msg += f"\nResposta da API (Status {e.response.status_code}): {e.response.text[:500]}..."
        logging.error(error_msg)
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON da resposta da API: {e}")
        return None

def fetch_all_pages_with_status(api_url, status_descricao, classe_processual=None, progress_callback=None):
    """
    Busca todos os resultados com paginação usando search_after e filtro específico de status.descricao.
    Opcionalmente filtra por classe processual.
    """
    all_hits = []
    page = 1
    search_after = None
    total_hits = 0
    
    # Primeira tentativa com match exato
    while True:
        # Criar query para a página atual
        query = create_query_status_descricao(status_descricao, classe_processual, search_after)
        
        # Fazer a requisição à API
        response = fetch_datajud_data(api_url, query)
        
        if not response or 'hits' not in response or 'hits' not in response['hits']:
            break
        
        hits = response['hits']['hits']
        if not hits:
            break
        
        # Adicionar resultados à lista
        all_hits.extend(hits)
        
        # Obter total de hits se disponível
        if 'total' in response['hits']:
            if isinstance(response['hits']['total'], dict) and 'value' in response['hits']['total']:
                total_hits = response['hits']['total']['value']
            else:
                total_hits = response['hits']['total']
        
        # Atualizar callback de progresso se fornecido
        if progress_callback is not None:
            progress_callback(len(all_hits), total_hits)
        
        # Verificar se há mais páginas
        if len(hits) < 100 or page >= MAX_PAGES:  # Tamanho da página ou limite máximo
            break
        
        # Obter o último valor para search_after
        search_after = hits[-1]['sort']
        page += 1
        
        # Log para debug
        logging.info(f"Buscando página {page}, total de resultados até agora: {len(all_hits)}")
        
        # Pequena pausa para não sobrecarregar a API
        time.sleep(0.5)
    
    # Se não encontrou resultados com match exato, tenta com match_phrase (fallback)
    if not all_hits:
        logging.info(f"Tentando busca com match_phrase para status: {status_descricao}")
        page = 1
        search_after = None
        
        while True:
            # Criar query para a página atual
            query = create_query_fallback(status_descricao, classe_processual, search_after)
            
            # Fazer a requisição à API
            response = fetch_datajud_data(api_url, query)
            
            if not response or 'hits' not in response or 'hits' not in response['hits']:
                break
            
            hits = response['hits']['hits']
            if not hits:
                break
            
            # Adicionar resultados à lista
            all_hits.extend(hits)
            
            # Obter total de hits se disponível
            if 'total' in response['hits']:
                if isinstance(response['hits']['total'], dict) and 'value' in response['hits']['total']:
                    total_hits = response['hits']['total']['value']
                else:
                    total_hits = response['hits']['total']
            
            # Atualizar callback de progresso se fornecido
            if progress_callback is not None:
                progress_callback(len(all_hits), total_hits)
            
            # Verificar se há mais páginas
            if len(hits) < 100 or page >= MAX_PAGES:  # Tamanho da página ou limite máximo
                break
            
            # Obter o último valor para search_after
            search_after = hits[-1]['sort']
            page += 1
            
            # Log para debug
            logging.info(f"Buscando página {page} (fallback), total de resultados até agora: {len(all_hits)}")
            
            # Pequena pausa para não sobrecarregar a API
            time.sleep(0.5)
    
    return all_hits

def fetch_all_classes(api_url):
    """Busca todas as classes processuais disponíveis para o tribunal."""
    # Query para buscar apenas as classes distintas
    aggs_query = {
        "size": 0,  # Não precisamos dos documentos, apenas da agregação
        "aggs": {
            "classes": {
                "terms": {
                    "field": "classe.nome",
                    "size": 1000  # Buscar até 1000 classes distintas
                }
            }
        }
    }
    
    headers = {
        'Authorization': f'APIKey {API_KEY}',
        'Content-Type': 'application/json'
    }
    
    # Primeiro tenta com agregação
    try:
        response = requests.post(api_url, headers=headers, json=aggs_query, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        # Extrair as classes da resposta
        if 'aggregations' in data and 'classes' in data['aggregations'] and 'buckets' in data['aggregations']['classes']:
            classes = [bucket['key'] for bucket in data['aggregations']['classes']['buckets']]
            return sorted(classes)
    except Exception as e:
        logging.warning(f"Não foi possível obter a lista de classes processuais via agregação: {e}")
    
    # Se falhar, retorna lista vazia
    return []

def process_data(all_hits):
    """Processa os dados JSON retornados e extrai as informações completas."""
    if not all_hits:
        logging.warning("Nenhum resultado encontrado na resposta da API.")
        return pd.DataFrame()

    # Extrair todos os dados completos de cada processo
    results = []
    for hit in all_hits:
        # Adiciona o documento completo como está na API
        source_data = hit.get('_source', {})
        
        # Opção 1: Manter a estrutura original para processamento
        results.append(source_data)
    
    # Criar DataFrame com todos os dados
    df = pd.DataFrame(results)
    
    # Expandir colunas que são dicionários para melhor visualização
    # Isso ajuda a mostrar objetos aninhados como sistema, classe, etc.
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            # Tentar expandir dicionários em colunas separadas
            try:
                expanded = pd.json_normalize(df[col].dropna())
                for exp_col in expanded.columns:
                    df[f"{col}.{exp_col}"] = df[col].apply(
                        lambda x: x.get(exp_col) if isinstance(x, dict) else None
                    )
            except Exception as e:
                logging.warning(f"Erro ao expandir coluna {col}: {e}")
        
        # Expandir colunas que são listas de dicionários (como movimentos)
        elif col == 'movimentos' and df[col].apply(lambda x: isinstance(x, list)).any():
            try:
                # Criar colunas para os primeiros N movimentos
                max_movimentos = 5  # Limitar para não sobrecarregar a visualização
                
                for i in range(max_movimentos):
                    # Adicionar coluna para o nome do movimento
                    df[f"movimento_{i+1}_nome"] = df[col].apply(
                        lambda x: x[i].get('nome') if isinstance(x, list) and len(x) > i and isinstance(x[i], dict) else None
                    )
                    
                    # Adicionar coluna para a data do movimento
                    df[f"movimento_{i+1}_data"] = df[col].apply(
                        lambda x: x[i].get('dataHora') if isinstance(x, list) and len(x) > i and isinstance(x[i], dict) else None
                    )
            except Exception as e:
                logging.warning(f"Não foi possível expandir a coluna de movimentos: {e}")
    
    return df

def save_to_csv(df, uf, classe_processual=None, status_descricao=None):
    """Salva o DataFrame em um arquivo CSV."""
    # Cria o diretório se não existir
    os.makedirs("/home/ubuntu/consultas_datajud", exist_ok=True)
    
    # Formata o nome do arquivo
    if classe_processual:
        # Limita o nome do arquivo para não ficar muito grande
        classe_slug = classe_processual.lower().replace(" ", "_")[:30]
    else:
        classe_slug = "sem_filtro_classe"
    
    # Adiciona o status ao nome do arquivo
    if status_descricao:
        status_slug = status_descricao.lower().replace(" ", "_").replace("-", "_")[:30]
    else:
        status_slug = "prescricao"
        
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/home/ubuntu/consultas_datajud/{status_slug}_{uf}_{classe_slug}_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename
    except Exception as e:
        logging.error(f"Erro ao salvar o arquivo CSV: {e}")
        return None

def consultar_uf_com_status(uf, status_descricao, classe_processual=None, progress_callback=None):
    """Consulta uma UF específica com o filtro de status fornecido."""
    api_url = get_api_url(uf)
    if not api_url:
        return None, f"Erro: UF {uf} não encontrada no dicionário de endpoints."
    
    try:
        # Buscar todos os resultados com paginação
        all_hits = fetch_all_pages_with_status(
            api_url, 
            status_descricao, 
            classe_processual, 
            progress_callback
        )
        
        if not all_hits:
            return None, f"Nenhum processo encontrado para UF {uf} com status '{status_descricao}'."
        
        # Processar os dados
        df = process_data(all_hits)
        
        if df.empty:
            return None, f"Erro ao processar dados para UF {uf}."
        
        # Salvar em CSV
        csv_path = save_to_csv(df, uf, classe_processual, status_descricao)
        
        if not csv_path:
            return df, f"Erro ao salvar CSV para UF {uf}."
        
        return df, csv_path
    except Exception as e:
        logging.error(f"Erro ao consultar UF {uf}: {e}")
        return None, f"Erro ao consultar UF {uf}: {str(e)}"

def consultar_todas_ufs_com_status(status_descricao, classe_processual=None):
    """Consulta todas as UFs com o filtro de status fornecido."""
    resultados = {}
    erros = {}
    
    # Criar barra de progresso geral
    progress_text = st.empty()
    progress_bar = st.progress(0)
    
    total_ufs = len(UF_ENDPOINTS)
    ufs_processadas = 0
    
    # Função para atualizar o progresso
    def update_progress(uf, hits_count, total_hits):
        nonlocal ufs_processadas
        progress_text.text(f"Consultando UF: {uf} - {hits_count} processos encontrados de {total_hits if total_hits else 'total desconhecido'}")
    
    # Processar cada UF
    for uf in UF_ENDPOINTS.keys():
        progress_text.text(f"Consultando UF: {uf}...")
        
        # Callback para atualizar progresso específico da UF
        def uf_progress_callback(hits_count, total_hits):
            update_progress(uf, hits_count, total_hits)
        
        # Consultar a UF
        df, resultado = consultar_uf_com_status(uf, status_descricao, classe_processual, uf_progress_callback)
        
        if df is not None:
            resultados[uf] = df
        else:
            erros[uf] = resultado
        
        # Atualizar progresso geral
        ufs_processadas += 1
        progress_bar.progress(ufs_processadas / total_ufs)
    
    progress_text.text("Consulta finalizada!")
    
    return resultados, erros

def main():
    st.set_page_config(
        page_title="Consulta DataJud - Status Processuais",
        page_icon="⚖️",
        layout="wide"
    )
    
    st.title("Consulta de Processos por Status no DataJud")
    st.markdown("""
    Esta aplicação consulta processos judiciais por status específicos em todos os tribunais 
    disponíveis na API pública do DataJud, com foco especial em **"Arquivado - Extinção da Punibilidade - Prescrição"**.
    """)
    
    # Sidebar para filtros
    st.sidebar.header("Filtros de Consulta")
    
    # Opções de consulta
    consulta_option = st.sidebar.radio(
        "Tipo de Consulta",
        ["Consultar UF específica", "Consultar todas as UFs"],
        index=0
    )
    
    if consulta_option == "Consultar UF específica":
        # Dropdown para seleção de UF
        uf_options = list(UF_ENDPOINTS.keys())
        selected_uf = st.sidebar.selectbox("Selecione a UF", uf_options)
        
        # Obter URL da API para a UF selecionada
        api_url = None
        available_classes = []
        available_status = []
        
        if selected_uf:
            api_url = get_api_url(selected_uf)
            if api_url:
                # Carregar status disponíveis para a UF selecionada
                with st.spinner(f"Carregando status disponíveis para {selected_uf}..."):
                    available_status = fetch_status_descricao_options(api_url)
                    if not available_status:
                        st.warning(f"Não foi possível carregar os status disponíveis para {selected_uf}. Verifique a conexão com a API.")
                    else:
                        st.success(f"Encontrados {len(available_status)} status distintos para {selected_uf}.")
                
                # Carregar classes processuais disponíveis para a UF selecionada
                with st.spinner(f"Carregando classes processuais para {selected_uf}..."):
                    available_classes = fetch_all_classes(api_url)
                    if not available_classes:
                        st.sidebar.warning("Não foi possível carregar as classes processuais. Você pode prosseguir sem filtro de classe.")
        
        # Seção de Status Processuais
        st.subheader(f"Status Disponíveis para {selected_uf}")
        
        if available_status:
            # Exibir os status disponíveis em uma tabela
            status_df = pd.DataFrame({"Status Disponíveis": available_status})
            st.dataframe(status_df, height=300)
            
            # Verificar se o status de prescrição está disponível
            prescricao_status = [s for s in available_status if "prescrição" in s.lower() or "prescricao" in s.lower()]
            if prescricao_status:
                st.success(f"✅ Encontrados {len(prescricao_status)} status relacionados à prescrição:")
                for status in prescricao_status:
                    st.info(f"• {status}")
            else:
                st.warning("⚠️ Nenhum status relacionado à prescrição encontrado nesta UF.")
            
            # Permitir seleção do status
            selected_status = st.selectbox(
                "Selecione o Status para Consulta",
                options=[""] + available_status,
                format_func=lambda x: "Selecione um status..." if x == "" else x,
                index=0
            )
            
            # Opção para usar o status padrão de prescrição
            use_default_status = st.checkbox(
                "Usar status padrão de prescrição",
                value=False,
                help=f"Se marcado, usará o valor '{STATUS_PRESCRICAO}' independente da seleção acima."
            )
            
            if use_default_status:
                selected_status = STATUS_PRESCRICAO
                st.info(f"Usando status padrão: {STATUS_PRESCRICAO}")
        else:
            st.error("Não foi possível carregar os status disponíveis. Usando status padrão.")
            selected_status = STATUS_PRESCRICAO
            use_default_status = True
        
        # Seção de Classes Processuais
        st.sidebar.subheader("Filtro de Classe Processual (opcional)")
        
        # Opção para entrada manual ou seleção de classes processuais
        class_input_method = st.sidebar.radio(
            "Método de seleção de Classe Processual",
            ["Selecionar da lista", "Digitar manualmente", "Sem filtro de classe"],
            index=2 if not available_classes else 0,
            key="class_input_method"
        )
        
        selected_class = None
        if class_input_method == "Selecionar da lista":
            if available_classes:
                selected_class = st.sidebar.selectbox(
                    "Selecione a Classe Processual",
                    options=[""] + available_classes,
                    format_func=lambda x: "Sem filtro de classe" if x == "" else x,
                    key="class_select"
                )
                if selected_class == "":
                    selected_class = None
            else:
                st.sidebar.warning("Não foi possível carregar a lista de classes. Por favor, use a opção de digitação manual.")
        elif class_input_method == "Digitar manualmente":
            # Entrada manual com exemplos
            selected_class = st.sidebar.text_input(
                "Digite a Classe Processual",
                placeholder="Exemplo: Procedimento do Juizado Especial Cível",
                key="manual_class_input"
            )
            if not selected_class:
                selected_class = None
        
        # Botão para consulta
        if st.button("Consultar"):
            if not selected_uf:
                st.error("Por favor, selecione uma UF para continuar.")
            elif not selected_status and not use_default_status:
                st.error("Por favor, selecione um status ou marque a opção para usar o status padrão.")
            else:
                # Exibir informações da consulta
                st.subheader(f"Consultando processos com status para UF: {selected_uf}")
                st.info(f"Status selecionado: {selected_status}")
                
                if selected_class:
                    st.info(f"Filtro adicional por classe processual: {selected_class}")
                else:
                    st.info("Sem filtro adicional de classe processual.")
                
                # Realizar a consulta
                with st.spinner("Consultando API do DataJud..."):
                    progress_text = st.empty()
                    progress_bar = st.progress(0)
                    
                    # Callback para atualizar progresso
                    def progress_callback(hits_count, total_hits):
                        if total_hits:
                            progress = min(1.0, hits_count / total_hits)
                            progress_bar.progress(progress)
                            progress_text.text(f"Encontrados {hits_count} de {total_hits} processos...")
                        else:
                            progress_text.text(f"Encontrados {hits_count} processos...")
                    
                    df, resultado = consultar_uf_com_status(selected_uf, selected_status, selected_class, progress_callback)
                
                if isinstance(df, pd.DataFrame) and not df.empty:
                    # Exibir informações sobre os resultados
                    st.success(f"Consulta realizada com sucesso! {len(df)} processos encontrados.")
                    
                    # Exibir os dados em uma tabela
                    st.subheader("Resultados da Consulta")
                    st.dataframe(df)
                    
                    # Mostrar caminho do CSV
                    if isinstance(resultado, str) and resultado.startswith("/home"):
                        st.success(f"Dados exportados para: {resultado}")
                        
                        # Oferecer download do arquivo
                        with open(resultado, "rb") as file:
                            st.download_button(
                                label="Baixar CSV",
                                data=file,
                                file_name=os.path.basename(resultado),
                                mime="text/csv"
                            )
                        
                        # Aguardar confirmação do usuário
                        st.info("⚠️ Aguardando confirmação do usuário antes de finalizar a execução.")
                        if st.button("✅ Confirmar Resultados"):
                            st.success("Resultados confirmados pelo usuário. Execução finalizada.")
                    else:
                        st.error(f"Erro ao salvar CSV: {resultado}")
                else:
                    st.warning(f"Nenhum processo encontrado ou erro na consulta: {resultado}")
    
    else:  # Consultar todas as UFs
        # Seção para selecionar status
        st.subheader("Selecione o Status para Consulta em Todas as UFs")
        
        # Opção para usar o status padrão de prescrição
        use_default_status = st.checkbox(
            "Usar status padrão de prescrição",
            value=True,
            help=f"Se marcado, usará o valor '{STATUS_PRESCRICAO}' para todas as UFs."
        )
        
        if use_default_status:
            selected_status = STATUS_PRESCRICAO
            st.info(f"Usando status padrão: {STATUS_PRESCRICAO}")
        else:
            # Entrada manual para status
            selected_status = st.text_input(
                "Digite o Status Processual",
                placeholder="Exemplo: Arquivado - Extinção da Punibilidade - Prescrição",
                key="all_ufs_status_input"
            )
            if not selected_status:
                st.error("Por favor, digite um status ou marque a opção para usar o status padrão.")
        
        # Seção de Classes Processuais
        st.sidebar.subheader("Filtro de Classe Processual (opcional)")
        
        # Entrada manual para classe processual
        selected_class = st.sidebar.text_input(
            "Digite a Classe Processual (opcional)",
            placeholder="Exemplo: Procedimento do Juizado Especial Cível",
            key="all_ufs_class_input"
        )
        if not selected_class:
            selected_class = None
        
        # Botão para consulta
        if st.button("Consultar Todas as UFs"):
            if not selected_status:
                st.error("Por favor, digite um status ou marque a opção para usar o status padrão.")
            else:
                # Exibir informações da consulta
                st.subheader("Consultando processos com status para todas as UFs")
                st.info(f"Status selecionado: {selected_status}")
                
                if selected_class:
                    st.info(f"Filtro adicional por classe processual: {selected_class}")
                else:
                    st.info("Sem filtro adicional de classe processual.")
                
                # Realizar a consulta
                with st.spinner("Consultando API do DataJud para todas as UFs..."):
                    resultados, erros = consultar_todas_ufs_com_status(selected_status, selected_class)
                
                if resultados:
                    # Exibir informações sobre os resultados
                    st.success(f"Consulta realizada com sucesso! Encontrados processos em {len(resultados)} UFs.")
                    
                    # Criar tabs para cada UF com resultados
                    tabs = st.tabs(list(resultados.keys()))
                    
                    for i, (uf, df) in enumerate(resultados.items()):
                        with tabs[i]:
                            st.subheader(f"Resultados para {uf}")
                            st.info(f"{len(df)} processos encontrados")
                            st.dataframe(df)
                    
                    # Exibir erros, se houver
                    if erros:
                        st.subheader("UFs com erros ou sem resultados")
                        for uf, erro in erros.items():
                            st.warning(f"{uf}: {erro}")
                    
                    # Aguardar confirmação do usuário
                    st.info("⚠️ Aguardando confirmação do usuário antes de finalizar a execução.")
                    if st.button("✅ Confirmar Resultados"):
                        st.success("Resultados confirmados pelo usuário. Execução finalizada.")
                else:
                    st.warning("Nenhum processo encontrado em nenhuma UF.")
                    
                    # Exibir erros
                    st.subheader("Detalhes dos erros")
                    for uf, erro in erros.items():
                        st.error(f"{uf}: {erro}")
    
    # Informações adicionais
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Sobre a API DataJud**
    
    A API Pública do DataJud é uma ferramenta que disponibiliza ao público o acesso 
    aos metadados dos processos públicos dos tribunais do judiciário brasileiro.
    
    **Sobre a Consulta de Status**
    
    Esta aplicação busca processos com status específicos, com foco especial em "Arquivado - Extinção da Punibilidade - Prescrição",
    utilizando o campo status.descricao conforme recomendado pela documentação oficial.
    """)

if __name__ == "__main__":
    main()
