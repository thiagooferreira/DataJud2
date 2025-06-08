import streamlit as st
import requests
import pandas as pd
import json
import os
import datetime
import time
from urllib.parse import quote
import itertools
import numpy as np

# Configurações da API
API_KEY = "cDZHYzlZa0JadVREZDJCendQbXY6SkJlTzNjLV9TRENyQk1RdnFKZGRQdw=="
QUERY_SIZE = 100  # Define quantos resultados buscar por página (aumentado para 100)
MAX_PAGES = 20   # Limite máximo de páginas para evitar sobrecarga

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
        st.error(f"UF {uf} não encontrada no dicionário de endpoints.")
        return None
    return f"https://api-publica.datajud.cnj.jus.br/api_publica_{endpoint}/_search"

def create_query_specific(status_descricao=None, search_after=None):
    """Cria uma query DSL específica para busca por status.descricao apenas."""
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": []
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
    
    # Filtro para status.descricao
    if status_descricao:
        query["query"]["bool"]["must"].append({
            "match": {
                "status.descricao": status_descricao
            }
        })
    
    return query

def create_query_fallback(movimentacoes_texto=None, search_after=None):
    """Cria uma query DSL de fallback usando movimentacoes.texto."""
    query = {
        "size": 100,
        "query": {
            "bool": {
                "must": []
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
    
    # Filtro de fallback para movimentacoes.texto
    if movimentacoes_texto:
        query["query"]["bool"]["must"].append({
            "match": {
                "movimentos.nome": movimentacoes_texto
            }
        })
    
    return query
def fetch_all_pages_specific(api_url, status_descricao, progress_bar=None):
    """Busca todos os resultados com paginação usando search_after e filtro específico de status.descricao."""
    all_hits = []
    page = 1
    search_after = None
    total_hits = 0
    
    while True:
        # Criar query para a página atual
        query = create_query_specific(status_descricao, search_after)
        
        # Fazer a requisição à API
        response = fetch_datajud_data(api_url, query)
        
        if not response or 'hits' not in response or 'hits' not in response['hits']:
            break
        
        hits = response['hits']['hits']
        if not hits:
            break
        
        # Adicionar resultados à lista
        all_hits.extend(hits)
        total_hits = response['hits']['total']['value']
        
        # Atualizar barra de progresso se fornecida
        if progress_bar is not None:
            progress = min(1.0, len(all_hits) / total_hits) if total_hits > 0 else 1.0
            progress_bar.progress(progress)
        
        # Verificar se há mais páginas
        if len(hits) < 100:  # Tamanho da página
            break
        
        # Obter o último valor para search_after
        search_after = hits[-1]['sort']
        page += 1
        
        # Log para debug
        logging.info(f"Buscando página {page}, total de resultados até agora: {len(all_hits)}")
    
    return all_hits

def fetch_all_pages_fallback(api_url, movimentacoes_texto, progress_bar=None):
    """Busca todos os resultados com paginação usando search_after e filtro de fallback em movimentacoes.texto."""
    all_hits = []
    page = 1
    search_after = None
    total_hits = 0
    
    while True:
        # Criar query para a página atual
        query = create_query_fallback(movimentacoes_texto, search_after)
        
        # Fazer a requisição à API
        response = fetch_datajud_data(api_url, query)
        
        if not response or 'hits' not in response or 'hits' not in response['hits']:
            break
        
        hits = response['hits']['hits']
        if not hits:
            break
        
        # Adicionar resultados à lista
        all_hits.extend(hits)
        total_hits = response['hits']['total']['value']
        
        # Atualizar barra de progresso se fornecida
        if progress_bar is not None:
            progress = min(1.0, len(all_hits) / total_hits) if total_hits > 0 else 1.0
            progress_bar.progress(progress)
        
        # Verificar se há mais páginas
        if len(hits) < 100:  # Tamanho da página
            break
        
        # Obter o último valor para search_after
        search_after = hits[-1]['sort']
        page += 1
        
        # Log para debug
        logging.info(f"Buscando página {page} (fallback), total de resultados até agora: {len(all_hits)}")
    
    return all_hits
            "bool": {
                "must": []
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
    
    # Adiciona filtro de classes processuais se fornecido
    if classes_processuais and len(classes_processuais) > 0:
        should_clauses = []
        for classe in classes_processuais:
            should_clauses.append({
                "match": {  # Usando match em vez de match_phrase para busca parcial
                    "classe.nome": classe
                }
            })
        
        # Se houver mais de uma classe, usa should (OR) entre elas
        if len(should_clauses) > 1:
            query["query"]["bool"]["must"].append({
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            })
        else:
            # Se for apenas uma classe, adiciona diretamente
            query["query"]["bool"]["must"].append(should_clauses[0])
    
    # Adiciona filtro de status processuais se fornecido
    if status_processuais and len(status_processuais) > 0:
        should_clauses = []
        for status in status_processuais:
            should_clauses.append({
                "match": {  # Usando match em vez de match_phrase para busca parcial
                    "movimentos.nome": status
                }
            })
        
        # Se houver mais de um status, usa should (OR) entre eles
        if len(should_clauses) > 1:
            query["query"]["bool"]["must"].append({
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            })
        else:
            # Se for apenas um status, adiciona diretamente
            query["query"]["bool"]["must"].append(should_clauses[0])
    
    return query

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
    
    # Query alternativa para buscar alguns documentos e extrair classes deles
    sample_query = {
        "size": 100,  # Buscar 100 documentos para extrair classes
        "query": {
            "match_all": {}
        },
        "_source": ["classe.nome"]  # Buscar apenas o campo classe.nome
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
        st.warning(f"Não foi possível obter a lista de classes processuais via agregação. Tentando método alternativo...")
    
    # Se falhar, tenta com amostragem de documentos
    try:
        response = requests.post(api_url, headers=headers, json=sample_query, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        classes = set()
        if 'hits' in data and 'hits' in data['hits']:
            for hit in data['hits']['hits']:
                source = hit.get('_source', {})
                classe = source.get('classe', {})
                if isinstance(classe, dict) and 'nome' in classe:
                    classes.add(classe['nome'])
        
        if classes:
            return sorted(list(classes))
        else:
            st.warning("Não foi possível obter a lista de classes processuais.")
            return []
    except Exception as e:
        st.error(f"Erro ao buscar classes processuais: {e}")
        return []

def fetch_all_status(api_url):
    """Busca todos os status processuais disponíveis para o tribunal."""
    # Query para buscar apenas os status distintos
    aggs_query = {
        "size": 0,  # Não precisamos dos documentos, apenas da agregação
        "aggs": {
            "status": {
                "terms": {
                    "field": "movimentos.nome",
                    "size": 1000  # Buscar até 1000 status distintos
                }
            }
        }
    }
    
    # Query alternativa para buscar alguns documentos e extrair status deles
    sample_query = {
        "size": 100,  # Buscar 100 documentos para extrair status
        "query": {
            "match_all": {}
        },
        "_source": ["movimentos"]  # Buscar apenas o campo movimentos
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
        
        # Extrair os status da resposta
        if 'aggregations' in data and 'status' in data['aggregations'] and 'buckets' in data['aggregations']['status']:
            status_list = [bucket['key'] for bucket in data['aggregations']['status']['buckets']]
            return sorted(status_list)
    except Exception as e:
        st.warning(f"Não foi possível obter a lista de status processuais via agregação. Tentando método alternativo...")
    
    # Se falhar, tenta com amostragem de documentos
    try:
        response = requests.post(api_url, headers=headers, json=sample_query, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        status_set = set()
        if 'hits' in data and 'hits' in data['hits']:
            for hit in data['hits']['hits']:
                source = hit.get('_source', {})
                movimentos = source.get('movimentos', [])
                if isinstance(movimentos, list):
                    for movimento in movimentos:
                        if isinstance(movimento, dict) and 'nome' in movimento:
                            status_set.add(movimento['nome'])
        
        if status_set:
            return sorted(list(status_set))
        else:
            st.warning("Não foi possível obter a lista de status processuais.")
            return []
    except Exception as e:
        st.error(f"Erro ao buscar status processuais: {e}")
        return []

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
        st.error(error_msg)
        return None
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON da resposta da API: {e}")
        return None

def fetch_all_pages(api_url, classes_processuais, status_processuais=None, progress_bar=None):
    """Busca todas as páginas de resultados com paginação usando search_after."""
    all_hits = []
    total_hits = 0
    current_page = 0
    
    # Primeira consulta para obter o total de resultados
    first_query = create_query(classes_processuais, status_processuais)
    first_data = fetch_datajud_data(api_url, first_query)
    
    if not first_data or 'hits' not in first_data or 'total' not in first_data['hits']:
        return []
    
    # Extrair o total de resultados e os primeiros hits
    if isinstance(first_data['hits']['total'], dict) and 'value' in first_data['hits']['total']:
        total_hits = first_data['hits']['total']['value']
    else:
        total_hits = first_data['hits']['total']
    
    # Adicionar os primeiros resultados
    if 'hits' in first_data['hits'] and first_data['hits']['hits']:
        all_hits.extend(first_data['hits']['hits'])
        
        # Obter o valor de sort do último documento para search_after
        last_sort = first_data['hits']['hits'][-1].get('sort')
    else:
        # Se não houver resultados, retorna lista vazia
        return []
    
    # Calcular o número de páginas necessárias
    total_pages = min(MAX_PAGES, (total_hits + QUERY_SIZE - 1) // QUERY_SIZE)
    
    if progress_bar:
        progress_bar.progress(1 / total_pages if total_pages > 0 else 1.0)
    
    # Buscar as páginas restantes usando search_after
    for page in range(1, total_pages):
        current_page = page
        
        # Se não tiver valor de sort para continuar, interrompe
        if not last_sort:
            break
            
        # Pequena pausa para não sobrecarregar a API
        time.sleep(0.5)
        
        # Criar query com search_after para a próxima página
        query = create_query(classes_processuais, status_processuais, last_sort)
        data = fetch_datajud_data(api_url, query)
        
        if data and 'hits' in data and 'hits' in data['hits'] and data['hits']['hits']:
            all_hits.extend(data['hits']['hits'])
            
            # Atualizar o valor de sort para a próxima página
            last_sort = data['hits']['hits'][-1].get('sort')
            
            if progress_bar:
                progress_bar.progress((page + 1) / total_pages)
        else:
            # Se não houver mais resultados, interrompe
            break
    
    return all_hits

def flatten_json(nested_json, prefix=''):
    """Transforma um JSON aninhado em um dicionário plano para melhor visualização."""
    flattened = {}
    
    for key, value in nested_json.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_json(item, f"{new_key}[{i}]"))
                else:
                    flattened[f"{new_key}[{i}]"] = item
        else:
            flattened[new_key] = value
            
    return flattened

def process_data(all_hits):
    """Processa os dados JSON retornados e extrai as informações completas."""
    if not all_hits:
        st.warning("Nenhum resultado encontrado na resposta da API.")
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
            except Exception:
                pass
        
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
                st.warning(f"Não foi possível expandir a coluna de movimentos: {e}")
    
    return df

def save_to_csv(df, uf, classes_processuais):
    """Salva o DataFrame em um arquivo CSV."""
    # Cria o diretório se não existir
    os.makedirs("/home/ubuntu/consultas_datajud", exist_ok=True)
    
    # Formata o nome do arquivo
    if classes_processuais and len(classes_processuais) > 0:
        # Limita o nome do arquivo para não ficar muito grande
        if len(classes_processuais) == 1:
            classe_slug = classes_processuais[0].lower().replace(" ", "_")[:30]
        else:
            classe_slug = f"multiplas_classes_{len(classes_processuais)}"
    else:
        classe_slug = "sem_filtro"
        
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"/home/ubuntu/consultas_datajud/resultados_uf_{uf}_{classe_slug}_{timestamp}.csv"
    
    try:
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        return filename
    except Exception as e:
        st.error(f"Erro ao salvar o arquivo CSV: {e}")
        return None

def main():
    st.set_page_config(
        page_title="Consulta DataJud - API Pública",
        page_icon="⚖️",
        layout="wide"
    )
    
    st.title("Consulta à API Pública do DataJud")
    st.markdown("""
    Esta aplicação permite consultar processos judiciais através da API pública do DataJud, 
    filtrando por Unidade da Federação (UF) e Classes Processuais.
    """)
    
    # Sidebar para filtros
    st.sidebar.header("Filtros de Consulta")
    
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
            # Carregar classes processuais disponíveis para a UF selecionada
            with st.spinner("Carregando dados do tribunal..."):
                col1, col2 = st.columns(2)
                with col1:
                    available_classes = fetch_all_classes(api_url)
                    if not available_classes:
                        st.sidebar.warning("Não foi possível carregar as classes processuais. Você pode prosseguir sem filtro de classe.")
                
                with col2:
                    available_status = fetch_all_status(api_url)
                    if not available_status:
                        st.sidebar.warning("Não foi possível carregar os status processuais. Você pode prosseguir sem filtro de status.")
    
    # Seção de Classes Processuais
    st.sidebar.subheader("Filtro de Classes Processuais")
    
    # Opção para entrada manual ou seleção de classes processuais
    class_input_method = st.sidebar.radio(
        "Método de seleção de Classes Processuais",
        ["Selecionar da lista", "Digitar manualmente"],
        index=0 if available_classes else 1,
        help="Escolha como deseja informar as classes processuais",
        key="class_input_method"
    )
    
    selected_classes = []
    if class_input_method == "Selecionar da lista":
        if available_classes:
            selected_classes = st.sidebar.multiselect(
                "Selecione as Classes Processuais",
                options=available_classes,
                help="Você pode selecionar múltiplas classes ou deixar em branco para buscar todos os processos",
                key="class_multiselect"
            )
        else:
            st.sidebar.warning("Não foi possível carregar a lista de classes. Por favor, use a opção de digitação manual.")
    else:
        # Entrada manual com exemplos
        manual_class_input = st.sidebar.text_area(
            "Digite as Classes Processuais (uma por linha)",
            placeholder="Exemplo:\nProcedimento do Juizado Especial Cível\nProcedimento Comum Cível\nExecução Fiscal",
            help="Digite cada classe processual em uma linha separada",
            key="manual_class_input"
        )
        if manual_class_input:
            # Divide o texto em linhas e remove espaços em branco
            selected_classes = [cls.strip() for cls in manual_class_input.split('\n') if cls.strip()]
    
    # Seção de Status Processuais
    st.sidebar.subheader("Filtro de Status Processuais")
    
    # Opção para entrada manual ou seleção de status processuais
    status_input_method = st.sidebar.radio(
        "Método de seleção de Status Processuais",
        ["Selecionar da lista", "Digitar manualmente"],
        index=0 if available_status else 1,
        help="Escolha como deseja informar os status processuais",
        key="status_input_method"
    )
    
    selected_status = []
    if status_input_method == "Selecionar da lista":
        if available_status:
            selected_status = st.sidebar.multiselect(
                "Selecione os Status Processuais",
                options=available_status,
                help="Você pode selecionar múltiplos status ou deixar em branco para buscar todos os processos",
                key="status_multiselect"
            )
        else:
            st.sidebar.warning("Não foi possível carregar a lista de status. Por favor, use a opção de digitação manual.")
    else:
        # Entrada manual com exemplos
        manual_status_input = st.sidebar.text_area(
            "Digite os Status Processuais (um por linha)",
            placeholder="Exemplo:\nJulgado\nExtinto\nArquivado Definitivamente",
            help="Digite cada status processual em uma linha separada",
            key="manual_status_input"
        )
        if manual_status_input:
            # Divide o texto em linhas e remove espaços em branco
            selected_status = [status.strip() for status in manual_status_input.split('\n') if status.strip()]
    
    # Botão para busca específica por status de prescrição
    if st.sidebar.button("🎯 Busca Específica: Extinção da Punibilidade - Prescrição"):
        if not selected_uf:
            st.error("Por favor, selecione uma UF para continuar.")
            return
        
        if not api_url:
            return
        
        # Executar busca específica conforme instruções do Planner Module
        st.subheader("🎯 Busca Específica por Status de Prescrição")
        st.info("Executando busca específica por processos com status 'Arquivado - Extinção da Punibilidade - Prescrição'")
        
        # Importar e executar função específica
        try:
            from validation_functions import execute_specific_search
            
            specific_results = execute_specific_search(api_url, "Arquivado - Extinção da Punibilidade - Prescrição")
            
            if specific_results:
                # Processar os dados retornados
                df = process_data(specific_results)
                
                if df is not None and not df.empty:
                    st.success(f"✅ Busca específica concluída! {len(specific_results)} processos encontrados.")
                    
                    # Exibir os dados
                    st.subheader("Resultados da Busca Específica")
                    st.dataframe(df)
                    
                    # Aguardar confirmação do usuário conforme Planner Module
                    st.info("⚠️ Aguardando confirmação do usuário antes de finalizar a execução.")
                    
                    if st.button("✅ Confirmar Resultados"):
                        st.success("Resultados confirmados pelo usuário. Execução finalizada.")
                        
                        # Opção para exportar
                        if st.button("📥 Exportar para CSV"):
                            csv_path = save_to_csv(df, selected_uf, ["Extinção da Punibilidade - Prescrição"])
                            if csv_path:
                                st.success(f"Dados exportados para: {csv_path}")
                                with open(csv_path, "rb") as file:
                                    st.download_button(
                                        label="Baixar CSV",
                                        data=file,
                                        file_name=os.path.basename(csv_path),
                                        mime="text/csv"
                                    )
                else:
                    st.warning("Dados encontrados, mas houve problema no processamento.")
            else:
                st.error("Nenhum processo encontrado com o status específico solicitado.")
                
        except ImportError:
            st.error("Erro ao importar funções de validação. Verifique a implementação.")
    
    # Botão original para consulta geral
    if st.sidebar.button("Consultar"):
        if not selected_uf:
            st.error("Por favor, selecione uma UF para continuar.")
            return
        
        if not api_url:
            return
        
        # Exibir informações da consulta
        if selected_classes:
            st.info(f"Consultando processos para UF: {selected_uf}, Classes Processuais: {', '.join(selected_classes[:3])}" + 
                   (f" e mais {len(selected_classes)-3} classes..." if len(selected_classes) > 3 else ""))
        else:
            st.info(f"Consultando todos os processos para UF: {selected_uf} (sem filtro de classe)")
        
        # Garantir que os filtros sejam listas válidas
        if not isinstance(selected_classes, list):
            selected_classes = []
        if not isinstance(selected_status, list):
            selected_status = []
        
        # Realizar a consulta à API com paginação
        progress_bar = st.progress(0)
        with st.spinner("Consultando API do DataJud e buscando todas as páginas de resultados..."):
            all_hits = fetch_all_pages(api_url, selected_classes, selected_status, progress_bar)
        
        if all_hits:
            # Processar os dados retornados
            try:
                df = process_data(all_hits)
                
                # Validar se o DataFrame foi criado corretamente
                if df is not None and not df.empty:
                    # Exibir informações sobre os resultados
                    st.success(f"Consulta realizada com sucesso! {len(df)} processos encontrados em múltiplas páginas.")
                    
                    # Exibir os dados em uma tabela
                    st.subheader("Resultados da Consulta")
                    
                    # Mostrar número total de resultados e aviso sobre paginação
                    st.info(f"Exibindo {len(df)} resultados de um total de {len(all_hits)} processos encontrados.")
                    
                    # Tentar exibir o DataFrame com tratamento de erro
                    try:
                        st.dataframe(df)
                    except Exception as e:
                        st.error(f"Erro ao exibir os dados: {e}")
                        st.info("Tentando exibir uma versão simplificada dos dados...")
                        
                        # Criar uma versão simplificada do DataFrame
                        simplified_df = pd.DataFrame()
                        for col in df.columns:
                            try:
                                # Tentar converter colunas complexas para string
                                simplified_df[col] = df[col].astype(str)
                            except:
                                # Se falhar, pular a coluna
                                continue
                        
                        if not simplified_df.empty:
                            st.dataframe(simplified_df)
                        else:
                            st.warning("Não foi possível exibir os dados em formato de tabela.")
                    
                    # Botão para exportar para CSV
                    if st.button("Exportar para CSV"):
                        try:
                            csv_path = save_to_csv(df, selected_uf, selected_classes)
                            if csv_path:
                                st.success(f"Dados exportados com sucesso para: {csv_path}")
                                
                                # Oferecer download do arquivo
                                with open(csv_path, "rb") as file:
                                    st.download_button(
                                        label="Baixar CSV",
                                        data=file,
                                        file_name=os.path.basename(csv_path),
                                        mime="text/csv"
                                    )
                        except Exception as e:
                            st.error(f"Erro ao exportar para CSV: {e}")
                else:
                    st.warning("Nenhum processo encontrado com os filtros informados.")
                    st.info("Sugestões: tente selecionar outras classes processuais ou status, ou remover alguns filtros.")
            except Exception as e:
                st.error(f"Erro ao processar os dados retornados: {e}")
                st.info("Dados brutos encontrados, mas houve problema no processamento. Tente ajustar os filtros.")
        else:
            st.warning("Nenhum resultado encontrado ou erro na consulta.")
            st.info("Sugestões: verifique se a UF selecionada está correta e tente remover alguns filtros.")
    
    # Informações adicionais
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **Sobre a API DataJud**
    
    A API Pública do DataJud é uma ferramenta que disponibiliza ao público o acesso 
    aos metadados dos processos públicos dos tribunais do judiciário brasileiro.
    
    **Sobre a Paginação**
    
    Esta aplicação busca automaticamente todas as páginas de resultados disponíveis 
    (até um limite de segurança) para garantir que você tenha acesso a todos os processos.
    """)

if __name__ == "__main__":
    main()
