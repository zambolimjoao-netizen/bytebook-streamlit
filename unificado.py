import streamlit as st
import pandas as pd
import json
import unicodedata
import math
import io
import sqlite3
from collections import Counter
import zipfile

# --- Configuração da Página Streamlit ---
st.set_page_config(
    page_title="Conversor e Gerenciador de Peças",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Funções para Processamento de Dados ---
def extrair_valor(categoria):
    """Extrai o valor antes do hífen em uma string."""
    if pd.isna(categoria):
        return ""
    return str(categoria).split('-')[0].strip()

def normalizar_colunas(df):
    """Normaliza os nomes das colunas, removendo acentos e espaços."""
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8').strip()
        for col in df.columns
    ]
    return df

def encontrar_coluna(df, nome_procurado):
    """Encontra uma coluna no DataFrame com base em um nome aproximado."""
    # Prioriza a correspondência exata (ignorando maiúsculas/minúsculas)
    for col in df.columns:
        if nome_procurado.lower() == col.lower():
            return col
    # Se não encontrar, busca por substring
    for col in df.columns:
        if nome_procurado.lower() in col.lower():
            return col
    return None


def validar_formato_atributos(df):
    """Verifica se há colunas de atributos com formato potencialmente incorreto (ex: AXT_ em vez de ATT_)."""
    import re
    colunas_problematicas = []
    # Regex para encontrar padrões como 'XXX_12345' que não começam com ATT
    padrao_atributo = re.compile(r'^[A-Z]{3}_\d+$', re.IGNORECASE)

    for col in df.columns:
        if padrao_atributo.match(col) and not col.upper().startswith('ATT_'):
            colunas_problematicas.append(col)
    
    return colunas_problematicas

def converter_para_json(df, progress_bar=None, cpf_cnpj_raiz_selecionado=None):
    """Converte um DataFrame em uma lista de dicionários no formato JSON desejado de forma dinâmica."""
    dados_convertidos = []
    seq = 1
    total_rows = len(df)
    current_row_count = 0 # Novo contador para o progresso

    # Identifica colunas de atributos (começam com ATT_)
    colunas_atributos = [col for col in df.columns if col.upper().startswith('ATT_')]

    for _, row in df.iterrows(): # Usar '_' pois o índice original não é mais necessário para o progresso
        atributos = []
        
        for col_name in colunas_atributos:
            if pd.notna(row.get(col_name)):
                valor = row[col_name]
                attr_code = col_name.upper()

                # Determina o tratamento dinamicamente
                valor_str = str(valor).strip().lower()
                if valor_str in ['ok', 'nok']:
                    # Tratamento booleano
                    if valor_str == 'ok':
                        atributos.append({"atributo": attr_code, "valor": "true"})
                    elif valor_str == 'nok':
                        atributos.append({"atributo": attr_code, "valor": "false"})
                elif attr_code == 'ATT_10824':
                    # Tratamento de texto puro para caso especial
                    atributos.append({"atributo": attr_code, "valor": str(valor).strip()})
                else:
                    # Tratamento padrão
                    atributos.append({"atributo": attr_code, "valor": extrair_valor(valor)})

        dado = {
            "seq": seq,
            "descricao": row.get("Descricao", ""),
            "denominacao": row.get("Denominacao", ""),
            "cpfCnpjRaiz": cpf_cnpj_raiz_selecionado if cpf_cnpj_raiz_selecionado else "39318225", # Usa o valor selecionado ou o padrão
            "situacao": "Ativado",
            "modalidade": "IMPORTACAO",
            "ncm": str(row.get("NCM", "")),
            "atributos": atributos,
            "codigosInterno": [str(row.get("PART_NUMBER", ""))],
            "atributosMultivalorados": [],
            "atributosCompostos": [],
            "atributosCompostosMultivalorados": []
        }
        dados_convertidos.append(dado)
        seq += 1
        current_row_count += 1 # Incrementa o contador
        if progress_bar:
            progress_bar.progress(current_row_count / total_rows) # Usa o contador para o progresso
    return dados_convertidos

def criar_df_pecas(json_data):
    """Cria um DataFrame com os dados de peças prontos para o banco de dados, incluindo a descrição."""
    dados_para_excel = []
    for item in json_data:
        part_number = item['codigosInterno'][0] if item['codigosInterno'] else ''
        ncm = item.get('ncm', '')
        descricao = item.get('descricao', '') # Inclui a descrição
        atributos_usados = [attr['atributo'] for attr in item.get('atributos', [])]
        atributos_str = ", ".join(atributos_usados)
        
        dados_processados = {
            'part_number': part_number,
            'descricao': descricao, # Adiciona a descrição
            'ncm': ncm,
            'atributos_usados': atributos_str
        }
        dados_para_excel.append(dados_processados)
    return pd.DataFrame(dados_para_excel)


def validar_json_vs_df(json_data, df):
    """Valida se os dados no JSON correspondem aos do DataFrame processado."""
    # 1. Validação de contagem de linhas
    if len(json_data) != len(df):
        return False, f"Erro de validação: A contagem de itens no JSON ({len(json_data)}) não corresponde à contagem de linhas no DataFrame ({len(df)})."

    # 2. Validação de conteúdo, linha por linha
    for index, json_item in enumerate(json_data):
        df_row = df.iloc[index]
        
        # Comparar campos principais
        part_number_json = str(json_item['codigosInterno'][0]).strip() if json_item['codigosInterno'] else ''
        # Encontrar a coluna PART_NUMBER dinamicamente no DataFrame
        col_part_number_df = encontrar_coluna(df, "PART_NUMBER")
        part_number_df = str(df_row.get(col_part_number_df, "")).strip()
        if part_number_json != part_number_df:
            return False, f"Erro na linha {index+2}: PART_NUMBER não corresponde ('{part_number_json}' vs '{part_number_df}')."

        ncm_json = json_item.get('ncm', '')
        col_ncm_df = encontrar_coluna(df, "NCM")
        ncm_df = str(df_row.get(col_ncm_df, ''))
        if ncm_json != ncm_df:
             return False, f"Erro na linha {index+2}: NCM não corresponde ('{ncm_json}' vs '{ncm_df}')."

        # Comparar descrição e denominação
        descricao_json = json_item.get('descricao', '')
        col_descricao_df = encontrar_coluna(df, "Descricao")
        descricao_df = df_row.get(col_descricao_df, "")
        if descricao_json != descricao_df:
            return False, f"Erro na linha {index+2} (Part Number: {part_number_df}): Descrição não corresponde ('{descricao_json}' vs '{descricao_df}')."

        denominacao_json = json_item.get('denominacao', '')
        col_denominacao_df = encontrar_coluna(df, "Denominacao")
        denominacao_df = df_row.get(col_denominacao_df, "")
        if denominacao_json != denominacao_df:
            return False, f"Erro na linha {index+2} (Part Number: {part_number_df}): Denominação não corresponde ('{denominacao_json}' vs '{denominacao_df}')."

        # Validação de atributos: contagem e valores
        colunas_atributos_df = [col for col in df.columns if col.upper().startswith('ATT_')]
        atributos_esperados = []
        for col_name in colunas_atributos_df:
            if pd.notna(df_row.get(col_name)):
                valor = df_row[col_name]
                attr_code = col_name.upper()
                valor_str = str(valor).strip().lower()

                if valor_str in ['ok', 'nok']:
                    if valor_str == 'ok':
                        atributos_esperados.append({"atributo": attr_code, "valor": "true"})
                    elif valor_str == 'nok':
                        atributos_esperados.append({"atributo": attr_code, "valor": "false"})
                elif attr_code == 'ATT_10824':
                    atributos_esperados.append({"atributo": attr_code, "valor": str(valor).strip()})
                else:
                    atributos_esperados.append({"atributo": attr_code, "valor": extrair_valor(valor)})
        
        atributos_no_json = json_item.get('atributos', [])

        if len(atributos_esperados) != len(atributos_no_json):
            return False, f"Erro na linha {index+2} (Part Number: {part_number_df}): A contagem de atributos não corresponde (Planilha: {len(atributos_esperados)}, JSON: {len(atributos_no_json)})."

        # Ordenar listas de atributos para comparação consistente
        atributos_esperados_sorted = sorted(atributos_esperados, key=lambda x: x['atributo'])
        atributos_no_json_sorted = sorted(atributos_no_json, key=lambda x: x['atributo'])

        if atributos_esperados_sorted != atributos_no_json_sorted:
            return False, (
                f"Erro na linha {index+2} (Part Number: {part_number_df}): Os atributos gerados no JSON não correspondem aos esperados da planilha.\n"
                f"Atributos Esperados (Planilha): {json.dumps(atributos_esperados_sorted, ensure_ascii=False, indent=2)}\n"
                f"Atributos Gerados (JSON): {json.dumps(atributos_no_json_sorted, ensure_ascii=False, indent=2)}"
            )

    return True, "Validação bem-sucedida: Os dados do JSON correspondem aos da planilha."


def get_atributos_from_df(df_original):
    """
    Extrai atributos de colunas do DataFrame original para a tabela COD_ATRIBUTOS
    de forma dinâmica, lidando com os padrões "Nome - COD_ATRIB" e "ATT_...".
    """
    atributos_data = []
    
    for nome_original in df_original.columns:
        nome_upper = nome_original.upper()
        
        # Caso 1: Padrão "Nome - ATT_..."
        if ' - ATT_' in nome_upper:
            try:
                nome_atributo_completo, codigo_atributo = nome_original.rsplit(' - ', 1)
                codigo_atributo_limpo = codigo_atributo.strip()
                col_data = df_original[nome_original].astype(str).str.strip().str.lower()

                if 'ok' in col_data.values or 'nok' in col_data.values:
                    atributos_data.append({'NOME_ATRIBUTO': f"{nome_atributo_completo.strip()} (OK)", 'CODIGO_ATRIB': f"{codigo_atributo_limpo}_true", 'MODALIDADE': 'Importação', 'ORGAO': None})
                    atributos_data.append({'NOME_ATRIBUTO': f"{nome_atributo_completo.strip()} (NOK)", 'CODIGO_ATRIB': f"{codigo_atributo_limpo}_false", 'MODALIDADE': 'Importação', 'ORGAO': None})
                else:
                    atributos_data.append({'NOME_ATRIBUTO': nome_atributo_completo.strip(), 'CODIGO_ATRIB': codigo_atributo_limpo, 'MODALIDADE': 'Importação', 'ORGAO': None})
            except ValueError:
                continue
        # Caso 2: Padrão "ATT_..."
        elif nome_upper.startswith('ATT_'):
            codigo_atributo_limpo = nome_upper
            col_data = df_original[nome_original].astype(str).str.strip().str.lower()

            if 'ok' in col_data.values or 'nok' in col_data.values:
                atributos_data.append({'NOME_ATRIBUTO': f"{codigo_atributo_limpo} (OK)", 'CODIGO_ATRIB': f"{codigo_atributo_limpo}_true", 'MODALIDADE': 'Importação', 'ORGAO': None})
                atributos_data.append({'NOME_ATRIBUTO': f"{codigo_atributo_limpo} (NOK)", 'CODIGO_ATRIB': f"{codigo_atributo_limpo}_false", 'MODALIDADE': 'Importação', 'ORGAO': None})
            else:
                atributos_data.append({'NOME_ATRIBUTO': codigo_atributo_limpo, 'CODIGO_ATRIB': codigo_atributo_limpo, 'MODALIDADE': 'Importação', 'ORGAO': None})

    return pd.DataFrame(atributos_data).drop_duplicates(subset=['CODIGO_ATRIB'])

def converter_para_df_ncm_x_atrib(df_original):
    """
    Converte o DataFrame original em um novo DataFrame com uma linha
    para cada combinação NCM e ATRIBUTO, de forma dinâmica.
    """
    dados_para_tabela = []
    col_ncm = encontrar_coluna(df_original, "NCM")
    
    # Identifica colunas de atributos (começam com ATT_)
    colunas_atributos = [col for col in df_original.columns if col.upper().startswith('ATT_')]

    for _, row in df_original.iterrows():
        ncm = str(row.get(col_ncm, "")).strip()
        if not ncm:
            continue
            
        for col_name in colunas_atributos:
            if pd.notna(row.get(col_name)):
                attr_code = col_name.upper()
                valor = row[col_name]
                valor_str = str(valor).strip().lower()

                if valor_str in ['ok', 'nok']:
                    if valor_str == 'ok':
                        dados_para_tabela.append({'NCM': ncm, 'ATRIB': f"{attr_code}_true"})
                    elif valor_str == 'nok':
                        dados_para_tabela.append({'NCM': ncm, 'ATRIB': f"{attr_code}_false"})
                else:
                    dados_para_tabela.append({'NCM': ncm, 'ATRIB': attr_code})
    
    return pd.DataFrame(dados_para_tabela).drop_duplicates()
    
def converter_df_excel_para_ncm_x_atrib(df_original):
    """
    Converte um DataFrame com múltiplas colunas de atributos
    em um novo DataFrame com uma linha para cada combinação NCM e ATRIBUTO.
    """
    dados_para_tabela = []
    
    # Identifica a coluna NCM e todas as colunas de atributos
    col_ncm = None
    col_atributos = []
    for col in df_original.columns:
        if "NCM" in col.upper():
            col_ncm = col
        elif "ATRIB" in col.upper():
            col_atributos.append(col)
    
    if not col_ncm:
        st.error("Coluna 'NCM' não encontrada na planilha.")
        return pd.DataFrame(columns=['NCM', 'ATRIB'])
    
    for _, row in df_original.iterrows():
        ncm_valor = str(row.get(col_ncm, "")).strip()
        if not ncm_valor:
            continue
            
        for col_atrib in col_atributos:
            atrib_valor = str(row.get(col_atrib, "")).strip()
            if atrib_valor: # Garante que o atributo não seja vazio
                dados_para_tabela.append({
                    'NCM': ncm_valor,
                    'ATRIB': atrib_valor
                })
    
    # Cria o DataFrame e remove linhas duplicadas
    df_result = pd.DataFrame(dados_para_tabela).drop_duplicates()
    return df_result


# --- Funções para o Banco de Dados SQLite ---
def get_db_connection():
    """Cria e retorna uma nova conexão com o banco de dados para cada uso."""
    return sqlite3.connect('bytebook.db')

def create_table_ncm_x_atrib_x_pn():
    """Cria a tabela de pecas se ela não existir, com a nova coluna 'descricao'."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ncm_x_atrib_x_pn (
            part_number TEXT PRIMARY KEY,
            descricao TEXT,
            ncm TEXT,
            atributos_usados TEXT
        )
    ''')
    conn.commit()
    conn.close()

def create_table_cod_atributos():
    """Cria a tabela COD_ATRIBUTOS se ela não existir."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS COD_ATRIBUTOS (
            NOME_ATRIBUTO TEXT,
            CODIGO_ATRIB TEXT PRIMARY KEY,
            MODALIDADE TEXT,
            ORGAO TEXT
        )
    ''')
    conn.commit()
    conn.close()

def create_table_ncm_x_atrib():
    """Cria a tabela NCM_X_ATRIB se ela não existir, com chave primária composta."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS NCM_X_ATRIB (
            NCM TEXT,
            ATRIB TEXT,
            PRIMARY KEY (NCM, ATRIB)
        )
    ''')
    conn.commit()
    conn.close()


def insert_new_items(df_new_items):
    """Insere novos itens na base de dados, ignorando duplicatas."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    novos_itens = 0
    
    sql_insert = "INSERT OR IGNORE INTO ncm_x_atrib_x_pn (part_number, descricao, ncm, atributos_usados) VALUES (?, ?, ?, ?)"
    
    for _, row in df_new_items.iterrows():
        try:
            cursor.execute(sql_insert, (row['part_number'], row['descricao'], row['ncm'], row['atributos_usados']))
            if cursor.rowcount > 0:
                novos_itens += 1
        except Exception as e:
            st.error(f"Erro ao inserir item {row['part_number']}: {e}")
    
    conn.commit()
    conn.close()
    return novos_itens

def insert_data_from_df(df, table_name):
    """Insere dados de um DataFrame em uma tabela especificada."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    novos_itens = 0
    
    # Use tuple(df.columns) para garantir a ordem e evitar erros
    columns = ", ".join(df.columns)
    placeholders = ", ".join("?" * len(df.columns))
    
    sql_insert = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    for _, row in df.iterrows():
        try:
            cursor.execute(sql_insert, tuple(row))
            if cursor.rowcount > 0:
                novos_itens += 1
        except Exception as e:
            st.error(f"Erro ao inserir dados na tabela {table_name}: {e}")
            
    conn.commit()
    conn.close()
    return novos_itens

def get_all_items():
    """Recupera todos os itens da base de dados e os retorna como DataFrame."""
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM ncm_x_atrib_x_pn", conn)
    conn.close()
    
    df.rename(columns={'part_number': 'Part Number', 'atributos_usados': 'Atributos Usados'}, inplace=True)
    return df

def create_table_cnpj_options():
    """Cria a tabela cnpj_options se ela não existir."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cnpj_options (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            cpf_cnpj_raiz TEXT NOT NULL
        )
    ''')
    conn.commit()
    conn.close()

def insert_cnpj_option(name, cpf_cnpj_raiz):
    """Insere uma nova opção de CNPJ/CPF Raiz na tabela cnpj_options."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO cnpj_options (name, cpf_cnpj_raiz) VALUES (?, ?)", (name, cpf_cnpj_raiz))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        st.error(f"Erro: Já existe uma opção com o nome '{name}'.")
        return False
    finally:
        conn.close()

def get_cnpj_options():
    """Recupera todas as opções de CNPJ/CPF Raiz da tabela cnpj_options."""
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT id, name, cpf_cnpj_raiz FROM cnpj_options ORDER BY name", conn)
    conn.close()
    return df

def update_cnpj_option(option_id, new_name, new_cpf_cnpj_raiz):
    """Atualiza uma opção de CNPJ/CPF Raiz existente na tabela cnpj_options."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE cnpj_options SET name = ?, cpf_cnpj_raiz = ? WHERE id = ?", (new_name, new_cpf_cnpj_raiz, option_id))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        st.error(f"Erro: Já existe uma opção com o nome '{new_name}'.")
        return False
    finally:
        conn.close()

def delete_cnpj_option(option_id):
    """Deleta uma opção de CNPJ/CPF Raiz da tabela cnpj_options."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("DELETE FROM cnpj_options WHERE id = ?", (option_id,))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Erro ao deletar a opção: {e}")
        return False
    finally:
        conn.close()

# --- Lógica Principal da Aplicação Streamlit ---

# Garante que as tabelas do banco de dados existam
create_table_ncm_x_atrib_x_pn()
create_table_cod_atributos()
create_table_ncm_x_atrib()
create_table_cnpj_options()

# Inicializa o estado da sessão para armazenar os JSONs gerados
if 'generated_jsons' not in st.session_state:
    st.session_state.generated_jsons = {}
if 'uploader_key' not in st.session_state:
    st.session_state.uploader_key = 0
if 'expand_all' not in st.session_state:
    st.session_state.expand_all = False
if 'selected_cpf_cnpj_raiz' not in st.session_state:
    st.session_state.selected_cpf_cnpj_raiz = None
if 'confirm_delete_cnpj_id' not in st.session_state:
    st.session_state.confirm_delete_cnpj_id = None
if 'split_json_files' not in st.session_state:
    st.session_state.split_json_files = True # Default: quebrar em lotes de 100

# Cria as abas na parte superior
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Processamento de Planilhas", "Gerenciamento do Banco de Dados", "Análises e Estatísticas", "Consulta de Atributos", "Configuração de CNPJ/CPF Raiz"])

# Conteúdo da Aba 1: Processamento de Planilhas
with tab1:
    st.title("Conversor e Atualizador de Base de Dados de Peças")
    st.markdown("Envie uma ou mais planilhas Excel para converter em JSON e atualizar a base de dados.")

    # Seletor de CPF/CNPJ Raiz
    cnpj_options_df = get_cnpj_options()
    if not cnpj_options_df.empty:
        options = cnpj_options_df['name'].tolist()
        selected_name = st.selectbox(
            "Selecione o CPF/CNPJ Raiz para gerar o JSON:",
            options,
            key="cnpj_selector"
        )
        if selected_name:
            st.session_state.selected_cpf_cnpj_raiz = cnpj_options_df[cnpj_options_df['name'] == selected_name]['cpf_cnpj_raiz'].iloc[0]
            st.info(f"CPF/CNPJ Raiz selecionado: **{st.session_state.selected_cpf_cnpj_raiz}**")
    else:
        st.warning("Nenhuma opção de CPF/CNPJ Raiz cadastrada. Por favor, cadastre uma na aba 'Configuração de CNPJ/CPF Raiz'.")
        st.session_state.selected_cpf_cnpj_raiz = None # Garante que não há valor selecionado se a tabela estiver vazia

    st.divider() # Separador visual mais elegante

    st.session_state.split_json_files = st.checkbox(
        "Quebrar arquivos JSON em lotes de 100 itens (desmarque para gerar um único arquivo por planilha)",
        value=st.session_state.split_json_files,
        key="split_json_checkbox"
    )

    uploaded_files = st.file_uploader(
        "Envie suas planilhas Excel",
        type=["xlsx", "csv"],
        accept_multiple_files=True,
        key=f"uploader_{st.session_state.uploader_key}"
    )

    if uploaded_files:
        # Organiza os botões em colunas
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Limpar Lista de Arquivos", width='stretch'):
                st.session_state.uploader_key += 1
                st.session_state.generated_jsons = {}
                st.rerun()
        with col2:
            if st.button("Expandir Todos", width='stretch'):
                st.session_state.expand_all = True
        with col3:
            if st.button("Recolher Todos", width='stretch'):
                st.session_state.expand_all = False

        st.session_state.generated_jsons = {} # Limpa os resultados anteriores a cada novo upload
        
        total_files = len(uploaded_files)
        overall_progress_text = st.empty()
        overall_progress_bar = st.progress(0)

        for i, uploaded_file in enumerate(uploaded_files):
            overall_progress_text.text(f"Processando arquivo {i+1} de {total_files}: {uploaded_file.name}")
            overall_progress_bar.progress((i + 1) / total_files)

            with st.expander(f"Processando arquivo: {uploaded_file.name}", expanded=st.session_state.expand_all):
                with st.spinner("Analisando e processando..."):
                    # Leitura e pré-processamento
                    # Leitura e pré-processamento
                    if uploaded_file.name.endswith('.csv'):
                        # Para CSV, ainda tratamos como uma única "aba"
                        dfs = {uploaded_file.name.rsplit('.', 1)[0]: pd.read_csv(uploaded_file)}
                    else:
                        # Para Excel, lemos todas as abas
                        dfs = pd.read_excel(uploaded_file, sheet_name=None, engine="openpyxl")
                    
                    for sheet_name, df_original in dfs.items():
                        st.subheader(f"Processando aba: **{sheet_name}**")
                        df_original = df_original.dropna(how='all')

                        # Validação de formato de colunas
                        colunas_erradas = validar_formato_atributos(df_original)
                        if colunas_erradas:
                            st.error(f"Erro de Formato na Aba '{sheet_name}' do Arquivo: '{uploaded_file.name}'")
                            st.warning("As seguintes colunas parecem ser atributos, mas estão com formato incorreto (o correto é 'ATT_...'):")
                            st.code(', '.join(colunas_erradas))
                            continue

                        st.info(f"Total de linhas na aba '{sheet_name}': **{len(df_original)}**")
                        # Adiciona a coluna 'ID' sequencial
                        df_original.insert(0, 'ID', range(1, len(df_original) + 1))
                        st.dataframe(df_original, hide_index=True, width='stretch') # Removido .head(3) para mostrar todos os registros
                        df_original = df_original.map(lambda x: x.strip() if isinstance(x, str) else x)

                        col_part_number = encontrar_coluna(df_original, "PART_NUMBER")
                        if not col_part_number:
                            st.error(f"A coluna 'PART_NUMBER' é obrigatória e não foi encontrada na aba '{sheet_name}'.")
                            continue
                        
                        if df_original.duplicated(subset=[col_part_number]).any():
                            st.warning(f"Part Numbers duplicados encontrados na aba '{sheet_name}'. Apenas a primeira ocorrência será processada.")
                        df_original.drop_duplicates(subset=[col_part_number], keep='first', inplace=True)

                        # Normalização e conversão
                        json_progress_text = st.empty()
                        json_progress_bar = st.progress(0)
                        json_progress_text.text(f"Convertendo aba '{sheet_name}' para JSON...")
                        df = normalizar_colunas(df_original.copy())
                        
                        if st.session_state.selected_cpf_cnpj_raiz:
                            json_convertido = converter_para_json(df, json_progress_bar, st.session_state.selected_cpf_cnpj_raiz)
                        else:
                            st.error("Por favor, selecione um CPF/CNPJ Raiz antes de processar a planilha.")
                            continue # Pula para o próximo arquivo ou encerra o processamento
                        
                        json_progress_text.empty() # Remove o texto de progresso
                        json_progress_bar.empty() # Remove a barra de progresso
                        st.success(f"Conversão JSON da aba '{sheet_name}' concluída! {len(json_convertido)} itens processados.", icon="✅")
                        
                        is_valid, message = validar_json_vs_df(json_convertido, df)
                        if not is_valid:
                            st.error(f"Falha na validação da aba '{sheet_name}': {message}", icon="❌")
                            continue
                        else:
                            st.success(f"Validação da aba '{sheet_name}' bem-sucedida: {message}", icon="✅")

                        # Armazena o JSON gerado no estado da sessão
                        nome_base_arquivo = uploaded_file.name.rsplit('.', 1)[0]
                        st.session_state.generated_jsons[f"{nome_base_arquivo}_{sheet_name}"] = json_convertido

                        # Inserção no banco de dados
                        st.markdown("---")
                        st.subheader(f"Atualizando Banco de Dados para a aba '{sheet_name}'")
                        db_progress_text = st.empty()
                        db_progress_bar = st.progress(0)

                        df_novos_itens = criar_df_pecas(json_convertido)
                        db_progress_text.text("Inserindo novas peças...")
                        num_novos_itens = insert_new_items(df_novos_itens)
                        db_progress_bar.progress(0.33)
                        
                        df_atributos_para_inserir = get_atributos_from_df(df_original)
                        db_progress_text.text("Inserindo novos atributos...")
                        num_novos_atributos = insert_data_from_df(df_atributos_para_inserir, 'COD_ATRIBUTOS')
                        db_progress_bar.progress(0.66)
                        
                        df_ncm_x_atrib = converter_para_df_ncm_x_atrib(df_original)
                        db_progress_text.text("Inserindo novas combinações NCM x Atributo...")
                        num_ncm_atrib_novos = insert_data_from_df(df_ncm_x_atrib, 'NCM_X_ATRIB')
                        db_progress_bar.progress(1.0)

                        db_progress_text.empty()
                        db_progress_bar.empty()
                        st.success(f"Atualização do Banco de Dados para a aba '{sheet_name}' concluída!", icon="✅")

                        # Exibição de Resultados
                        st.markdown("---")
                        st.subheader(f"Resumo da Atualização do Banco de Dados para a aba '{sheet_name}'")
                        
                        if num_novos_itens > 0:
                            st.success(f"**Peças:** {num_novos_itens} novos itens adicionados da aba '{sheet_name}'.", icon="✅")
                        else:
                            st.info(f"**Peças:** Base de dados já estava atualizada para a aba '{sheet_name}'.", icon="ℹ️")

                        if num_novos_atributos > 0:
                            st.success(f"**Atributos:** {num_novos_atributos} novos códigos de atributos adicionados da aba '{sheet_name}'.", icon="✅")
                        else:
                            st.info(f"**Atributos:** Tabela de códigos de atributos já estava atualizada para a aba '{sheet_name}'.", icon="ℹ️")

                        if num_ncm_atrib_novos > 0:
                            st.success(f"**NCM x Atributo:** {num_ncm_atrib_novos} novas combinações adicionadas da aba '{sheet_name}'.", icon="✅")
                        else:
                            st.info(f"**NCM x Atributo:** Tabela de combinações já estava atualizada para a aba '{sheet_name}'.", icon="ℹ️")
                        st.markdown("---") # Separador entre abas

        # --- Seção de Download dos Resultados ---
        if st.session_state.generated_jsons:
            st.divider()
            with st.expander("Download dos Resultados Gerados", expanded=True):
                st.subheader("Download dos Arquivos JSON Gerados")

                # Botões de download individuais
                for nome_base, json_data in st.session_state.generated_jsons.items():
                    if st.session_state.split_json_files:
                        tamanho_lote = 100
                        total_lotes = math.ceil(len(json_data) / tamanho_lote)
                        
                        for i in range(total_lotes):
                            inicio = i * tamanho_lote
                            fim = inicio + tamanho_lote
                            lote = json_data[inicio:fim]
                            json_string = json.dumps(lote, ensure_ascii=False, indent=2)
                            nome_arquivo = f"{nome_base}_lote_{i+1}.json"
                            st.download_button(
                                label=f"Baixar {nome_arquivo}",
                                data=json_string,
                                file_name=nome_arquivo,
                                mime="application/json",
                                key=f"download_{nome_base}_{i}"
                            )
                    else:
                        json_string = json.dumps(json_data, ensure_ascii=False, indent=2)
                        nome_arquivo = f"{nome_base}.json"
                        st.download_button(
                            label=f"Baixar {nome_arquivo}",
                            data=json_string,
                            file_name=nome_arquivo,
                            mime="application/json",
                            key=f"download_{nome_base}_single"
                        )

            # Botão para baixar todos como ZIP
            if len(st.session_state.generated_jsons) > 0: # Alterado para > 0, pois pode haver 1 arquivo sem lotes
                total_json_files_in_zip = 0
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zip_file:
                    for nome_base, json_data in st.session_state.generated_jsons.items():
                        if st.session_state.split_json_files:
                            tamanho_lote = 100
                            total_lotes = math.ceil(len(json_data) / tamanho_lote)
                            total_json_files_in_zip += total_lotes # Acumula o total de arquivos
                            for i in range(total_lotes):
                                inicio = i * tamanho_lote
                                fim = inicio + tamanho_lote
                                lote = json_data[inicio:fim]
                                json_string = json.dumps(lote, ensure_ascii=False, indent=2)
                                nome_arquivo = f"{nome_base}_lote_{i+1}.json"
                                zip_file.writestr(nome_arquivo, json_string)
                        else:
                            json_string = json.dumps(json_data, ensure_ascii=False, indent=2)
                            nome_arquivo = f"{nome_base}.json"
                            zip_file.writestr(nome_arquivo, json_string)
                            total_json_files_in_zip += 1 # Acumula o total de arquivos
                
                st.download_button(
                    label=f"Baixar Todos os JSONs ({total_json_files_in_zip} arquivos .zip)", # Rótulo atualizado
                    data=zip_buffer.getvalue(),
                    file_name="todos_jsons.zip",
                    mime="application/zip"
                )
            
                st.divider()
                # Seção de Download da Base de Dados de Peças Atualizada
                st.subheader("Download da Base de Dados de Peças Atualizada")
                
                df_final = get_all_items()
                
                st.info(f"A base de dados atualizada contém {len(df_final)} itens no total.", icon="ℹ️")
                st.dataframe(df_final.tail())

                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    df_final.to_excel(writer, index=False, sheet_name='Base_de_Pecas')
                excel_buffer.seek(0)
                
                st.download_button(
                    label="Baixar Base de Dados de Peças Atualizada (.xlsx)",
                    data=excel_buffer,
                    file_name="base_de_pecas.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
# Conteúdo da Aba 2: Gerenciamento do Banco de Dados
# Início do conteúdo da Aba 2
with tab2:
    st.title("Gerenciamento do Banco de Dados")
    st.markdown("Use esta seção para inspecionar tabelas existentes, executar comandos SQL ou criar novas tabelas diretamente no banco de dados `bytebook.db`.")

    with st.expander("Visualizar Tabelas", expanded=False):
        st.subheader("Visualizar Dados de uma Tabela")
        
        conn = get_db_connection()
        try:
            df_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
            
            if not df_tables.empty:
                tabela_selecionada = st.selectbox(
                    "Selecione uma tabela para visualizar:",
                    df_tables['name']
                )
                
                if tabela_selecionada:
                    st.info("Mostrando os primeiros 10 registros. Use 'Executar SQL' para ver a tabela completa.")
                    st.markdown(f"**Conteúdo da Tabela `{tabela_selecionada}`:**")
                    df_data = pd.read_sql_query(f"SELECT * FROM `{tabela_selecionada}` LIMIT 10;", conn)
                    st.dataframe(df_data)
            else:
                st.info("Nenhuma tabela encontrada no banco de dados.")
        except Exception as e:
            st.error(f"Erro ao listar as tabelas: {e}")
        finally:
            conn.close()

    with st.expander("Executar SQL", expanded=False):
        st.subheader("Executar Comandos SQL")
        query = st.text_area(
            "Digite sua query SQL:", 
            "SELECT name FROM sqlite_master WHERE type='table';"
        )
        
        if st.button("Executar Query"):
            try:
                conn = get_db_connection()
                if query.strip().lower().startswith("select"):
                    df = pd.read_sql_query(query, conn)
                    st.dataframe(df)
                else:
                    cursor = conn.cursor()
                    cursor.execute(query)
                    conn.commit()
                    st.success("Query executada com sucesso.")
            except Exception as e:
                st.error(f"Erro ao executar a query: {e}")
            finally:
                if 'conn' in locals() and conn:
                    conn.close()
    
    with st.expander("Criar Nova Tabela", expanded=False):
        st.subheader("Criar Nova Tabela no Banco de Dados")

        nome_tabela = st.text_input("Nome da nova tabela")
        num_colunas = st.number_input("Número de colunas", min_value=1, max_value=20, step=1)

        colunas = []
        for i in range(int(num_colunas)):
            st.markdown(f"**Coluna {i+1}**")
            nome_coluna = st.text_input(f"Nome da coluna {i+1}", key=f"nome_{i}")
            tipo_coluna = st.selectbox(f"Tipo da coluna {i+1}", ["INTEGER", "TEXT", "REAL", "BLOB", "NUMERIC"], key=f"tipo_{i}")
            colunas.append((nome_coluna, tipo_coluna))

        if st.button("Criar Tabela"):
            if nome_tabela:
                try:
                    conn = get_db_connection()
                    cursor = conn.cursor()
                    colunas_sql = ", ".join([f"`{nome}` {tipo}" for nome, tipo in colunas if nome])
                    sql = f"CREATE TABLE IF NOT EXISTS `{nome_tabela}` ({colunas_sql});"
                    cursor.execute(sql)
                    conn.commit()
                    st.success(f"Tabela '{nome_tabela}' criada com sucesso.")
                except Exception as e:
                    st.error(f"Erro ao criar a tabela: {e}")
                finally:
                    if 'conn' in locals() and conn:
                        conn.close()
            else:
                st.warning("Por favor, insira um nome para a tabela.")

    with st.expander("Carregar Dados da Planilha", expanded=True):
        st.subheader("Carregar Dados de uma Planilha Excel")
        st.markdown("Selecione a tabela de destino e envie um arquivo Excel com colunas que correspondam à sua tabela.")
        
        conn = get_db_connection()
        try:
            df_tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)
            
            if not df_tables.empty:
                tabela_destino = st.selectbox(
                    "Selecione a tabela de destino:",
                    df_tables['name']
                )

                uploaded_file_data = st.file_uploader("Envie seu arquivo Excel (.xlsx)", type=["xlsx", "csv"], key="upload_tab2")

                if uploaded_file_data and tabela_destino:
                    st.markdown("---")
                    st.subheader("Prévia dos dados do arquivo Excel")
                    
                    # Usa o pandas para ler o arquivo do Streamlit
                    if uploaded_file_data.name.endswith('.csv'):
                        df_upload = pd.read_csv(uploaded_file_data)
                    else:
                        df_upload = pd.read_excel(uploaded_file_data, engine="openpyxl")
                    df_upload = df_upload.dropna(how='all')
                    st.dataframe(df_upload.head())

                    if st.button(f"Inserir dados na tabela '{tabela_destino}'"):
                        with st.spinner("Inserindo dados..."):
                            try:
                                # Lógica para processar dados de forma diferente dependendo da tabela
                                if tabela_destino.lower() == 'ncm_x_atrib':
                                    # Usa a nova função para lidar com o formato específico da sua planilha
                                    df_processado = converter_df_excel_para_ncm_x_atrib(df_upload)
                                else:
                                    # Caso contrário, usa o DataFrame original da planilha
                                    df_processado = df_upload

                                # 2. A verificação agora é feita no DataFrame processado
                                cursor = conn.cursor()
                                cursor.execute(f"PRAGMA table_info({tabela_destino});")
                                table_columns = [col[1] for col in cursor.fetchall()]

                                # Normaliza as colunas do DataFrame para correspondência
                                df_processado.columns = [col.lower() for col in df_processado.columns]
                                table_columns_lower = [col.lower() for col in table_columns]
                                
                                missing_columns = [col for col in table_columns_lower if col not in df_processado.columns]
                                
                                if missing_columns:
                                    st.error(f"O arquivo Excel não possui as colunas obrigatórias da tabela: {', '.join(missing_columns)}. Por favor, verifique se a sua planilha contém as colunas para gerar os dados da tabela '{tabela_destino}'.")
                                else:
                                    # Trata colunas extras no DataFrame
                                    extra_columns = [col for col in df_processado.columns if col not in table_columns_lower]
                                    if extra_columns:
                                        st.warning(f"As seguintes colunas do Excel serão ignoradas pois não existem na tabela: {', '.join(extra_columns)}")
                                        df_processado = df_processado[table_columns_lower]
                                    
                                    # Garante que a ordem das colunas seja a mesma da tabela
                                    df_processado.columns = table_columns
                                    
                                    novos_itens = insert_data_from_df(df_processado, tabela_destino)
                                    st.success(f"Dados inseridos com sucesso! {novos_itens} novos registros adicionados à tabela `{tabela_destino}`.")
                                    
                                    # Mostra os dados atualizados
                                    st.markdown(f"**Conteúdo atualizado da Tabela `{tabela_destino}`:**")
                                    df_data = pd.read_sql_query(f"SELECT * FROM {tabela_destino};", conn)
                                    st.dataframe(df_data)

                            except Exception as e:
                                st.error(f"Erro ao inserir os dados: {e}")

            else:
                st.info("Nenhuma tabela encontrada no banco de dados. Crie uma na seção 'Criar Nova Tabela'.")
        except Exception as e:
            st.error(f"Erro ao listar as tabelas: {e}")
        finally:
            if 'conn' in locals() and conn:
                conn.close()

# Conteúdo da Aba 3: Análises e Estatísticas
with tab3:
    st.title("Análises e Estatísticas")
    st.markdown("Esta seção apresenta dados e insights da sua base de dados de peças (`ncm_x_atrib_x_pn`).")

    conn = get_db_connection()
    try:
        # --- Análise de Part Numbers ---
        st.subheader("Part Numbers")
        df_part_numbers = pd.read_sql_query("SELECT part_number FROM ncm_x_atrib_x_pn", conn)
        st.info(f"Total de Part Numbers únicos cadastrados: **{len(df_part_numbers)}**")
        if not df_part_numbers.empty:
            st.dataframe(df_part_numbers.rename(columns={'part_number': 'Part Number'}).head(10))

        # --- Análise de NCMs mais utilizados ---
        st.subheader("NCMs mais utilizados")
        df_ncm_counts = pd.read_sql_query("SELECT ncm, COUNT(*) as Frequencia FROM ncm_x_atrib_x_pn GROUP BY ncm ORDER BY Frequencia DESC", conn)
        st.dataframe(df_ncm_counts)

        # --- Análise de Atributos mais utilizados ---
        st.subheader("Atributos mais utilizados")
        df_all_attributes = pd.read_sql_query("SELECT atributos_usados FROM ncm_x_atrib_x_pn", conn)
        df_cod_atributos = pd.read_sql_query("SELECT CODIGO_ATRIB, NOME_ATRIBUTO FROM COD_ATRIBUTOS", conn)
        
        # Mapeia código para nome do atributo
        attr_mapping = pd.Series(df_cod_atributos.NOME_ATRIBUTO.values, index=df_cod_atributos.CODIGO_ATRIB).to_dict()
        
        all_attributes_list = []
        for index, row in df_all_attributes.iterrows():
            if row['atributos_usados']:
                attributes = [attr.strip() for attr in row['atributos_usados'].split(',')]
                all_attributes_list.extend(attributes)
                
        attribute_counts = Counter(all_attributes_list)
        
        df_attr_counts = pd.DataFrame(attribute_counts.items(), columns=['Atributo', 'Frequência']).sort_values(by='Frequência', ascending=False).reset_index(drop=True)
        
        # Adiciona a coluna de descrição
        df_attr_counts['Descricao'] = df_attr_counts['Atributo'].map(attr_mapping).fillna('Descrição não encontrada')
        
        st.dataframe(df_attr_counts[['Atributo', 'Descricao', 'Frequência']])

        # --- Nova Análise: Atributos por NCM (Visão Agrupada) ---
        st.subheader("Atributos por NCM (Visão Agrupada)")
        df_ncm_atrib = pd.read_sql_query("SELECT NCM, ATRIB FROM NCM_X_ATRIB ORDER BY NCM", conn)
        
        if not df_ncm_atrib.empty:
            # Agrupa os atributos por NCM
            df_grouped = df_ncm_atrib.groupby('NCM')['ATRIB'].apply(lambda x: ', '.join(x)).reset_index()
            df_grouped.rename(columns={'NCM': 'NCM', 'ATRIB': 'Atributos Associados'}, inplace=True)
            st.dataframe(df_grouped)
        else:
            st.info("Não há dados na tabela NCM_X_ATRIB para exibir.")
        
    except Exception as e:
        st.error(f"Erro ao carregar análises: {e}")
    finally:
        conn.close()

# Conteúdo da Aba 4: Consulta de Atributos com Linguagem Natural
with tab4:
    st.title("Consulta Inteligente de Atributos")
    st.markdown("Faça uma pergunta como: `quais são os atributos para o ncm 84143091?`")

    # Importa o módulo de expressões regulares
    import re

    query_text = st.text_input("Faça sua pergunta ou digite um NCM:")

    if st.button("Buscar Atributos"):
        if not query_text:
            st.warning("Por favor, digite um NCM ou faça uma pergunta.")
        else:
            # Tenta extrair um NCM de 8 dígitos da pergunta
            match = re.search(r'\b(\d{8})\b', query_text)
            ncm_encontrado = match.group(1) if match else None

            if not ncm_encontrado:
                # Se não encontrar um NCM de 8 dígitos, assume que o texto é o NCM
                ncm_encontrado = query_text.strip()

            conn = get_db_connection()
            try:
                # Busca os atributos para o NCM encontrado
                query = f"SELECT NCM, ATRIB FROM NCM_X_ATRIB WHERE NCM = '{ncm_encontrado}';"
                df_result = pd.read_sql_query(query, conn)

                if not df_result.empty:
                    st.subheader(f"Atributos para o NCM: {ncm_encontrado}")
                    
                    # Junta com a tabela de nomes de atributos para obter as descrições
                    df_cod_atributos = pd.read_sql_query("SELECT CODIGO_ATRIB, NOME_ATRIBUTO FROM COD_ATRIBUTOS", conn)
                    df_final = pd.merge(df_result, df_cod_atributos, left_on='ATRIB', right_on='CODIGO_ATRIB', how='left')
                    df_final.rename(columns={'NOME_ATRIBUTO': 'Descrição do Atributo', 'ATRIB': 'Código do Atributo'}, inplace=True)
                    
                    # Seleciona e exibe as colunas desejadas
                    st.dataframe(df_final[['Código do Atributo', 'Descrição do Atributo']])
                else:
                    st.info(f"Nenhum atributo encontrado para o NCM '{ncm_encontrado}'.")
            
            except Exception as e:
                st.error(f"Ocorreu um erro na busca: {e}")
            finally:
                conn.close()

# Conteúdo da Aba 5: Configuração de CNPJ/CPF Raiz
with tab5:
    st.title("Configuração de CNPJ/CPF Raiz")
    st.markdown("Cadastre e gerencie as opções de CPF/CNPJ Raiz disponíveis para a geração de JSONs.")

    st.subheader("Cadastrar Nova Opção")
    with st.form("form_add_cnpj_option"):
        new_name = st.text_input("Nome da Opção (ex: Crawl, Kia)")
        new_cpf_cnpj_raiz = st.text_input("CPF/CNPJ Raiz")
        submitted = st.form_submit_button("Adicionar Opção")

        if submitted:
            if new_name and new_cpf_cnpj_raiz:
                if insert_cnpj_option(new_name, new_cpf_cnpj_raiz):
                    st.success(f"Opção '{new_name}' com CPF/CNPJ Raiz '{new_cpf_cnpj_raiz}' adicionada com sucesso!")
                else:
                    st.error("Falha ao adicionar a opção. Verifique se o nome já existe.")
            else:
                st.warning("Por favor, preencha todos os campos.")
    
    st.markdown("---")
    st.subheader("Opções de CPF/CNPJ Raiz Cadastradas")
    
    current_cnpj_options_df = get_cnpj_options()
    if not current_cnpj_options_df.empty:
        # Adiciona uma coluna de índice para seleção
        # Adiciona uma coluna de índice para seleção
        # current_cnpj_options_df_with_id = current_cnpj_options_df.reset_index(drop=True) # Não é mais necessário, o ID já vem do DB
        # current_cnpj_options_df_with_id['id'] = current_cnpj_options_df_with_id.index + 1 # IDs para seleção
        
        st.dataframe(current_cnpj_options_df[['id', 'name', 'cpf_cnpj_raiz']], hide_index=True)

        st.markdown("---")
        st.subheader("Editar ou Deletar Opção")

        # Seletor para escolher a opção a ser editada/deletada
        options_for_selection = current_cnpj_options_df.apply(lambda row: f"{row['id']} - {row['name']} ({row['cpf_cnpj_raiz']})", axis=1).tolist()
        selected_option_str = st.selectbox("Selecione uma opção para editar ou deletar:", options_for_selection, key="edit_delete_selector")

        if selected_option_str:
            selected_id = int(selected_option_str.split(' - ')[0]) # Este é o ID real do banco de dados
            selected_option_data = current_cnpj_options_df[current_cnpj_options_df['id'] == selected_id].iloc[0]
            
            # Formulário de Edição
            with st.form("form_edit_cnpj_option"):
                st.markdown(f"**Editando: {selected_option_data['name']}**")
                edited_name = st.text_input("Novo Nome da Opção", value=selected_option_data['name'], key="edit_name")
                edited_cpf_cnpj_raiz = st.text_input("Novo CPF/CNPJ Raiz", value=selected_option_data['cpf_cnpj_raiz'], key="edit_cpf_cnpj_raiz")
                
                col_edit_btn, col_delete_btn = st.columns(2)
                with col_edit_btn:
                    if st.form_submit_button("Salvar Edição"):
                        if edited_name and edited_cpf_cnpj_raiz:
                            # Usa o selected_id que já é o ID real do banco de dados
                            if update_cnpj_option(selected_id, edited_name, edited_cpf_cnpj_raiz):
                                st.success(f"Opção '{edited_name}' atualizada com sucesso!")
                                st.rerun()
                            else:
                                st.error("Falha ao atualizar a opção. Verifique se o nome já existe.")
                        else:
                            st.warning("Por favor, preencha todos os campos para edição.")
                
                with col_delete_btn:
                    if st.form_submit_button("Deletar Opção"):
                        # Usa o selected_id que já é o ID real do banco de dados
                        if delete_cnpj_option(selected_id):
                            st.success(f"Opção '{selected_option_data['name']}' deletada com sucesso!")
                            st.rerun()
                        else:
                            st.error("Falha ao deletar a opção.")
    else:
        st.info("Nenhuma opção de CPF/CNPJ Raiz cadastrada ainda.")
