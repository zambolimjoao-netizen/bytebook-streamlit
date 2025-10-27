import streamlit as st
import pandas as pd
import json
import unicodedata
import math
import io
import sqlite3
from collections import Counter

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
    for col in df.columns:
        if nome_procurado.lower() in col.lower():
            return col
    return None

def converter_para_json(df):
    """Converte um DataFrame em uma lista de dicionários no formato JSON desejado."""
    dados_convertidos = []
    seq = 1
    
    # Define os nomes das colunas de onde iremos buscar
    col_anvisa = encontrar_coluna(df, "ATT_14545")
    if col_anvisa is None:
        col_anvisa = encontrar_coluna(df, "Categoria regulatoria - Anvisa")
    col_inmetro = encontrar_coluna(df, "Referencia de licenciamento Inmetro - ATT_13200")
    col_inmetro_ATT_13241 = encontrar_coluna(df, "Referencia de licenciamento Inmetro - ATT_13241")
    col_balistica = encontrar_coluna(df, "Balistica - ATT_10627")
    col_destaque_LI_ATT_2802 = encontrar_coluna(df, "Destaque LI - ATT_2802")
    col_destaque_LI_ATT_2640 = encontrar_coluna(df, "Destaque LI - ATT_2640")
    col_destaque_LI_ATT_2708 = encontrar_coluna(df, "Destaque LI - ATT_2708")
    col_detalhe_ATT_2327 = encontrar_coluna(df, "Detalhamento - ATT_2327")
    col_detalhe_ATT_12663 = encontrar_coluna(df, "Detalhamento - ATT_12663")
    col_detalhe_ATT_2604 = encontrar_coluna(df, "Detalhamento - ATT_2604")
    col_detalhe_ATT_2342 = encontrar_coluna(df, "Detalhamento - ATT_2342")
    col_detalhe_ATT_2536 = encontrar_coluna(df, "Detalhamento - ATT_2536")
    col_detalhe_ATT_2307 = encontrar_coluna(df, "Detalhamento - ATT_2307")
    col_detalhe_ATT_2707 = encontrar_coluna(df, "Detalhamento - ATT_2707")
    col_detalhe_ATT_2265 = encontrar_coluna(df, "Detalhamento - ATT_2265")
    col_other_ATT_10824 = encontrar_coluna(df, "Especifique outros - ATT_10824")
    col_cas_ATT_8571 = encontrar_coluna(df, "Número CAS (quando aplicável) - ATT_8571")
    col_hum_ATT_14880 = encontrar_coluna(df, "Para acondicionar alimento de uso humano - ATT_14880")
    col_mili_ATT_9764 = encontrar_coluna(df, "Uso militar - ATT_9764")

    for _, row in df.iterrows():
        atributos = []
        valor_anvisa = row.get(col_anvisa, None)
        valor_inmetro = row.get(col_inmetro, None)
        valor_inmetro_ATT_13241 = row.get(col_inmetro_ATT_13241, None)
        valor_detalhe_ATT_2327 = row.get(col_detalhe_ATT_2327, None)
        valor_detalhe_ATT_12663 = row.get(col_detalhe_ATT_12663, None)
        valor_detalhe_ATT_2604 = row.get(col_detalhe_ATT_2604, None)
        valor_detalhe_ATT_2342 = row.get(col_detalhe_ATT_2342, None)
        valor_detalhe_ATT_2536 = row.get(col_detalhe_ATT_2536, None)
        valor_detalhe_ATT_2307 = row.get(col_detalhe_ATT_2307, None)
        valor_detalhe_ATT_2707 = row.get(col_detalhe_ATT_2707, None)
        valor_detalhe_ATT_2265 = row.get(col_detalhe_ATT_2265, None)
        valor_destaque_li_ATT_2802 = row.get(col_destaque_LI_ATT_2802, None)
        valor_destaque_li_ATT_2640 = row.get(col_destaque_LI_ATT_2640, None)
        valor_destaque_li_ATT_2708 = row.get(col_destaque_LI_ATT_2708, None)
        valor_other_ATT_10824 = row.get(col_other_ATT_10824, None)
        valor_cas_ATT_8571 = row.get(col_cas_ATT_8571, None)
        valor_hum_ATT_14880 = row.get(col_hum_ATT_14880, None)
        valor_mili_ATT_9764 = row.get(col_mili_ATT_9764, None)

        if pd.notna(valor_anvisa):
            atributos.append({"atributo": "ATT_14545", "valor": extrair_valor(valor_anvisa)})
        if pd.notna(valor_inmetro):
            atributos.append({"atributo": "ATT_13200", "valor": extrair_valor(valor_inmetro)})
        if pd.notna(valor_inmetro_ATT_13241):
            atributos.append({"atributo": "ATT_13241", "valor": extrair_valor(valor_inmetro_ATT_13241)})

        if col_balistica in row:
            valor_balistica = str(row[col_balistica]).strip().lower()
            if valor_balistica == 'ok':
                atributos.append({"atributo": "ATT_10627", "valor": "true"})
            elif valor_balistica == 'nok':
                atributos.append({"atributo": "ATT_10627", "valor": "false"})

        if pd.notna(valor_hum_ATT_14880):
            valor_hum = str(valor_hum_ATT_14880).strip().lower()
            if valor_hum == 'ok':
                atributos.append({"atributo": "ATT_14880", "valor": "true"})
            elif valor_hum == 'nok':
                atributos.append({"atributo": "ATT_14880", "valor": "false"})

        if pd.notna(valor_mili_ATT_9764):
            valor_mili = str(valor_mili_ATT_9764).strip().lower()
            if valor_mili == 'ok':
                atributos.append({"atributo": "ATT_9764", "valor": "true"})
            elif valor_mili == 'nok':
                atributos.append({"atributo": "ATT_9764", "valor": "false"})
        
         # Adiciona os atributos de detalhamento se disponíveis

        if pd.notna(valor_detalhe_ATT_2327):
            atributos.append({"atributo": "ATT_2327", "valor": extrair_valor(valor_detalhe_ATT_2327)})
        if pd.notna(valor_detalhe_ATT_12663):
            atributos.append({"atributo": "ATT_12663", "valor": extrair_valor(valor_detalhe_ATT_12663)})
        if pd.notna(valor_other_ATT_10824):
        # Apenas pegue o valor, sem extrair
            atributos.append({"atributo": "ATT_10824", "valor": str(valor_other_ATT_10824).strip()})
        if pd.notna(valor_destaque_li_ATT_2802):
            atributos.append({"atributo": "ATT_2802", "valor": extrair_valor(valor_destaque_li_ATT_2802)})
        if pd.notna(valor_destaque_li_ATT_2640):
            atributos.append({"atributo": "ATT_2640", "valor": extrair_valor(valor_destaque_li_ATT_2640)})
        if pd.notna(valor_destaque_li_ATT_2708):
            atributos.append({"atributo": "ATT_2708", "valor": extrair_valor(valor_destaque_li_ATT_2708)})
        if pd.notna(valor_detalhe_ATT_2604):
            atributos.append({"atributo": "ATT_2604", "valor": extrair_valor(valor_detalhe_ATT_2604)})
        if pd.notna(valor_detalhe_ATT_2342):
            atributos.append({"atributo": "ATT_2342", "valor": extrair_valor(valor_detalhe_ATT_2342)})
        if pd.notna(valor_detalhe_ATT_2536):
            atributos.append({"atributo": "ATT_2536", "valor": extrair_valor(valor_detalhe_ATT_2536)})
        if pd.notna(valor_detalhe_ATT_2307):
            atributos.append({"atributo": "ATT_2307", "valor": extrair_valor(valor_detalhe_ATT_2307)})
        if pd.notna(valor_detalhe_ATT_2707):
            atributos.append({"atributo": "ATT_2707", "valor": extrair_valor(valor_detalhe_ATT_2707)})
        if pd.notna(valor_detalhe_ATT_2265):
            atributos.append({"atributo": "ATT_2265", "valor": extrair_valor(valor_detalhe_ATT_2265)})
         # Adiciona o atributo CAS se disponível
        if pd.notna(valor_cas_ATT_8571):
            atributos.append({"atributo": "ATT_8571", "valor": extrair_valor(valor_cas_ATT_8571)})
        

        dado = {
            "seq": seq,
            "descricao": row.get("Descricao", ""),
            "denominacao": row.get("Denominacao", ""),
            "cpfCnpjRaiz": "39318225",
            "situacao": "Ativado",
            "modalidade": "IMPORTACAO",
            "ncm": str(row.get("NCM", "")),
            "atributos": atributos,
            "codigosInterno": [row.get("PART_NUMBER", "")],
            "atributosMultivalorados": [],
            "atributosCompostos": [],
            "atributosCompostosMultivalorados": []
        }
        dados_convertidos.append(dado)
        seq += 1
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


def get_atributos_from_df(df_original):
    """
    Extrai atributos de colunas do DataFrame original para a tabela COD_ATRIBUTOS
    de forma dinâmica, baseando-se no padrão "Nome - COD_ATRIB".
    """
    atributos_data = []
    
    for nome_original in df_original.columns:
        if '-' in nome_original:
            try:
                # Separa o nome do atributo e o código
                nome_atributo_completo, codigo_atributo = nome_original.rsplit(' - ', 1)
                codigo_atributo_limpo = codigo_atributo.strip()

                # Verificar se a coluna contém valores 'ok' ou 'nok'
                col_data = df_original[nome_original].astype(str).str.strip().str.lower()
                
                # Se a coluna contiver 'ok' ou 'nok', crie dois atributos
                if 'ok' in col_data.values or 'nok' in col_data.values:
                    atributos_data.append({
                        'NOME_ATRIBUTO': f"{nome_atributo_completo.strip()} (OK)",
                        'CODIGO_ATRIB': f"{codigo_atributo_limpo}_true",
                        'MODALIDADE': 'Importação',
                        'ORGAO': None
                    })
                    atributos_data.append({
                        'NOME_ATRIBUTO': f"{nome_atributo_completo.strip()} (NOK)",
                        'CODIGO_ATRIB': f"{codigo_atributo_limpo}_false",
                        'MODALIDADE': 'Importação',
                        'ORGAO': None
                    })
                else:
                    # Caso contrário, adicione o atributo normalmente
                    atributos_data.append({
                        'NOME_ATRIBUTO': nome_atributo_completo.strip(),
                        'CODIGO_ATRIB': codigo_atributo_limpo,
                        'MODALIDADE': 'Importação',
                        'ORGAO': None
                    })
            except ValueError:
                # Ignora colunas que não seguem o padrão esperado
                continue
    
    # Remove duplicatas com base no código do atributo
    return pd.DataFrame(atributos_data).drop_duplicates(subset=['CODIGO_ATRIB'])

def converter_para_df_ncm_x_atrib(df_original):
    """
    Converte o DataFrame original em um novo DataFrame
    com uma linha para cada combinação NCM e ATRIBUTO.
    """
    dados_para_tabela = []
    
    # Encontra as colunas de NCM e atributos
    col_ncm = encontrar_coluna(df_original, "NCM")
    
    # Dicionário de mapeamento de nomes de colunas para códigos de atributos
    # Isso garante que a normalização aconteça corretamente
    col_anvisa_ncm = encontrar_coluna(df_original, "ATT_14545")
    if col_anvisa_ncm is None:
        col_anvisa_ncm = encontrar_coluna(df_original, "Categoria regulatoria - Anvisa")
    
    col_map_to_atrib = {
        col_anvisa_ncm: "ATT_14545",
        encontrar_coluna(df_original, "Referencia de licenciamento Inmetro - ATT_13200"): "ATT_13200",
        encontrar_coluna(df_original, "Referencia de licenciamento Inmetro - ATT_13241"): "ATT_13241",
        encontrar_coluna(df_original, "Detalhamento - ATT_2327"): "ATT_2327",
        encontrar_coluna(df_original, "Detalhamento - ATT_12663"): "ATT_12663",
        encontrar_coluna(df_original, "Detalhamento - ATT_2604"): "ATT_2604",
        encontrar_coluna(df_original, "Detalhamento - ATT_2342"): "ATT_2342",
        encontrar_coluna(df_original, "Detalhamento - ATT_2536"): "ATT_2536",
        encontrar_coluna(df_original, "Especifique outros - ATT_10824"): "ATT_10824",
        encontrar_coluna(df_original, "Destaque LI - ATT_2802"): "ATT_2802",
        encontrar_coluna(df_original, "Balistica - ATT_10627"): "ATT_10627"
    }
    
    for _, row in df_original.iterrows():
        ncm = str(row.get(col_ncm, "")).strip()
        if not ncm:
            continue
            
        atributos = []
        
        # Coleta todos os atributos de forma similar à sua função `converter_para_json`
        for col, atrib_code in col_map_to_atrib.items():
            if col and pd.notna(row.get(col, None)):
                if atrib_code == "ATT_10627":
                    valor_balistica = str(row[col]).strip().lower()
                    if valor_balistica == 'ok':
                        atributos.append("ATT_10627_true")
                    elif valor_balistica == 'nok':
                        atributos.append("ATT_10627_false")
                else:
                    atributos.append(atrib_code)
        
        # Quebra os atributos e adiciona as linhas ao novo DataFrame
        for atrib in atributos:
            if atrib: # Garante que o atributo não seja vazio
                dados_para_tabela.append({
                    'NCM': ncm,
                    'ATRIB': atrib
                })
    
    df_result = pd.DataFrame(dados_para_tabela).drop_duplicates()
    return df_result
    
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
    """Cria a tabela NCM_X_ATRIB se ela não existir."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS NCM_X_ATRIB (
            NCM TEXT,
            ATRIB TEXT
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

# --- Lógica Principal da Aplicação Streamlit ---

# Garante que as tabelas do banco de dados existam
create_table_ncm_x_atrib_x_pn()
create_table_cod_atributos()
create_table_ncm_x_atrib()

# Cria as abas na parte superior
tab1, tab2, tab3, tab4 = st.tabs(["Processamento de Planilhas", "Gerenciamento do Banco de Dados", "Análises e Estatísticas", "Consulta de Atributos"])

# Conteúdo da Aba 1: Processamento de Planilhas
with tab1:
    st.title("Conversor e Atualizador de Base de Dados de Peças")
    st.markdown("Envie uma planilha Excel para converter seus dados para o formato JSON e atualizar automaticamente uma base de dados interna de peças.")

    uploaded_file = st.file_uploader("Envie sua planilha Excel", type=["xlsx", "csv"])

    if uploaded_file:
        with st.spinner("Processando planilha... Por favor, aguarde."):
            # Prévia da planilha original (sem normalização)
            if uploaded_file.name.endswith('.csv'):
                df_original = pd.read_csv(uploaded_file)
            else:
                df_original = pd.read_excel(uploaded_file, engine="openpyxl")
            df_original = df_original.dropna(how='all')
            
            st.subheader("Prévia da Planilha Original")
            st.dataframe(df_original.head())

            # Remove espaços em branco de todas as células do tipo texto
            df_original = df_original.applymap(lambda x: x.strip() if isinstance(x, str) else x)

            # --- Identificação e tratamento de colunas essenciais ---
            col_part_number = encontrar_coluna(df_original, "PART_NUMBER")

            if not col_part_number:
                st.error("A coluna 'PART_NUMBER' é obrigatória e não foi encontrada.")
                st.stop()

            # Informa sobre duplicatas antes de remover
            if df_original.duplicated(subset=[col_part_number]).any():
                st.warning("Foram encontrados Part Numbers duplicados. Apenas a primeira ocorrência de cada um será processada.")
            
            # Remove duplicatas com base no PART_NUMBER, mantendo a primeira ocorrência
            df_original.drop_duplicates(subset=[col_part_number], keep='first', inplace=True)

            # Normalização e processamento para JSON
            df = normalizar_colunas(df_original.copy()) # Usar uma cópia para não alterar o df_original com a normalização
            
            st.subheader("Prévia da Planilha Processada (Sem Duplicatas e com Nomes de Coluna Normalizados)")
            st.dataframe(df.head())
            
            # --- Inserção de dados na tabela de peças (ncm_x_atrib_x_pn) ---
            json_convertido = converter_para_json(df)
            df_novos_itens = criar_df_pecas(json_convertido)
            num_novos_itens = insert_new_items(df_novos_itens)

            # --- Inserção de dados na tabela de atributos (COD_ATRIBUTOS) ---
            df_atributos_para_inserir = get_atributos_from_df(df_original)
            num_novos_atributos = insert_data_from_df(df_atributos_para_inserir, 'COD_ATRIBUTOS')
            
            # --- Inserção de dados na nova tabela NCM_X_ATRIB ---
            df_ncm_x_atrib = converter_para_df_ncm_x_atrib(df_original)
            num_ncm_atrib_novos = insert_data_from_df(df_ncm_x_atrib, 'NCM_X_ATRIB')


        st.success(f"{num_novos_itens} novos itens de peças foram adicionados à base de dados com sucesso!")
        if num_novos_itens < len(df_novos_itens):
            st.warning(f"{len(df_novos_itens) - num_novos_itens} itens de peças já existiam e foram ignorados.")
        
        st.success(f"{num_novos_atributos} novos atributos foram adicionados à tabela COD_ATRIBUTOS com sucesso!")
        if num_novos_atributos < len(df_atributos_para_inserir):
             st.warning(f"{len(df_atributos_para_inserir) - num_novos_atributos} atributos já existiam e foram ignorados.")

        st.success(f"{num_ncm_atrib_novos} novas combinações NCM-Atributo foram adicionadas com sucesso!")
        
        # Seção de Download do JSON
        st.subheader("Download dos Arquivos JSON")
        
        nome_planilha = uploaded_file.name.rsplit('.', 1)[0]
        tamanho_lote = 100
        total_lotes = math.ceil(len(json_convertido) / tamanho_lote)

        for i in range(total_lotes):
            inicio = i * tamanho_lote
            fim = inicio + tamanho_lote
            lote = json_convertido[inicio:fim]

            json_string = json.dumps(lote, ensure_ascii=False, indent=2)
            nome_arquivo = f"{nome_planilha}_lote_{i+1}.json"
            
            st.download_button(
                label=f"Baixar {nome_arquivo}",
                data=json_string,
                file_name=nome_arquivo,
                mime="application/json"
            )
        
        # Seção de Download da Base de Dados de Peças Atualizada
        st.subheader("Download da Base de Dados de Peças Atualizada")
        
        df_final = get_all_items()
        
        st.info(f"A base de dados atualizada contém {len(df_final)} itens no total.")
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
        
    except Exception as e:
        st.error(f"Erro ao carregar análises: {e}")
    finally:
        conn.close()

# Conteúdo da Aba 4: Consulta de Atributos
with tab4:
    st.title("Consulta de Atributos por NCM ou Descrição")
    st.markdown("Digite um NCM completo ou uma parte da descrição para encontrar os atributos associados a ele.")

    busca_ncm = st.text_input("Buscar por NCM:")
    busca_descricao = st.text_input("Buscar por Descrição:")

    if st.button("Buscar Atributos"):
        if not busca_ncm and not busca_descricao:
            st.warning("Por favor, digite um NCM ou uma descrição para realizar a busca.")
        else:
            conn = get_db_connection()
            try:
                if busca_ncm:
                    # Busca por NCM na tabela NCM_X_ATRIB
                    query = f"SELECT NCM, ATRIB FROM NCM_X_ATRIB WHERE NCM LIKE '%{busca_ncm}%';"
                    df_result = pd.read_sql_query(query, conn)
                
                elif busca_descricao:
                    # Busca por Descrição na tabela ncm_x_atrib_x_pn
                    query = f"SELECT ncm, atributos_usados FROM ncm_x_atrib_x_pn WHERE descricao LIKE '%{busca_descricao}%';"
                    df_descricao_result = pd.read_sql_query(query, conn)
                    
                    if not df_descricao_result.empty:
                        # Extrai NCMs e atributos únicos
                        ncms = df_descricao_result['ncm'].unique()
                        all_attrs = []
                        for attrs in df_descricao_result['atributos_usados']:
                            all_attrs.extend([a.strip() for a in attrs.split(',')])
                        
                        df_result = pd.DataFrame({
                            'NCM': [ncm for ncm in ncms for _ in range(len(all_attrs))],
                            'ATRIB': all_attrs * len(ncms)
                        }).drop_duplicates()
                    else:
                        df_result = pd.DataFrame() # DataFrame vazio se a busca por descrição não retornar nada

                if not df_result.empty:
                    # Junta com a tabela de nomes de atributos para obter as descrições
                    df_cod_atributos = pd.read_sql_query("SELECT CODIGO_ATRIB, NOME_ATRIBUTO FROM COD_ATRIBUTOS", conn)
                    df_final = pd.merge(df_result, df_cod_atributos, left_on='ATRIB', right_on='CODIGO_ATRIB', how='left')
                    df_final.rename(columns={'NOME_ATRIBUTO': 'Descrição do Atributo', 'ATRIB': 'Código do Atributo'}, inplace=True)
                    df_final = df_final[['NCM', 'Código do Atributo', 'Descrição do Atributo']]
                    
                    st.subheader("Resultados da Busca")
                    st.dataframe(df_final)
                else:
                    st.info("Nenhum atributo encontrado para o termo de busca.")
            
            except Exception as e:
                st.error(f"Ocorreu um erro na busca: {e}")
            finally:
                conn.close()