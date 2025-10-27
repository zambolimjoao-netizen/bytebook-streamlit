import streamlit as st
import pandas as pd
import json
import unicodedata
import math
import io
import sqlite3

# --- Funções para Processamento de Dados ---
# Mantenha as funções 'extrair_valor', 'normalizar_colunas', 'encontrar_coluna',
# 'converter_para_json' e 'criar_df_pecas' exatamente como na sua última versão.

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
    col_anvisa = encontrar_coluna(df, "Categoria regulatoria - Anvisa")
    # Referencia Inmetro
    col_inmetro = encontrar_coluna(df, "Referencia de licenciamento Inmetro - ATT_13200")
    col_inmetro_ATT_13241 = encontrar_coluna(df, "Referencia de licenciamento Inmetro - ATT_13241")
    # Balistica
    col_balistica = encontrar_coluna(df, "Balistica - ATT_10627")
    # Destaque LI
    col_destaque_LI_ATT_2802 = encontrar_coluna(df, "Destaque LI - ATT_2802")
    # Detalhamentos
    col_detalhe_ATT_2327 = encontrar_coluna(df, "Detalhamento - ATT_2327")
    col_detalhe_ATT_12663 = encontrar_coluna(df, "Detalhamento - ATT_12663")
    col_detalhe_ATT_2604 = encontrar_coluna(df, "Detalhamento - ATT_2604")
    col_detalhe_ATT_2342 = encontrar_coluna(df, "Detalhamento - ATT_2342")
    col_detalhe_ATT_2536 = encontrar_coluna(df, "Detalhamento - ATT_2536")
    
    # Especifique Outros
    col_other_ATT_10824 = encontrar_coluna(df, "Especifique outros - ATT_10824")

    for _, row in df.iterrows():
        atributos = []
        # Atribui os valores nas variaveis
        valor_anvisa = row.get(col_anvisa, None)
        # Referencia Inmetro
        valor_inmetro = row.get(col_inmetro, None)
        valor_inmetro_ATT_13241 = row.get(col_inmetro_ATT_13241, None)
        # Detalhamentos
        valor_detalhe_ATT_2327 = row.get(col_detalhe_ATT_2327, None)
        valor_detalhe_ATT_12663 = row.get(col_detalhe_ATT_12663, None)
        valor_detalhe_ATT_2604 = row.get(col_detalhe_ATT_2604, None)
        valor_detalhe_ATT_2342 = row.get(col_detalhe_ATT_2342, None)
        valor_detalhe_ATT_2536 = row.get(col_detalhe_ATT_2536, None)
        # Destaque LI
        valor_destaque_li_ATT_2802 = row.get(col_destaque_LI_ATT_2802, None)
        # Especifique Outros
        valor_other_ATT_10824 = row.get(col_other_ATT_10824, None)

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

        if pd.notna(valor_detalhe_ATT_2327):
            atributos.append({"atributo": "ATT_2327", "valor": extrair_valor(valor_detalhe_ATT_2327)})
        if pd.notna(valor_detalhe_ATT_12663):
            atributos.append({"atributo": "ATT_12663", "valor": extrair_valor(valor_detalhe_ATT_12663)})
        if pd.notna(valor_other_ATT_10824):
            atributos.append({"atributo": "ATT_10824", "valor": extrair_valor(valor_other_ATT_10824)})
        if pd.notna(valor_destaque_li_ATT_2802):
            atributos.append({"atributo": "ATT_2802", "valor": extrair_valor(valor_destaque_li_ATT_2802)})
        if pd.notna(valor_detalhe_ATT_2604):
            atributos.append({"atributo": "ATT_2604", "valor": extrair_valor(valor_detalhe_ATT_2604)})
        if pd.notna(valor_detalhe_ATT_2342):
            atributos.append({"atributo": "ATT_2342)", "valor": extrair_valor(valor_detalhe_ATT_2342)})
        if pd.notna(valor_detalhe_ATT_2536):
            atributos.append({"atributo": "ATT_2536)", "valor": extrair_valor(valor_detalhe_ATT_2536)})

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
    """Cria um DataFrame com os dados de peças prontos para o banco de dados."""
    dados_para_excel = []
    for item in json_data:
        part_number = item['codigosInterno'][0] if item['codigosInterno'] else ''
        ncm = item.get('ncm', '')
        atributos_usados = [attr['atributo'] for attr in item.get('atributos', [])]
        atributos_str = ", ".join(atributos_usados)
        
        dados_processados = {
            'part_number': part_number,
            'ncm': ncm,
            'atributos_usados': atributos_str
        }
        dados_para_excel.append(dados_processados)
    return pd.DataFrame(dados_para_excel)


# --- Funções para o Banco de Dados SQLite ---

def get_db_connection():
    """Cria e retorna uma conexão com o banco de dados."""
    return sqlite3.connect('bytebook.db')

def create_table():
    """Cria a tabela de pecas se ela não existir."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pecas (
            part_number TEXT PRIMARY KEY,
            ncm TEXT,
            atributos_usados TEXT
        )
    ''')
    conn.commit()
    conn.close()

def insert_new_items(df_new_items):
    """Insere novos itens na base de dados, ignorando duplicatas."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    novos_itens = 0
    
    # Prepara a instrução SQL com 'INSERT OR IGNORE'
    sql_insert = "INSERT OR IGNORE INTO pecas (part_number, ncm, atributos_usados) VALUES (?, ?, ?)"
    
    # Itera sobre o DataFrame e tenta inserir cada linha
    for _, row in df_new_items.iterrows():
        try:
            cursor.execute(sql_insert, (row['part_number'], row['ncm'], row['atributos_usados']))
            if cursor.rowcount > 0:
                novos_itens += 1
        except Exception as e:
            st.error(f"Erro ao inserir item {row['part_number']}: {e}")
    
    conn.commit()
    conn.close()
    return novos_itens


def get_all_items():
    """Recupera todos os itens da base de dados e os retorna como DataFrame."""
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM pecas", conn)
    conn.close()
    
    # Renomeia as colunas para exibição no Streamlit
    df.rename(columns={'part_number': 'Part Number', 'atributos_usados': 'Atributos Usados'}, inplace=True)
    return df

# --- Lógica Principal da Aplicação Streamlit ---

st.title("Conversor e Atualizador de Base de Dados de Peças")
st.markdown("Envie uma planilha Excel para converter seus dados para o formato JSON e atualizar automaticamente uma base de dados interna de peças.")

# Garante que a tabela do banco de dados exista
create_table()

uploaded_file = st.file_uploader("Envie sua planilha Excel", type="xlsx")

if uploaded_file:
    df = pd.read_excel(uploaded_file, engine="openpyxl")
    # Remove espaços em branco de todas as células do tipo texto
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df = normalizar_colunas(df)
    df = df.dropna(how='all')

    # --- Identificação e tratamento de colunas essenciais ---
    col_part_number = encontrar_coluna(df, "PART_NUMBER")
    col_descricao = encontrar_coluna(df, "Descricao")
    col_denominacao = encontrar_coluna(df, "Denominacao")
    col_ncm = encontrar_coluna(df, "NCM")

    if not col_part_number:
        st.error("A coluna 'PART_NUMBER' é obrigatória e não foi encontrada.")
        st.stop()

    # Renomeia as colunas para um padrão esperado para garantir que o .get() funcione
    rename_map = {}
    # Apenas renomeia se a coluna foi encontrada
    if col_part_number: rename_map[col_part_number] = 'PART_NUMBER'
    if col_descricao: rename_map[col_descricao] = 'Descricao'
    if col_denominacao: rename_map[col_denominacao] = 'Denominacao'
    if col_ncm: rename_map[col_ncm] = 'NCM'
    df.rename(columns=rename_map, inplace=True)

    # Informa sobre duplicatas antes de remover
    if df.duplicated(subset=['PART_NUMBER']).any():
        st.warning("Foram encontrados Part Numbers duplicados. Apenas a primeira ocorrência de cada um será processada.")
    
    # Remove duplicatas com base no PART_NUMBER, mantendo a primeira ocorrência
    df.drop_duplicates(subset=['PART_NUMBER'], keep='first', inplace=True)

    st.subheader("Prévia da Planilha de Entrada (sem duplicatas)")
    st.dataframe(df.head())
    
    # Converte o DataFrame para os formatos JSON e de base de dados
    json_convertido = converter_para_json(df)
    df_novos_itens = criar_df_pecas(json_convertido)
    
    # Tenta inserir os novos itens no banco de dados
    num_novos_itens = insert_new_items(df_novos_itens)

    st.success(f"{num_novos_itens} novos itens foram adicionados à base de dados com sucesso!")
    if num_novos_itens < len(df_novos_itens):
        st.warning(f"{len(df_novos_itens) - num_novos_itens} itens já existiam na base de dados e não foram incluídos novamente.")

    # --- Seção de Download do JSON ---
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
    
    # --- Seção de Download da Base de Dados de Peças Atualizada ---
    st.subheader("Download da Base de Dados de Peças Atualizada")
    
    # Recupera todos os dados do banco de dados
    df_final = get_all_items()
    
    st.info(f"A base de dados atualizada contém {len(df_final)} itens no total.")
    st.dataframe(df_final.tail()) # Mostra os últimos itens adicionados

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
