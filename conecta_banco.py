import streamlit as st
import sqlite3
import pandas as pd

# Configuração da página
st.set_page_config(layout="wide")

# Conectar ao banco de dados
conn = sqlite3.connect("bytebook.db")
cursor = conn.cursor()

# Menu lateral
aba = st.sidebar.radio("Escolha uma aba", ["Executar SQL", "Criar Nova Tabela"])

# Aba 1: Executar comandos SQL
if aba == "Executar SQL":
    st.title("Executar comandos SQL")
    query = st.text_area("Digite sua query SQL:", "SELECT name FROM sqlite_master WHERE type='table';")
    if st.button("Executar"):
        try:
            if query.strip().lower().startswith("select"):
                df = pd.read_sql_query(query, conn)
                st.dataframe(df)
            else:
                cursor.execute(query)
                conn.commit()
                st.success("Query executada com sucesso.")
        except Exception as e:
            st.error(f"Erro ao executar a query: {e}")

# Aba 2: Criar nova tabela
elif aba == "Criar Nova Tabela":
    st.title("Criar Nova Tabela no Banco de Dados")

    nome_tabela = st.text_input("Nome da nova tabela")
    num_colunas = st.number_input("Número de colunas", min_value=1, max_value=20, step=1)

    colunas = []
    for i in range(int(num_colunas)):
        st.subheader(f"Coluna {i+1}")
        nome_coluna = st.text_input(f"Nome da coluna {i+1}", key=f"nome_{i}")
        tipo_coluna = st.selectbox(f"Tipo da coluna {i+1}", ["INTEGER", "TEXT", "REAL"], key=f"tipo_{i}")
        descricao = st.text_input(f"Descrição (opcional) da coluna {i+1}", key=f"desc_{i}")
        colunas.append((nome_coluna, tipo_coluna))

    if st.button("Criar Tabela"):
        try:
            colunas_sql = ", ".join([f"{nome} {tipo}" for nome, tipo in colunas if nome])
            sql = f"CREATE TABLE IF NOT EXISTS {nome_tabela} ({colunas_sql});"
            cursor.execute(sql)
            conn.commit()
            st.success(f"Tabela '{nome_tabela}' criada com sucesso.")
        except Exception as e:
            st.error(f"Erro ao criar a tabela: {e}")

# Fechar conexão
conn.close()