import streamlit as st
import pandas as pd
import json
import unicodedata
import math
import io

# Função para extrair o valor antes do hífen
def extrair_valor(categoria):
    if pd.isna(categoria):
        return ""
    return str(categoria).split('-')[0].strip()

# Função para normalizar os nomes das colunas
def normalizar_colunas(df):
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8').strip()
        for col in df.columns
    ]
    return df

# Função para encontrar uma coluna pelo nome aproximado
def encontrar_coluna(df, nome_procurado):
    for col in df.columns:
        if nome_procurado.lower() in col.lower():
            return col
    return None

# Função para converter o DataFrame em JSON com regras condicionais
def converter_para_json(df):
    dados_convertidos = []
    seq = 1 #1701

    col_anvisa = encontrar_coluna(df, "Categoria regulatoria - Anvisa")
    col_inmetro = encontrar_coluna(df, "Referencia de licenciamento Inmetro - ATT_13200")
    col_balistica = encontrar_coluna(df, "Balistica - ATT_10627")
    col_destaque_LI_ATT_2802 = encontrar_coluna(df, "Destaque LI - ATT_2802")
    col_detalhe_ATT_2327 = encontrar_coluna(df, "Detalhamento - ATT_2327")
    col_detalhe_ATT_12663 = encontrar_coluna(df, "Detalhamento - ATT_12663")
    col_detalhe_ATT_2604 = encontrar_coluna(df, "Detalhamento - ATT_2604")
    col_detalhe_ATT_2342 = encontrar_coluna(df, "Detalhamento - ATT_2342")
    col_other_ATT_10824 = encontrar_coluna(df, "Especifique outros - ATT_10824")

    for _, row in df.iterrows():
        atributos = []
        
        # Obtenção e verificação de valores
        valor_anvisa = row.get(col_anvisa, None)
        valor_inmetro = row.get(col_inmetro, None)
        valor_detalhe_ATT_2327 = row.get(col_detalhe_ATT_2327, None)
        valor_detalhe_ATT_12663 = row.get(col_detalhe_ATT_12663, None)
        valor_destaque_li_ATT_2802 = row.get(col_destaque_LI_ATT_2802, None)
        valor_detalhe_ATT_2604 = row.get(col_detalhe_ATT_2604, None)
        valor_detalhe_ATT_2342 = row.get(col_detalhe_ATT_2342, None)
        valor_other_ATT_10824 = row.get(col_other_ATT_10824, None)

        if pd.notna(valor_anvisa):
            atributos.append({"atributo": "ATT_14545", "valor": extrair_valor(valor_anvisa)})
        if pd.notna(valor_inmetro):
            atributos.append({"atributo": "ATT_13200", "valor": extrair_valor(valor_inmetro)})

        # Lógica para balística
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


### Interface Streamlit

st.title("Conversor de Planilha Excel (.xlsx) para JSON com divisão por lote")

uploaded_file = st.file_uploader("Envie sua planilha Excel", type="xlsx")

if uploaded_file:
    df = pd.read_excel(uploaded_file, engine="openpyxl")
    df = normalizar_colunas(df)
    df = df.dropna(how='all')

    st.subheader("Colunas após normalização")
    st.write(df.columns.tolist())

    st.subheader("Prévia da Planilha")
    st.dataframe(df)

    json_convertido = converter_para_json(df)

    st.subheader("Prévia do JSON Convertido (primeiros 10 itens)")
    st.json(json_convertido[:10])
    
    # Seção para download
    st.subheader("Download dos Arquivos JSON")

    # Extrai o nome do arquivo enviado sem a extensão
    nome_planilha = uploaded_file.name.rsplit('.', 1)[0]
    
    tamanho_lote = 100
    total_lotes = math.ceil(len(json_convertido) / tamanho_lote)

    for i in range(total_lotes):
        inicio = i * tamanho_lote
        fim = inicio + tamanho_lote
        lote = json_convertido[inicio:fim]

        # Converte o lote para uma string JSON
        json_string = json.dumps(lote, ensure_ascii=False, indent=2)
        
        # Nome do arquivo para download
        nome_arquivo = f"{nome_planilha}_lote_{i+1}.json"
        
        st.download_button(
            label=f"Baixar {nome_arquivo}",
            data=json_string,
            file_name=nome_arquivo,
            mime="application/json"
        )