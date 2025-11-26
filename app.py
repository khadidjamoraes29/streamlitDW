import streamlit as st
import pandas as pd
from sqlalchemy import create_engine


# Criar engine de conexão
engine = create_engine("mysql+pymysql://root:Khadidja@127.0.0.1/filmes_dw")

st.title("📊 Análises Dimensionais - Data Warehouse de Filmes")

# Seleção de dimensão
dimensao = st.selectbox(
    "Escolha uma dimensão para analisar:",
    [
        "Diretor",
        "Idioma",
        "Gênero",
        "País",
        "Empresa",
        "Estrela",
        "Roteirista",
        "Tempo",
        "Filmes"
    ]
)

# ==============================
# Funções de consulta
# ==============================

def load_dataframe(query):
    return pd.read_sql(query, engine)

# ==============================
# Exemplos prontos de análises
# ==============================

if dimensao == "Diretor":
    st.subheader("🎬 Bilheteria por Diretor")
    query = """
        SELECT d.nome_diretor,
               SUM(f.bilheteria_mundial) AS total_bilheteria
        FROM fato_filme f
        JOIN dim_diretor d ON d.id_diretor_sk = f.id_diretor_sk
        GROUP BY d.nome_diretor
        ORDER BY total_bilheteria DESC
        LIMIT 20;
    """
    df = load_dataframe(query)
    st.dataframe(df)
    st.bar_chart(df.set_index("nome_diretor"))

elif dimensao == "Idioma":
    st.subheader("🌍 Média de Nota por Idioma")
    query = """
        SELECT i.nome_idioma,
               AVG(f.nota_imdb) AS media_nota
        FROM fato_filme f
        JOIN dim_idioma i ON i.id_idioma_sk = f.id_idioma_sk
        GROUP BY i.nome_idioma
        ORDER BY media_nota DESC;
    """
    df = load_dataframe(query)
    st.dataframe(df)
    st.bar_chart(df.set_index("nome_idioma"))

elif dimensao == "Tempo":
    st.subheader("📅 Bilheteria por Ano")
    query = """
        SELECT t.ano, SUM(f.bilheteria_mundial) AS total_bilheteria
        FROM fato_filme f
        JOIN dim_tempo t ON t.id_tempo_sk = f.id_tempo_sk
        GROUP BY t.ano
        ORDER BY t.ano;
    """
    df = load_dataframe(query)
    st.dataframe(df)
    st.line_chart(df.set_index("ano"))

elif dimensao == "Filmes":
    st.subheader("🎥 Ranking dos 20 Filmes com Maior Bilheteria")
    query = """
        SELECT fi.titulo,
               SUM(f.bilheteria_mundial) AS total_bilheteria
        FROM fato_filme f
        JOIN dim_filme fi ON fi.id_filme_sk = f.id_filme_sk
        GROUP BY fi.titulo
        ORDER BY total_bilheteria DESC
        LIMIT 20;
    """
    df = load_dataframe(query)
    st.dataframe(df)
    st.bar_chart(df.set_index("titulo"))
