import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine

# conexão (ajuste se necessário)
engine = create_engine("mysql+pymysql://root:Khadidja@127.0.0.1/filmes_dw")

st.set_page_config(page_title="DW Filmes - OLAP (Streamlit)", layout="wide")
st.title("🎬 DW Filmes — Análises Dimensionais")

def load_dataframe(query):
    return pd.read_sql(query, engine)

consulta = st.selectbox(
    "Escolha a análise:",
    [
        "1 - Diretor x Gênero (Bilheteria por diretor por gênero)",
        "2 - Empresa x Ano (Bilheteria por estúdio ao longo do tempo)",
        "3 - País x Nota IMDb (Média de nota por país)",
        "4 - Top Estrelas (Atores com maior bilheteria)",
        "5 - Produtividade de Estúdios (Qtd filmes)",
        "6 - Roteirista x Bilheteria (Roteiristas com maiores qtd. de filmes ao longo dos anos)",
        "7 - Idioma x Gênero (Distribuição idioma vs gênero)",
        "8 - Filmes indicados por País e Ano (Indicações por país/ano)"
    ]
)

if consulta.startswith("1 -"):
    st.header("1 — Diretor x Gênero: Top Diretores por Gênero")

    # --- Buscar todos os gêneros do banco para o dropdown ---
    generos_df = load_dataframe("SELECT DISTINCT nome_genero FROM dim_genero ORDER BY nome_genero;")
    lista_generos = ["Mostrar apenas os 8 principais"] + generos_df["nome_genero"].tolist()

    genero_escolhido = st.sidebar.selectbox(
        "Selecione um gênero (opcional):",
        lista_generos
    )

    top_n = st.sidebar.number_input(
        "Quantidade de diretores por gênero (Top N)",
        min_value=1, max_value=20, value=3
    )

    # --- Lógica dos 8 principais gêneros ---
    if genero_escolhido == "Mostrar apenas os 8 principais":
        query_generos_top = """
            SELECT nome_genero
            FROM (
                SELECT 
                    g.nome_genero,
                    SUM(f.bilheteria_mundial) AS total_bilheteria
                FROM bridge_filme_genero bg
                JOIN dim_genero g ON bg.id_genero_sk = g.id_genero_sk
                JOIN dim_filme fi ON bg.id_filme_sk = fi.id_filme_sk
                JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
                GROUP BY g.nome_genero
                ORDER BY total_bilheteria DESC
                LIMIT 8
            ) AS sub;
        """
        top_generos_df = load_dataframe(query_generos_top)
        filtro_generos_sql = "WHERE g.nome_genero IN (" + \
            ",".join([f"'{g}'" for g in top_generos_df.nome_genero]) + ")"
    else:
        filtro_generos_sql = f"WHERE g.nome_genero = '{genero_escolhido}'"

    # --- Consulta principal com ranking ---
    query = f"""
        WITH ranking AS (
            SELECT 
                g.nome_genero,
                d.nome_diretor,
                SUM(f.bilheteria_mundial) AS total_bilheteria,
                COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes,
                ROW_NUMBER() OVER (
                    PARTITION BY g.nome_genero
                    ORDER BY SUM(f.bilheteria_mundial) DESC
                ) AS rn
            FROM bridge_filme_genero bg
            JOIN dim_genero g ON bg.id_genero_sk = g.id_genero_sk
            JOIN dim_filme fi ON bg.id_filme_sk = fi.id_filme_sk
            JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
            JOIN dim_diretor d ON d.id_diretor_sk = f.id_diretor_sk
            {filtro_generos_sql}
            GROUP BY g.nome_genero, d.nome_diretor
        )
        SELECT * 
        FROM ranking
        WHERE rn <= {top_n}
        ORDER BY nome_genero, total_bilheteria DESC;
    """

    df = load_dataframe(query)
    st.dataframe(df)

    # --- Gráfico ---
    if df.empty:
        st.warning("Nenhum dado encontrado para esse filtro.")
    else:
        fig = px.bar(
            df,
            x="nome_diretor",
            y="total_bilheteria",
            color="nome_genero",
            title=f"Top {top_n} Diretores por Gênero",
            barmode="group"
        )
        st.plotly_chart(fig)


elif consulta.startswith("2 -"):
    st.header("2 — Empresa x Ano: Bilheteria por Estúdio ao longo do tempo")

    # 1) Controle: Top N
    top_n_empresas = st.sidebar.number_input(
        "Top N empresas por bilheteria total",
        min_value=3, max_value=50,
        value=5
    )

    # 2) Buscar Top N empresas (default)
    query_top = f"""
        SELECT 
            e.nome_empresa,
            SUM(f.bilheteria_mundial) AS total_bilheteria
        FROM bridge_filme_empresa be
        JOIN dim_empresa e ON be.id_empresa_sk = e.id_empresa_sk
        JOIN dim_filme fi ON be.id_filme_sk = fi.id_filme_sk
        JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
        GROUP BY e.nome_empresa
        ORDER BY total_bilheteria DESC
        LIMIT {top_n_empresas};
    """
    df_top = load_dataframe(query_top)
    top_empresas = df_top["nome_empresa"].tolist()

    # 3) Buscar TODAS as empresas para o Dropdown
    query_all = "SELECT nome_empresa FROM dim_empresa ORDER BY nome_empresa;"
    df_all = load_dataframe(query_all)
    todas_empresas = df_all["nome_empresa"].tolist()

    # 4) Campo de comparação (default = vazio)
    empresas_comparacao = st.sidebar.multiselect(
        "Comparar empresas (substitui Top N):",
        options=todas_empresas,
        default=[]
    )

    # 5) Lógica: se o user selecionar algo → substitui Top N
    if empresas_comparacao:
        empresas_final = empresas_comparacao
    else:
        empresas_final = top_empresas

    # 6) Montar string SQL
    empresas_sql_list = "', '".join(empresas_final)

    # 7) Query final
    query = f"""
        SELECT 
            e.nome_empresa,
            t.ano,
            SUM(f.bilheteria_mundial) AS total_bilheteria,
            COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes
        FROM bridge_filme_empresa be
        JOIN dim_empresa e ON be.id_empresa_sk = e.id_empresa_sk
        JOIN dim_filme fi ON be.id_filme_sk = fi.id_filme_sk
        JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
        JOIN dim_tempo t ON t.id_tempo_sk = f.id_tempo_sk
        WHERE e.nome_empresa IN ('{empresas_sql_list}')
        GROUP BY e.nome_empresa, t.ano
        ORDER BY e.nome_empresa, t.ano;
    """

    df = load_dataframe(query)
    st.dataframe(df)

    # 8) Gráfico
    fig = px.line(
        df,
        x="ano",
        y="total_bilheteria",
        color="nome_empresa",
        title="Evolução da Bilheteria — Empresas Selecionadas",
        markers=True
    )
    st.plotly_chart(fig)


elif consulta.startswith("3 -"):
    st.header("3 — País x Nota IMDb: Média de Nota por País")

    min_filmes = st.sidebar.number_input("Mínimo de filmes por país", min_value=1, value=40)

    query = f"""
    SELECT p.nome_pais,
           AVG(f.nota_imdb) AS media_imdb,
           COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes,
           SUM(f.bilheteria_mundial) AS total_bilheteria
    FROM bridge_filme_pais bp
    JOIN dim_pais p ON bp.id_pais_sk = p.id_pais_sk
    JOIN dim_filme fi ON bp.id_filme_sk = fi.id_filme_sk
    JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
    GROUP BY p.nome_pais
    HAVING COUNT(DISTINCT f.id_filme_sk) >= {int(min_filmes)}
    ORDER BY media_imdb DESC;
    """
    df = load_dataframe(query)
    st.dataframe(df)

    fig = px.bar(
    df,
    x="nome_pais",
    y="media_imdb",
    title="Média de Nota IMDb por País"
    )

    # --- AJUSTE DO EIXO Y ---
    min_val = df["media_imdb"].min()
    max_val = df["media_imdb"].max()

    fig.update_yaxes(
        range=[min_val - 0.07, max_val + 0.07],  # aumenta o espaço acima e abaixo
        tickformat=".3f"  # força duas casas decimais
    )

    st.plotly_chart(fig)

elif consulta.startswith("4 -"):
    st.header("4 — Top Estrelas: Atores que mais arrecadaram")

    top_n = st.sidebar.number_input("Top N", min_value=5, max_value=30, value=20)

    query = f"""
    SELECT es.nome_estrela,
           SUM(f.bilheteria_mundial) AS total_bilheteria,
           COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes
    FROM bridge_filme_estrela bst
    JOIN dim_estrela es ON bst.id_estrela_sk = es.id_estrela_sk
    JOIN dim_filme fi ON bst.id_filme_sk = fi.id_filme_sk
    JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
    GROUP BY es.nome_estrela
    ORDER BY total_bilheteria DESC
    LIMIT {int(top_n)};
    """

    df = load_dataframe(query)
    st.dataframe(df)

    fig = px.bar(
        df,
        x="nome_estrela",
        y="total_bilheteria",
        title="Atores que mais Arrecadaram"
    )
    st.plotly_chart(fig)

elif consulta.startswith("5 -"):
    st.header("5 — Produtividade de Estúdios")

    # Selecionar quantidade de empresas
    top_n = st.sidebar.number_input("Top N empresas", min_value=5, max_value=200, value=20)

    # 1) Buscar as Top N empresas por quantidade de filmes
    query_topN = f"""
        SELECT e.nome_empresa,
               COUNT(DISTINCT fi.id_filme_sk) AS qtd_filmes
        FROM bridge_filme_empresa be
        JOIN dim_empresa e ON be.id_empresa_sk = e.id_empresa_sk
        JOIN dim_filme fi ON be.id_filme_sk = fi.id_filme_sk
        JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
        GROUP BY e.nome_empresa
        ORDER BY qtd_filmes DESC
        LIMIT {int(top_n)};
    """
    df_top = load_dataframe(query_topN)

    if df_top.empty:
        st.info("Nenhuma empresa encontrada no DW.")
        st.stop()

    empresas_escolhidas = df_top["nome_empresa"].tolist()
    empresas_sql = "', '".join(empresas_escolhidas)

    # 2) Query principal apenas para as empresas selecionadas
    query = f"""
    SELECT e.nome_empresa,
           COUNT(DISTINCT fi.id_filme_sk) AS qtd_filmes,
           SUM(f.bilheteria_mundial) AS total_bilheteria,
           AVG(f.nota_imdb) AS media_nota
    FROM bridge_filme_empresa be
    JOIN dim_empresa e ON be.id_empresa_sk = e.id_empresa_sk
    JOIN dim_filme fi ON be.id_filme_sk = fi.id_filme_sk
    JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
    WHERE e.nome_empresa IN ('{empresas_sql}')
    GROUP BY e.nome_empresa
    ORDER BY qtd_filmes DESC;
    """
    df = load_dataframe(query)

    # Prevenir erro do Plotly: substituir NaN
    median_nota = df["media_nota"].median()
    if pd.isna(median_nota):
        median_nota = 0.1
    df["media_nota"] = df["media_nota"].fillna(median_nota)

    # 3) Normalizar as bolhas (evita crash do Plotly)
    min_v = df["media_nota"].min()
    max_v = df["media_nota"].max()

    if max_v == min_v:
        df["bubble_size"] = 12
    else:
        df["bubble_size"] = 5 + 35 * (df["media_nota"] - min_v) / (max_v - min_v)

    st.dataframe(df)

    fig = px.scatter(
        df,
        x="qtd_filmes",
        y="total_bilheteria",
        size="bubble_size",
        color="nome_empresa",
        hover_data={
            "media_nota": True,
            "qtd_filmes": True,
            "total_bilheteria": True,
            "bubble_size": False
        },
        title=f"Produtividade de Estúdios — Top {int(top_n)}"
    )

    st.plotly_chart(fig, use_container_width=True)


elif consulta.startswith("6 -"):
    st.header("6 — Quantidade de Filmes por Roteirista ao Longo dos Anos")

    top_n = st.sidebar.number_input(
        "Quantidade de roteiristas",
        min_value=1,
        max_value=50,
        value=10
    )

    query = """
        SELECT 
            r.nome_roteirista,
            t.ano,
            COUNT(DISTINCT fi.id_filme_sk) AS qtd_filmes
        FROM bridge_filme_roteirista br
        JOIN dim_roteirista r 
            ON br.id_roteirista_sk = r.id_roteirista_sk
        JOIN dim_filme fi 
            ON br.id_filme_sk = fi.id_filme_sk
        JOIN fato_filme f
            ON f.id_filme_sk = fi.id_filme_sk
        JOIN dim_tempo t
            ON t.id_tempo_sk = f.id_tempo_sk
        GROUP BY 
            r.nome_roteirista, t.ano
        ORDER BY 
            r.nome_roteirista, t.ano;
    """

    df = load_dataframe(query)

    # Seleciona os Top N roteiristas que mais aparecem
    top_authors = (
        df.groupby("nome_roteirista")["qtd_filmes"]
        .sum()
        .sort_values(ascending=False)
        .head(int(top_n))
        .index
    )

    df = df[df["nome_roteirista"].isin(top_authors)]

    st.dataframe(df)

    fig = px.line(
        df,
        x="ano",
        y="qtd_filmes",
        color="nome_roteirista",
        markers=True,
        title="Quantidade de Filmes por Roteirista ao Longo dos Anos"
    )

    st.plotly_chart(fig)

elif consulta.startswith("7 -"):
    st.header("7 — Idioma x Gênero")

    # --- Parâmetros no sidebar ---
    top_idiomas = st.sidebar.number_input(
        "Top Idiomas",
        min_value=5, max_value=30, value=5
    )

    top_generos = st.sidebar.number_input(
        "Top Gêneros",
        min_value=5, max_value=30, value=20
    )

    # --- Buscar Top Idiomas ---
    query_idiomas = f"""
        SELECT i.nome_idioma,
               COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes
        FROM fato_filme f
        JOIN dim_idioma i ON f.id_idioma_sk = i.id_idioma_sk
        GROUP BY i.nome_idioma
        ORDER BY qtd_filmes DESC
        LIMIT {int(top_idiomas)};
    """
    df_idiomas = load_dataframe(query_idiomas)
    top_idiomas_list = df_idiomas["nome_idioma"].tolist()

    # --- Buscar Top Gêneros ---
    query_generos = f"""
        SELECT g.nome_genero,
               COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes
        FROM fato_filme f
        JOIN dim_filme fi ON f.id_filme_sk = fi.id_filme_sk
        JOIN bridge_filme_genero bg ON bg.id_filme_sk = fi.id_filme_sk
        JOIN dim_genero g ON bg.id_genero_sk = g.id_genero_sk
        GROUP BY g.nome_genero
        ORDER BY qtd_filmes DESC
        LIMIT {int(top_generos)};
    """
    df_generos = load_dataframe(query_generos)
    top_generos_list = df_generos["nome_genero"].tolist()

    # --- Consulta principal filtrando Top Idiomas e Top Gêneros ---
    query = f"""
    SELECT 
        i.nome_idioma,
        g.nome_genero,
        COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes,
        SUM(f.bilheteria_mundial) AS total_bilheteria
    FROM fato_filme f
    JOIN dim_idioma i ON f.id_idioma_sk = i.id_idioma_sk
    JOIN dim_filme fi ON f.id_filme_sk = fi.id_filme_sk
    JOIN bridge_filme_genero bg ON bg.id_filme_sk = fi.id_filme_sk
    JOIN dim_genero g ON bg.id_genero_sk = g.id_genero_sk
    WHERE i.nome_idioma IN ({",".join([f"'{x}'" for x in top_idiomas_list])})
      AND g.nome_genero IN ({",".join([f"'{x}'" for x in top_generos_list])})
    GROUP BY i.nome_idioma, g.nome_genero
    ORDER BY g.nome_genero, qtd_filmes DESC;
    """
    
    df = load_dataframe(query)
    st.dataframe(df)

    # --- Gráfico ---
    fig = px.density_heatmap(
        df,
        x="nome_genero",
        y="nome_idioma",
        z="qtd_filmes",
        title="Quantidade de Filmes por Idioma e Gênero"
    )
    st.plotly_chart(fig)

elif consulta.startswith("8 -"):
    st.header("8 — Premiações por País e Ano (Indicações)")

    # --- Sidebar: Top países ---
    top_paises = st.sidebar.number_input(
        "Top Países",
        min_value=5, max_value=50, value=10
    )

    # --- Buscar Top Países com mais indicações ---
    query_top_paises = f"""
        SELECT p.nome_pais,
               SUM(f.nominacoes_premios) AS total_nominacoes
        FROM bridge_filme_pais bp
        JOIN dim_pais p ON bp.id_pais_sk = p.id_pais_sk
        JOIN dim_filme fi ON bp.id_filme_sk = fi.id_filme_sk
        JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
        GROUP BY p.nome_pais
        ORDER BY total_nominacoes DESC
        LIMIT {int(top_paises)};
    """
    df_top = load_dataframe(query_top_paises)
    lista_paises = df_top["nome_pais"].tolist()

    # --- Consulta principal filtrada pelos Top Países ---
    query = f"""
    SELECT 
        t.ano,
        p.nome_pais,
        SUM(f.nominacoes_premios) AS total_nominacoes,
        COUNT(DISTINCT f.id_filme_sk) AS qtd_filmes
    FROM bridge_filme_pais bp
    JOIN dim_pais p ON bp.id_pais_sk = p.id_pais_sk
    JOIN dim_filme fi ON bp.id_filme_sk = fi.id_filme_sk
    JOIN fato_filme f ON f.id_filme_sk = fi.id_filme_sk
    JOIN dim_tempo t ON t.id_tempo_sk = f.id_tempo_sk
    WHERE p.nome_pais IN ({",".join([f"'{x}'" for x in lista_paises])})
    GROUP BY t.ano, p.nome_pais
    ORDER BY t.ano, total_nominacoes DESC;
    """

    df = load_dataframe(query)
    st.dataframe(df)

    # --- Gráfico com bolinhas (markers=True) ---
    fig = px.line(
        df,
        x="ano",
        y="total_nominacoes",
        color="nome_pais",
        markers=True,
        title="Indicações a Prêmios por País e Ano"
    )
    st.plotly_chart(fig)


# footer / dica
st.markdown("---")
st.caption("Observação: as queries usam as bridge tables (quando aplicável). Ajuste LIMITs e filtros conforme necessidade.")
