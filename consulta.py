from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import pandas as pd
import datetime as _dt


user = "root"
password = "mdrzz.0301"
password_enc = quote_plus(password)
host = "localhost"
port = 3306
database = "filmes_db"

ano_inicio = 2000
ano_fim = _dt.datetime.now().year
filtro_empresa = None
filtro_pais = None      
top_n = 20


def build_query(filtros: dict):
        """Monta a consulta pró‑rata entre Empresas × Países com filtros opcionais.

        Filtros suportados: ano_inicio, ano_fim, empresa, pais, limit_n.
        Retorna (sql, params)
        """
        where_base = []
        params = {}

        if filtros.get("ano_inicio") is not None:
                where_base.append("b.ano_lancamento >= :ano_inicio")
                params["ano_inicio"] = int(filtros["ano_inicio"])
        if filtros.get("ano_fim") is not None:
                where_base.append("b.ano_lancamento <= :ano_fim")
                params["ano_fim"] = int(filtros["ano_fim"])

        where_base_sql = ("WHERE " + " AND ".join(where_base)) if where_base else ""

        # Filtros por nome exigem já ter juntado Empresas e Paises; aplicaremos na seleção final
        having_filters = []
        if filtros.get("empresa"):
                having_filters.append("e.nome_empresa = :empresa")
                params["empresa"] = filtros["empresa"]
        if filtros.get("pais"):
                having_filters.append("p.nome_pais = :pais")
                params["pais"] = filtros["pais"]

        and_clause = ("WHERE " + " AND ".join(having_filters)) if having_filters else ""

        limit_clause = ""
        if filtros.get("limit_n") is not None:
                try:
                        limit_n = int(filtros["limit_n"])
                        if limit_n > 0:
                                limit_clause = f"\nLIMIT {limit_n}"
                except Exception:
                        pass

        sql = f"""
WITH
base AS (
    SELECT
        f.id_filme,
        COALESCE(f.vitorias_premios, 0)   AS wins,
        COALESCE(f.nominacoes_premios, 0) AS nominations,
        COALESCE(f.vitorias_oscar, 0)     AS oscars,
        f.ano_lancamento
    FROM Filmes f
),
emp_count AS (
    SELECT id_filme, COUNT(DISTINCT id_empresa) AS n_empresas
    FROM Filme_Empresa_Producao
    GROUP BY id_filme
),
pais_count AS (
    SELECT id_filme, COUNT(DISTINCT id_pais) AS n_paises
    FROM Filme_Pais_Origem
    GROUP BY id_filme
),
film_pairs AS (
    SELECT fe.id_filme, fe.id_empresa, fp.id_pais
    FROM Filme_Empresa_Producao fe
    JOIN Filme_Pais_Origem fp
        ON fp.id_filme = fe.id_filme
),
shares AS (
    SELECT
        fp.id_filme,
        fp.id_empresa,
        fp.id_pais,
        b.wins,
        b.nominations,
        b.oscars,
        (ec.n_empresas * pc.n_paises) AS denom,
        b.ano_lancamento
    FROM film_pairs fp
    JOIN base b       ON b.id_filme = fp.id_filme
    JOIN emp_count ec ON ec.id_filme = fp.id_filme
    JOIN pais_count pc ON pc.id_filme = fp.id_filme
    {where_base_sql}
)
SELECT
    e.nome_empresa,
    p.nome_pais,
    COUNT(DISTINCT s.id_filme) AS filmes_compartilhados,
    ROUND(SUM(CASE WHEN s.denom > 0 THEN s.wins        / s.denom ELSE 0 END), 2) AS total_vitorias_prorata,
    ROUND(SUM(CASE WHEN s.denom > 0 THEN s.nominations / s.denom ELSE 0 END), 2) AS total_nominacoes_prorata,
    ROUND(SUM(CASE WHEN s.denom > 0 THEN s.oscars      / s.denom ELSE 0 END), 2) AS total_oscars_prorata
FROM shares s
JOIN Empresas e ON e.id_empresa = s.id_empresa
JOIN Paises   p ON p.id_pais    = s.id_pais
{and_clause}
GROUP BY e.id_empresa, p.id_pais
ORDER BY total_oscars_prorata DESC, total_vitorias_prorata DESC, filmes_compartilhados DESC
{limit_clause};
"""

        return sql, params


def main():
        # Cria engine do SQLAlchemy
        engine = create_engine(f"mysql+pymysql://{user}:{password_enc}@{host}:{port}/{database}")
        print(f"Conectando a {engine}")

        filtros = {
                "ano_inicio": ano_inicio,
                "ano_fim": ano_fim,
                "empresa": filtro_empresa,
                "pais": filtro_pais,
                "limit_n": top_n,
        }

        sql, params = build_query(filtros)

        print("Executando consulta (pode levar alguns segundos)...")
        try:
                df = pd.read_sql(text(sql), con=engine, params=params)
        except Exception as e:
                print("Erro ao executar consulta:\n", e)
                print("Verifique se as tabelas e colunas existem com os nomes esperados e se o database está correto.")
                return

        if df.empty:
                print("Nenhum resultado retornado para os filtros aplicados.")
                return

        # Exibe amostra
        print("\nTop resultados:")
        print(df.head(min(10, len(df))))

        # Salva CSV
        out_file = "resultado_empresas_paises_premiacoes.csv"
        df.to_csv(out_file, index=False, encoding="utf-8")
        print(f"\nArquivo salvo: {out_file} (linhas: {len(df)})")


if __name__ == "__main__":
        main()