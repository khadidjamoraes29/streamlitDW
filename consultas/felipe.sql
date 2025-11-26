USE filmes_db;

WITH
base AS (
  SELECT
    f.id_filme,
    COALESCE(f.vitorias_premios, 0)     AS wins,
    COALESCE(f.nominacoes_premios, 0)   AS nominations,
    COALESCE(f.vitorias_oscar, 0)       AS oscars,
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
  JOIN base b      ON b.id_filme = fp.id_filme
  JOIN emp_count ec ON ec.id_filme = fp.id_filme
  JOIN pais_count pc ON pc.id_filme = fp.id_filme
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
GROUP BY e.id_empresa, p.id_pais
ORDER BY total_oscars_prorata DESC, total_vitorias_prorata DESC, filmes_compartilhados DESC
LIMIT 20;