USE filmes_db;

-- =====================================================================
-- ANÁLISE MULTIDIMENSIONAL DE ATORES/ESTRELAS DE CINEMA
-- =====================================================================
-- Esta query complexa analisa os atores principais (tabela Filme_Estrela)
-- sob múltiplas perspectivas:
--   1. Desempenho comercial (bilheteria média, ROI)
--   2. Desempenho crítico (nota IMDB, premiações pró-rata)
--   3. Colaborações frequentes (top diretores e coatores)
--   4. Diversidade de gêneros cinematográficos
--   5. Evolução temporal da carreira (ano início/fim, duração)
--   6. Consistência (desvio padrão de notas)
-- =====================================================================

WITH
-- CTE 1: Métricas base por ator-filme
ator_filme_base AS (
  SELECT
    fe.id_pessoa,
    fe.id_filme,
    fe.ordem_credito,
    f.titulo,
    f.ano_lancamento,
    f.nota_imdb,
    f.votos_imdb,
    f.bilheteria_mundial,
    f.orcamento,
    COALESCE(f.vitorias_premios, 0) AS wins,
    COALESCE(f.nominacoes_premios, 0) AS nominations,
    COALESCE(f.vitorias_oscar, 0) AS oscars,
    -- Calcula número de atores por filme para pró-rata
    (SELECT COUNT(DISTINCT id_pessoa) FROM Filme_Estrela WHERE id_filme = fe.id_filme) AS n_atores_filme
  FROM Filme_Estrela fe
  JOIN Filmes f ON f.id_filme = fe.id_filme
),

-- CTE 2: Métricas agregadas por ator
metricas_ator AS (
  SELECT
    id_pessoa,
    COUNT(DISTINCT id_filme) AS total_filmes,
    ROUND(AVG(ordem_credito), 2) AS media_ordem_credito,
    
    -- Desempenho comercial
    ROUND(AVG(bilheteria_mundial), 2) AS media_bilheteria,
    ROUND(SUM(bilheteria_mundial), 2) AS bilheteria_total,
    ROUND(AVG(CASE 
      WHEN orcamento > 0 THEN (bilheteria_mundial - orcamento) / orcamento * 100 
      ELSE NULL 
    END), 2) AS roi_medio_percentual,
    
    -- Desempenho crítico
    ROUND(AVG(nota_imdb), 2) AS media_nota_imdb,
    ROUND(STDDEV(nota_imdb), 2) AS desvio_nota_imdb, -- Consistência
    ROUND(AVG(votos_imdb), 0) AS media_votos_imdb,
    
    -- Premiações (pró-rata entre atores do mesmo filme)
    ROUND(SUM(wins / n_atores_filme), 2) AS total_vitorias_prorata,
    ROUND(SUM(nominations / n_atores_filme), 2) AS total_nominacoes_prorata,
    ROUND(SUM(oscars / n_atores_filme), 2) AS total_oscars_prorata,
    
    -- Evolução temporal
    MIN(ano_lancamento) AS ano_primeiro_filme,
    MAX(ano_lancamento) AS ano_ultimo_filme,
    (MAX(ano_lancamento) - MIN(ano_lancamento)) AS anos_carreira
  FROM ator_filme_base
  GROUP BY id_pessoa
),

-- CTE 3: Gêneros por ator (diversidade)
generos_ator AS (
  SELECT
    fe.id_pessoa,
    COUNT(DISTINCT fg.id_genero) AS total_generos_distintos,
    GROUP_CONCAT(DISTINCT g.nome_genero ORDER BY g.nome_genero SEPARATOR ', ') AS generos_trabalhados
  FROM Filme_Estrela fe
  JOIN Filme_Genero fg ON fg.id_filme = fe.id_filme
  JOIN Generos g ON g.id_genero = fg.id_genero
  GROUP BY fe.id_pessoa
),

-- CTE 4: Países de origem dos filmes por ator
paises_ator AS (
  SELECT
    fe.id_pessoa,
    COUNT(DISTINCT fp.id_pais) AS total_paises_distintos,
    GROUP_CONCAT(DISTINCT p.nome_pais ORDER BY p.nome_pais SEPARATOR ', ') AS paises_trabalhados
  FROM Filme_Estrela fe
  JOIN Filme_Pais_Origem fp ON fp.id_filme = fe.id_filme
  JOIN Paises p ON p.id_pais = fp.id_pais
  GROUP BY fe.id_pessoa
),

-- CTE 5: Colaborações com diretores (top 3 mais frequentes)
diretores_frequentes AS (
  SELECT
    colabs.id_ator,
    GROUP_CONCAT(
      CONCAT(p.nome_pessoa, ' (', colabs.num_colabs, ')')
      ORDER BY colabs.num_colabs DESC, p.nome_pessoa
      SEPARATOR '; '
    ) AS top_diretores
  FROM (
    SELECT 
      fe.id_pessoa AS id_ator,
      fd.id_pessoa AS id_diretor,
      COUNT(DISTINCT fe.id_filme) AS num_colabs,
      ROW_NUMBER() OVER (PARTITION BY fe.id_pessoa ORDER BY COUNT(DISTINCT fe.id_filme) DESC) AS ranking
    FROM Filme_Estrela fe
    JOIN Filme_Diretor fd ON fd.id_filme = fe.id_filme
    GROUP BY fe.id_pessoa, fd.id_pessoa
  ) colabs
  JOIN Pessoas p ON p.id_pessoa = colabs.id_diretor
  WHERE colabs.ranking <= 3  -- Top 3 diretores
  GROUP BY colabs.id_ator
),

-- CTE 6: Colaborações com outros atores (top 3 coatores frequentes)
coatores_frequentes AS (
  SELECT
    colabs.id_ator,
    GROUP_CONCAT(
      CONCAT(p.nome_pessoa, ' (', colabs.num_colabs, ')')
      ORDER BY colabs.num_colabs DESC, p.nome_pessoa
      SEPARATOR '; '
    ) AS top_coatores
  FROM (
    SELECT 
      fe1.id_pessoa AS id_ator,
      fe2.id_pessoa AS id_coator,
      COUNT(DISTINCT fe1.id_filme) AS num_colabs,
      ROW_NUMBER() OVER (PARTITION BY fe1.id_pessoa ORDER BY COUNT(DISTINCT fe1.id_filme) DESC) AS ranking
    FROM Filme_Estrela fe1
    JOIN Filme_Estrela fe2 ON fe2.id_filme = fe1.id_filme AND fe2.id_pessoa != fe1.id_pessoa
    GROUP BY fe1.id_pessoa, fe2.id_pessoa
  ) colabs
  JOIN Pessoas p ON p.id_pessoa = colabs.id_coator
  WHERE colabs.ranking <= 3  -- Top 3 coatores
  GROUP BY colabs.id_ator
)

-- Query final: junta todas as métricas
SELECT
  p.nome_pessoa AS ator,
  m.total_filmes,
  m.media_ordem_credito,
  
  -- Desempenho comercial
  m.bilheteria_total,
  m.media_bilheteria,
  m.roi_medio_percentual,
  
  -- Desempenho crítico
  m.media_nota_imdb,
  m.desvio_nota_imdb AS consistencia_nota,
  m.media_votos_imdb,
  
  -- Premiações
  m.total_vitorias_prorata,
  m.total_nominacoes_prorata,
  m.total_oscars_prorata,
  
  -- Evolução temporal
  m.ano_primeiro_filme,
  m.ano_ultimo_filme,
  m.anos_carreira,
  
  -- Diversidade
  g.total_generos_distintos,
  pa.total_paises_distintos,
  
  -- Colaborações
  df.top_diretores,
  cf.top_coatores,
  
  -- Lista de gêneros (opcional, comentar se muito longo)
  g.generos_trabalhados

FROM metricas_ator m
JOIN Pessoas p ON p.id_pessoa = m.id_pessoa
LEFT JOIN generos_ator g ON g.id_pessoa = m.id_pessoa
LEFT JOIN paises_ator pa ON pa.id_pessoa = m.id_pessoa
LEFT JOIN diretores_frequentes df ON df.id_ator = m.id_pessoa
LEFT JOIN coatores_frequentes cf ON cf.id_ator = m.id_pessoa

-- Filtros configuráveis
WHERE m.total_filmes >= 3  -- Mínimo de filmes para análise significativa
  -- AND m.ano_primeiro_filme >= 2000  -- Descomentar para filtrar período
  -- AND m.media_nota_imdb >= 7.0      -- Descomentar para filtrar qualidade

-- Ordenação: prioriza atores com maior impacto (Oscars + bilheteria + filmes)
ORDER BY 
  m.total_oscars_prorata DESC,
  m.bilheteria_total DESC,
  m.total_filmes DESC

LIMIT 50;  -- Top 50 atores