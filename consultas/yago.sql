SELECT
    CASE
        WHEN duracao_minutos < 90 THEN 'Curto (<90 min)'
        WHEN duracao_minutos BETWEEN 90 AND 120 THEN 'Médio (90-120 min)'
        ELSE 'Longo (>120 min)'
    END AS faixa_duracao,
    COUNT(*) AS total_filmes,
    AVG(nota_imdb) AS media_nota_imdb,
    AVG(bilheteria_mundial) AS media_bilheteria_mundial,
    AVG(vitorias_oscar) AS media_vitorias_oscar,
    AVG(retorno_orcamento) AS media_retorno_orcamento
FROM (
    SELECT *,
        CASE 
            WHEN orcamento > 0 THEN bilheteria_mundial / orcamento
            ELSE NULL
        END AS retorno_orcamento
    FROM Filmes
) AS filmes_calc
GROUP BY faixa_duracao
ORDER BY faixa_duracao;
