{{
  config(
    materialized = 'table',
    schema       = 'diamond',
    alias        = 'mart_tabela_classificacao'
  )
}}

/*
  Mart: Tabela de classificacao completa com risco de rebaixamento
  e posicionamento nos grupos (libertadores, sul-americana, rebaixamento).
*/

with rebaixamento as (
    select * from {{ source('diamond_raw', 'analise_rebaixamento') }}
),

clubes as (
    select * from {{ ref('stg_clubes') }}
),

tabela as (
    select distinct on (clube_id)
        clube_id,
        pontos_acumulados,
        vitorias,
        empates,
        derrotas,
        gols_marcados,
        gols_sofridos,
        saldo_gols,
        aproveitamento_pct,
        vitorias + empates + derrotas as jogos
    from {{ ref('feat_tabela_clubes') }}
    order by clube_id, rodada desc
),

classificacao as (
    select
        row_number() over (
            order by t.pontos_acumulados desc,
                     t.vitorias         desc,
                     t.saldo_gols       desc,
                     t.gols_marcados    desc
        )                                      as posicao,
        c.nome                                 as clube,
        c.abreviacao,
        c.escudo_url,
        t.jogos,
        t.pontos_acumulados                    as pontos,
        t.vitorias                             as v,
        t.empates                              as e,
        t.derrotas                             as d,
        t.gols_marcados                        as gm,
        t.gols_sofridos                        as gs,
        t.saldo_gols                           as sg,
        t.aproveitamento_pct                   as aproveitamento,
        r.prob_rebaixamento,
        r.zona_rebaixamento

    from tabela t
    left join clubes       c on c.clube_id = t.clube_id
    left join rebaixamento r on r.clube_id = t.clube_id
),

final as (
    select
        posicao,
        clube,
        abreviacao,
        escudo_url,
        jogos,
        pontos,
        v, e, d,
        gm, gs, sg,
        aproveitamento,
        round(cast(prob_rebaixamento * 100 as numeric), 1) as prob_rebaixamento_pct,
        zona_rebaixamento,

        -- Classificacao para competicoes continentais
        case
            when posicao between 1 and 4  then 'Libertadores (fase de grupos)'
            when posicao between 5 and 6  then 'Libertadores (pre-fase)'
            when posicao between 7 and 12 then 'Sul-Americana'
            when posicao between 17 and 20 then 'Rebaixamento'
            else 'Meio de tabela'
        end as situacao,

        case
            when posicao between 1  and 4  then '#1a7a1a'   -- verde escuro
            when posicao between 5  and 6  then '#2e8b57'   -- verde medio
            when posicao between 7  and 12 then '#4682b4'   -- azul
            when posicao between 17 and 20 then '#cc2222'   -- vermelho
            else '#666666'
        end as cor_situacao

    from classificacao
)

select * from final
