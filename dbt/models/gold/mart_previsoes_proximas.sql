{{
  config(
    materialized = 'table',
    schema       = 'gold',
    alias        = 'mart_previsoes_proximas'
  )
}}

with previsoes as (
    select * from {{ source('gold_ml', 'previsoes_proximas_partidas') }}
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
        saldo_gols,
        aproveitamento_pct
    from {{ ref('feat_tabela_clubes') }}
    order by clube_id, rodada desc
),

elo as (
    select distinct on (clube_id)
        clube_id,
        elo_rating
    from {{ ref('feat_store_enhanced') }}
    order by clube_id, rodada desc
),

final as (
    select
        p.id,
        p.rodada,
        p.clube_casa_id,
        p.clube_vis_id,
        coalesce(p.nome_casa, cc.nome)                 as nome_casa,
        coalesce(p.nome_vis,  cv.nome)                 as nome_visitante,
        cc.abreviacao                                  as abrev_casa,
        cv.abreviacao                                  as abrev_visitante,
        cc.escudo_url                                  as escudo_casa,
        cv.escudo_url                                  as escudo_visitante,

        round(cast(p.prob_casa      * 100 as numeric), 1) as prob_casa_pct,
        round(cast(p.prob_empate    * 100 as numeric), 1) as prob_empate_pct,
        round(cast(p.prob_visitante * 100 as numeric), 1) as prob_visitante_pct,
        p.previsao,
        round(cast(p.confianca      * 100 as numeric), 1) as confianca_pct,
        p.modelo_versao,

        round(cast(e_casa.elo_rating as numeric), 0)   as elo_casa,
        round(cast(e_vis.elo_rating  as numeric), 0)   as elo_visitante,

        t_casa.pontos_acumulados                       as pontos_casa,
        t_vis.pontos_acumulados                        as pontos_visitante,
        t_casa.aproveitamento_pct                      as aprov_casa_pct,
        t_vis.aproveitamento_pct                       as aprov_vis_pct,

        p.processed_at

    from previsoes p
    left join clubes   cc     on cc.clube_id    = p.clube_casa_id
    left join clubes   cv     on cv.clube_id    = p.clube_vis_id
    left join tabela   t_casa on t_casa.clube_id = p.clube_casa_id
    left join tabela   t_vis  on t_vis.clube_id  = p.clube_vis_id
    left join elo      e_casa on e_casa.clube_id = p.clube_casa_id
    left join elo      e_vis  on e_vis.clube_id  = p.clube_vis_id
)

select * from final
order by rodada, prob_casa_pct desc
