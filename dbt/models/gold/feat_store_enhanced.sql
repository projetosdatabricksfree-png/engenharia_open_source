{{
  config(
    materialized = 'table',
    schema       = 'gold',
    alias        = 'feature_store_enhanced',
    indexes      = [{'columns': ['rodada', 'clube_id'], 'type': 'btree'}]
  )
}}

/*
  Feature Store avancado:
  - ELO rating (sistema de ranking adaptativo)
  - Medias moveis das ultimas 5 rodadas
  - Aproveitamento como mandante e visitante separados
  - Momentum (diferenca entre media das ultimas 3 vs ultimas 6 rodadas)
*/

with partidas as (
    select * from {{ ref('stg_partidas') }}
    where resultado is not null
),

partidas_long as (
    select
        rodada,
        clube_casa_id                                  as clube_id,
        case when resultado = 'casa'      then 3
             when resultado = 'empate'    then 1
             else 0 end                                as pontos,
        coalesce(placar_casa, 0)                       as gols_marcados,
        coalesce(placar_vis,  0)                       as gols_sofridos,
        true                                           as eh_casa,
        resultado = 'casa'                             as ganhou

    from partidas

    union all

    select
        rodada,
        clube_vis_id                                   as clube_id,
        case when resultado = 'visitante' then 3
             when resultado = 'empate'    then 1
             else 0 end                                as pontos,
        coalesce(placar_vis,  0)                       as gols_marcados,
        coalesce(placar_casa, 0)                       as gols_sofridos,
        false                                          as eh_casa,
        resultado = 'visitante'                        as ganhou

    from partidas
),

-- Medias moveis (5 jogos)
moving_avgs as (
    select
        rodada,
        clube_id,
        avg(pontos)        over w5 as media_pontos_5j,
        avg(gols_marcados) over w5 as media_gols_marc_5j,
        avg(gols_sofridos) over w5 as media_gols_sofr_5j,
        avg(pontos)        over w3 as media_pontos_3j,
        avg(pontos)        over w6 as media_pontos_6j
    from partidas_long
    window
        w5 as (partition by clube_id order by rodada rows between 4 preceding and current row),
        w3 as (partition by clube_id order by rodada rows between 2 preceding and current row),
        w6 as (partition by clube_id order by rodada rows between 5 preceding and current row)
),

-- Aproveitamento mandante
aprov_casa as (
    select
        clube_id,
        rodada,
        avg(case when ganhou then 1.0 else 0.0 end)
            over (partition by clube_id order by rodada
                  rows between unbounded preceding and current row
            ) as aproveitamento_casa
    from partidas_long
    where eh_casa = true
),

-- Aproveitamento visitante
aprov_fora as (
    select
        clube_id,
        rodada,
        avg(case when ganhou then 1.0 else 0.0 end)
            over (partition by clube_id order by rodada
                  rows between unbounded preceding and current row
            ) as aproveitamento_fora
    from partidas_long
    where eh_casa = false
),

combined as (
    select
        m.rodada,
        m.clube_id,
        -- ELO simplificado: 1500 base + bonus por aproveitamento
        1500.0 + (
            coalesce(ac.aproveitamento_casa, 0.33) * 200 +
            coalesce(af.aproveitamento_fora, 0.33) * 100 +
            coalesce(m.media_pontos_5j,      1.0)  * 50
        )                                                  as elo_rating,
        coalesce(m.media_pontos_5j,     0)                 as media_pontos_5j,
        coalesce(m.media_gols_marc_5j,  0)                 as media_gols_marc_5j,
        coalesce(m.media_gols_sofr_5j,  0)                 as media_gols_sofr_5j,
        coalesce(ac.aproveitamento_casa, 0)                as aproveitamento_casa,
        coalesce(af.aproveitamento_fora, 0)                as aproveitamento_fora,
        -- Momentum: diferenca media 3j vs 6j (positivo = em alta)
        coalesce(m.media_pontos_3j, 0) -
        coalesce(m.media_pontos_6j, 0)                     as momentum,
        now()                                              as updated_at
    from moving_avgs m
    left join aprov_casa ac using (clube_id, rodada)
    left join aprov_fora af using (clube_id, rodada)
)

select * from combined
