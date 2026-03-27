{{
  config(
    materialized = 'table',
    schema       = 'gold',
    alias        = 'mart_desempenho_modelo'
  )
}}

with validadas as (
    select * from {{ source('gold_ml', 'previsoes_validadas') }}
),

por_rodada as (
    select
        rodada,
        count(*)                                               as total_jogos,
        sum(case when acerto then 1 else 0 end)                as acertos,
        round(
            cast(sum(case when acerto then 1 else 0 end) as numeric)
            / nullif(count(*), 0) * 100
        , 1)                                                   as acuracia_pct,

        round(avg(case when resultado_real = 'casa'      and acerto then 1.0
                       when resultado_real = 'casa'               then 0.0 end) * 100, 1)
                                                               as acuracia_casa_pct,
        round(avg(case when resultado_real = 'empate'    and acerto then 1.0
                       when resultado_real = 'empate'             then 0.0 end) * 100, 1)
                                                               as acuracia_empate_pct,
        round(avg(case when resultado_real = 'visitante' and acerto then 1.0
                       when resultado_real = 'visitante'          then 0.0 end) * 100, 1)
                                                               as acuracia_visitante_pct,

        round(cast(avg(case when acerto then prob_casa end) * 100 as numeric), 1)     as conf_media_acerto,
        round(cast(avg(case when not acerto then prob_casa end) * 100 as numeric), 1) as conf_media_erro

    from validadas
    group by rodada
),

acumulado as (
    select
        rodada,
        total_jogos,
        acertos,
        acuracia_pct,
        acuracia_casa_pct,
        acuracia_empate_pct,
        acuracia_visitante_pct,
        conf_media_acerto,
        conf_media_erro,
        round(avg(acuracia_pct) over (
            order by rodada rows between 4 preceding and current row
        ), 1)                        as acuracia_media_5r,
        sum(total_jogos) over (
            order by rodada rows between unbounded preceding and current row
        )                            as total_jogos_acumulado,
        sum(acertos) over (
            order by rodada rows between unbounded preceding and current row
        )                            as acertos_acumulado
    from por_rodada
)

select
    *,
    round(
        cast(acertos_acumulado as numeric)
        / nullif(total_jogos_acumulado, 0) * 100
    , 1) as acuracia_geral_pct
from acumulado
order by rodada
