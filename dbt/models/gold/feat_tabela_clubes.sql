{{
  config(
    materialized = 'table',
    schema       = 'gold',
    alias        = 'feature_store',
    indexes      = [{'columns': ['rodada', 'clube_id'], 'type': 'btree'}]
  )
}}

/*
  Feature Store basico: pontuacao acumulada, V/E/D, gols por rodada.
  Cada linha representa o estado do clube APOS a rodada especificada.
*/

with partidas as (
    select * from {{ ref('stg_partidas') }}
    where resultado is not null
),

-- Expandir: cada partida gera uma linha para casa e outra para visitante
partidas_long as (

    select
        rodada,
        clube_casa_id                                  as clube_id,
        case when resultado = 'casa'      then 3
             when resultado = 'empate'    then 1
             else                              0 end   as pontos,
        case when resultado = 'casa'      then 1 else 0 end as vitoria,
        case when resultado = 'empate'    then 1 else 0 end as empate,
        case when resultado = 'visitante' then 1 else 0 end as derrota,
        coalesce(placar_casa, 0)                       as gols_marcados,
        coalesce(placar_vis,  0)                       as gols_sofridos,
        true                                           as eh_casa

    from partidas

    union all

    select
        rodada,
        clube_vis_id                                   as clube_id,
        case when resultado = 'visitante' then 3
             when resultado = 'empate'    then 1
             else                              0 end   as pontos,
        case when resultado = 'visitante' then 1 else 0 end as vitoria,
        case when resultado = 'empate'    then 1 else 0 end as empate,
        case when resultado = 'casa'      then 1 else 0 end as derrota,
        coalesce(placar_vis,  0)                       as gols_marcados,
        coalesce(placar_casa, 0)                       as gols_sofridos,
        false                                          as eh_casa

    from partidas
),

acumulado as (
    select
        rodada,
        clube_id,
        sum(pontos)        over w as pontos_acumulados,
        sum(vitoria)       over w as vitorias,
        sum(empate)        over w as empates,
        sum(derrota)       over w as derrotas,
        sum(gols_marcados) over w as gols_marcados,
        sum(gols_sofridos) over w as gols_sofridos,
        sum(gols_marcados) over w
            - sum(gols_sofridos) over w as saldo_gols,
        row_number()       over (partition by clube_id order by rodada) as jogos
    from partidas_long
    window w as (
        partition by clube_id
        order     by rodada
        rows between unbounded preceding and current row
    )
),

final as (
    select
        rodada,
        clube_id,
        pontos_acumulados,
        vitorias,
        empates,
        derrotas,
        gols_marcados,
        gols_sofridos,
        saldo_gols,
        round(
            cast(pontos_acumulados as numeric) /
            nullif(jogos * 3, 0) * 100
        , 2) as aproveitamento_pct,
        now() as updated_at
    from acumulado
)

select * from final
