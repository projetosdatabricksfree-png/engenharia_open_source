{{
  config(
    materialized = 'table',
    schema       = 'silver',
    alias        = 'partidas',
    indexes      = [{'columns': ['rodada'], 'type': 'btree'}]
  )
}}

with source as (
    select * from {{ source('bronze', 'partidas_raw') }}
),

renamed as (
    select
        rodada,
        clube_casa_id,
        clube_vis_id,
        placar_casa,
        placar_vis,

        case
            when placar_casa is null or placar_vis is null then null
            when placar_casa  > placar_vis              then 'casa'
            when placar_casa  < placar_vis              then 'visitante'
            else                                             'empate'
        end as resultado,

        cast(data_partida as timestamp) as data_partida,
        now()                           as processed_at

    from source
    where rodada is not null
      and clube_casa_id is not null
      and clube_vis_id  is not null
),

dedup as (
    select distinct on (rodada, clube_casa_id, clube_vis_id)
        *
    from renamed
    order by rodada, clube_casa_id, clube_vis_id, processed_at desc
)

select * from dedup
