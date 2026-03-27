{{
  config(
    materialized = 'table',
    schema       = 'silver',
    alias        = 'estatisticas_jogador_partida'
  )
}}

with pontuacoes as (
    select * from {{ source('bronze', 'pontuacoes_historico_raw') }}
),

status as (
    select * from {{ source('bronze', 'jogadores_status_raw') }}
),

joined as (
    select
        p.rodada,
        p.atleta_id,
        p.clube_id,
        s.posicao_id,
        s.status_id,
        p.pontos,
        now() as processed_at
    from pontuacoes p
    left join status s using (atleta_id)
    where p.pontos is not null
      and p.rodada  is not null
)

select * from joined
