{{
  config(
    materialized = 'view',
    schema       = 'gold',
    alias        = 'vw_elo_ranking'
  )
}}

with ultima_rodada as (
    select max(rodada) as rodada from {{ ref('feat_tabela_clubes') }}
)

select
    row_number() over (
        order by fs.pontos_acumulados desc,
                 fs.aproveitamento_pct desc
    )                                   as ranking,
    fs.clube_id,
    c.nome,
    c.abreviacao,
    fs.pontos_acumulados                as pontos,
    fs.aproveitamento_pct               as aproveitamento,
    fs.vitorias,
    fs.empates,
    fs.derrotas
from {{ ref('feat_tabela_clubes') }} fs
join {{ ref('stg_clubes') }} c on fs.clube_id = c.clube_id
join ultima_rodada ur                       on fs.rodada   = ur.rodada
order by pontos desc
