{{
  config(
    materialized = 'view',
    schema       = 'gold',
    alias        = 'vw_classificacao'
  )
}}

with ultima_rodada as (
    select max(rodada) as rodada from {{ ref('feat_tabela_clubes') }}
),

classificacao as (
    select
        row_number() over (
            order by fs.pontos_acumulados desc,
                     fs.vitorias         desc,
                     fs.saldo_gols       desc,
                     fs.gols_marcados    desc
        )                                    as posicao,
        fs.clube_id,
        c.nome,
        c.abreviacao,
        (fs.vitorias + fs.empates + fs.derrotas) as jogos,
        fs.pontos_acumulados                 as pontos,
        fs.vitorias,
        fs.empates,
        fs.derrotas,
        fs.gols_marcados                     as gols_pro,
        fs.gols_sofridos                     as gols_contra,
        fs.saldo_gols,
        fs.aproveitamento_pct                as aproveitamento
    from {{ ref('feat_tabela_clubes') }} fs
    join {{ ref('stg_clubes') }} c on fs.clube_id = c.clube_id
    join ultima_rodada ur                       on fs.rodada   = ur.rodada
)

select * from classificacao
