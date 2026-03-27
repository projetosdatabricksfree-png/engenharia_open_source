{{
  config(
    materialized = 'table',
    schema       = 'silver',
    alias        = 'clubes'
  )
}}

with source as (
    select * from {{ source('bronze', 'clubes_info_raw') }}
),

renamed as (
    select distinct on (clube_id)
        clube_id,
        trim(nome)       as nome,
        trim(abreviacao) as abreviacao,
        escudo_url,
        now()            as updated_at
    from source
    where clube_id is not null
      and nome     is not null
    order by clube_id, ingested_at desc
)

select * from renamed
