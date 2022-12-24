{{ config(
    materialized='table',
    partition_by={
      "field": "ID",
      "data_type": "int64",
      "range": {
        "start": 0,
        "end": 10000000,
        "interval": 100000
      }
    }
)}}

with credit_record as (
    select
        ID,
        MONTHS_BALANCE,
        STATUS as ID_STATUS
    from `lyrical-country-370008.data_fellowship.batch_credit_record`
)

select * from credit_record