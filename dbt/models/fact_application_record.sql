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

with application_record as (
    select
        ID,
        CODE_GENDER,
        FLAG_OWN_CAR,
        FLAG_OWN_REALTY,	
        CNT_CHILDREN,
        AMT_INCOME_TOTAL,
        {{ create_fact('NAME_INCOME_TYPE') }} ID_INCOME_TYPE,
        {{ create_fact('NAME_EDUCATION_TYPE') }} as ID_EDUCATION_TYPE,
        {{ create_fact('NAME_FAMILY_STATUS') }} as ID_FAMILY_STATUS,
        {{ create_fact('NAME_HOUSING_TYPE') }} as ID_HOUSING_TYPE,
        DAYS_BIRTH,
        DAYS_EMPLOYED,
        FLAG_MOBIL,
        FLAG_WORK_PHONE,
        FLAG_PHONE,	
        FLAG_EMAIL,
        {{ create_fact('OCCUPATION_TYPE') }} as ID_OCCUPATION_TYPE,
        CNT_FAM_MEMBERS
    from `lyrical-country-370008.data_fellowship.batch_application_record`
)

select * from application_record