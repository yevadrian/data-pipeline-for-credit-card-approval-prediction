{{ config(materialized='view') }}

with application_record as (
    select
        fact.ID as CLIENT_ID,
        cast({{ convert_age('DAYS_BIRTH') }} as int) as YEARS_AGE,
        cast({{ convert_employed('DAYS_EMPLOYED') }} as int) as YEARS_EMPLOYED,
        CNT_CHILDREN as CHILDREN_COUNTS,
        cast(round(CNT_FAM_MEMBERS, 0) as int) as FAMILY_MEMBERS,
        cast(round(AMT_INCOME_TOTAL, 0) as int) as ANNUAL_INCOME,
        NAME_INCOME_TYPE as INCOME_TYPE,
        NAME_EDUCATION_TYPE as EDUCATION_TYPE,
        NAME_FAMILY_STATUS as FAMILY_STATUS,
        NAME_HOUSING_TYPE as HOUSING_TYPE,
        OCCUPATION_TYPE,
        {{ convert_gender('CODE_GENDER') }} as GENDER_TYPE,
        {{ convert_boolean('FLAG_OWN_CAR') }} as FLAG_OWN_CAR,
        {{ convert_boolean('FLAG_OWN_REALTY') }} as FLAG_OWN_REALTY,
        {{ convert_binary('FLAG_MOBIL') }} as FLAG_MOBILE_PHONE,
        {{ convert_binary('FLAG_WORK_PHONE') }} as FLAG_WORK_PHONE,
        {{ convert_binary('FLAG_PHONE') }} as FLAG_HOME_PHONE,
        {{ convert_binary('FLAG_EMAIL') }} as FLAG_EMAIL_ADDRESS
    from `lyrical-country-370008.data_fellowship.fact_application_record` as fact
    left join {{ ref('dim_income_type') }} as income
    on fact.ID_INCOME_TYPE = income.ID
    left join {{ ref('dim_education_type') }} as education
    on fact.ID_EDUCATION_TYPE = education.ID
    left join {{ ref('dim_family_status') }} as family
    on fact.ID_FAMILY_STATUS = family.ID
    left join {{ ref('dim_housing_type') }} as housing
    on fact.ID_HOUSING_TYPE = housing.ID
    left join {{ ref('dim_occupation_type') }} as occupation
    on fact.ID_OCCUPATION_TYPE = occupation.ID
)

select * from application_record