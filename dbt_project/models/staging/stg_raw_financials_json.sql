SELECT
    v:"name"::STRING AS company_name,
    v:"year"::NUMBER AS fiscal_year,
    v:"quarter"::STRING AS fiscal_period,
    v:"data" AS data
FROM {{ source('raw_data', 'raw_financial_json') }}