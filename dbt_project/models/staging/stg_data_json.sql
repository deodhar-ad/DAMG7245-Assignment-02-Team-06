WITH raw_json AS (
    SELECT v AS json_data  
    FROM RAW_DATA.SEC_FINANCIAL_JSON
)

SELECT
    json_data:name::STRING AS company_name,
    json_data:year::INT AS fiscal_year,
    json_data:quarter::STRING AS fiscal_period,
    json_data:data AS financial_data
FROM raw_json