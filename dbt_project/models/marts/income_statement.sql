WITH raw AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        data
    FROM {{ ref('stg_raw_financials_json') }}
),
flattened_ic AS (
    SELECT
        r.company_name,
        r.fiscal_year,
        r.fiscal_period,
        ic.value AS ic_item
    FROM raw r,
         LATERAL FLATTEN(input => r.data:ic) ic
),
aggregated AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        MAX(CASE WHEN ic_item:concept::STRING = 'Revenues' THEN ic_item:value::NUMBER END) AS revenue,
        MAX(CASE WHEN ic_item:concept::STRING = 'OperatingIncomeLoss' THEN ic_item:value::NUMBER END) AS operating_income,
        MAX(CASE WHEN ic_item:concept::STRING = 'NetIncomeLoss' THEN ic_item:value::NUMBER END) AS net_income
    FROM flattened_ic
    GROUP BY company_name, fiscal_year, fiscal_period
)

SELECT * FROM aggregated