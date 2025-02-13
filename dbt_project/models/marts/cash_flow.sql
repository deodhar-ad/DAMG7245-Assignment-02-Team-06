WITH raw AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        data
    FROM {{ ref('stg_raw_financials_json') }}
),
flattened_cf AS (
    SELECT
        r.company_name,
        r.fiscal_year,
        r.fiscal_period,
        cf.value AS cf_item
    FROM raw r,
         LATERAL FLATTEN(input => r.data:cf) cf
),
aggregated AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        MAX(CASE WHEN cf_item:concept::STRING = 'NetCashProvidedByUsedInOperatingActivities' THEN cf_item:value::NUMBER END) AS operating_cash_flow,
        MAX(CASE WHEN cf_item:concept::STRING = 'NetCashProvidedByUsedInInvestingActivities' THEN cf_item:value::NUMBER END) AS investing_cash_flow,
        MAX(CASE WHEN cf_item:concept::STRING = 'NetCashProvidedByUsedInFinancingActivities' THEN cf_item:value::NUMBER END) AS financing_cash_flow
    FROM flattened_cf
    GROUP BY company_name, fiscal_year, fiscal_period
)

SELECT * FROM aggregated