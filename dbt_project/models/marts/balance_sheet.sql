WITH raw AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        data
    FROM {{ ref('stg_raw_financials_json') }}
),
flattened_bs AS (
    SELECT
        r.company_name,
        r.fiscal_year,
        r.fiscal_period,
        bs.value AS bs_item
    FROM raw r,
         LATERAL FLATTEN(input => r.data:bs) bs
),
aggregated AS (
    SELECT
        company_name,
        fiscal_year,
        fiscal_period,
        COALESCE(
            MAX(CASE WHEN bs_item:concept::STRING = 'Assets' THEN bs_item:value::NUMBER END),
            SUM(CASE 
                    WHEN bs_item:concept::STRING IN ('AssetsCurrent', 'PropertyPlantAndEquipmentNet', 'Goodwill', 'IntangibleAssetsNetExcludingGoodwill') 
                    THEN bs_item:value::NUMBER 
                    ELSE 0 
                END)
        ) AS total_assets,
        MAX(CASE WHEN bs_item:concept::STRING IN ('Liabilities', 'LiabilitiesCurrent') THEN bs_item:value::NUMBER END) AS total_liabilities,
        MAX(CASE WHEN bs_item:concept::STRING IN ('StockholdersEquity', 'Equity') THEN bs_item:value::NUMBER END) AS total_equity
    FROM flattened_bs
    GROUP BY company_name,fiscal_year, fiscal_period
)

SELECT * FROM aggregated