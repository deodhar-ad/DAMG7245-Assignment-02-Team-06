WITH raw_sub AS (
    SELECT 
        adsh::STRING AS accession_number,
        cik::STRING AS company_cik,
        name::STRING AS company_name,
        sic::STRING AS industry_code,
        countryba::STRING AS business_country,
        cityba::STRING AS business_city,
        fy::INT AS fiscal_year,
        fp::STRING AS fiscal_period,
        period::DATE AS report_period
    FROM RAW_DATA.SEC_SUBMISSIONS
)

SELECT * FROM raw_sub
