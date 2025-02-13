WITH raw_tags AS (
    SELECT 
        tag::STRING AS tag_name,
        version::STRING AS taxonomy_version,
        custom::BOOLEAN AS is_custom,
        datatype::STRING AS data_type,
        crdr::STRING AS credit_debit
    FROM RAW_DATA.SEC_TAGS
)

SELECT * FROM raw_tags
