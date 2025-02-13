WITH raw_num AS (
    SELECT 
        adsh::STRING AS accession_number,
        tag::STRING AS tag_name,
        version::STRING AS taxonomy_version,
        ddate::DATE AS report_date,
        qtrs::INT AS fiscal_quarters,
        uom::STRING AS unit_measure,
        segments::STRING AS segment_info,
        coreg::STRING AS coregistrant,
        value::FLOAT AS reported_value,
        footnote::STRING AS additional_notes
    FROM RAW_DATA.SEC_NUMBERS
)
SELECT * FROM raw_num
