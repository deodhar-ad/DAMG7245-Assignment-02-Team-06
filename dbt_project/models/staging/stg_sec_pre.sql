WITH raw_presentation AS (
    SELECT 
        adsh::STRING AS accession_number,
        report::INT AS report_number,
        line::INT AS line_number,
        stmt::STRING AS statement_type,
        tag::STRING AS tag_name,
        plabel::STRING AS presentation_label
    FROM RAW_DATA.SEC_PRESENTATION
)

SELECT * FROM raw_presentation
