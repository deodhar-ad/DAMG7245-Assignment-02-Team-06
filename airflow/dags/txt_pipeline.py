"""
Airflow DAG for:
1. Checking & creating Snowflake S3 stage
2. Loading txt data from S3 into Snowflake
3. Running DBT transformations
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime

# Load Airflow Variables (default to latest quarter)
selected_year = Variable.get("selected_year", default_var="2016")
selected_quarter = Variable.get("selected_quarter", default_var="q4")

# Fetch S3 Bucket Name from Airflow Variables
aws_s3_bucket = Variable.get("AWS_S3_BUCKET", default_var="team-6-a2-ds")

# DAG Configuration
default_args = {'owner': 'airflow', 'start_date': datetime(2024, 1, 1), 'retries': 1}
dag = DAG(dag_id='txt_s3_to_snowflake_dbt', default_args=default_args, schedule_interval=None, catchup=False)

# Task 1: Create External Snowflake Stage Using Manual AWS Credentials
create_stage_task1 = SnowflakeOperator(
    task_id='creating_snowflake_stage',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    CREATE OR REPLACE STAGE sec_txt_stage
    URL = 's3://team-6-a2-ds/sec_extracted_tsv/'
    CREDENTIALS = (AWS_KEY_ID='{{{{ conn.aws_default.login }}}}'
                   AWS_SECRET_KEY='{{{{ conn.aws_default.password }}}}');
    """,
    dag=dag
)

# Task 2: Create Snowflake Table (if not exists)
create_table_task1 = SnowflakeOperator(
    task_id='creating_snowflake_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS raw_data.sec_numbers (
    adsh STRING,
    tag STRING,
    version STRING,
    ddate DATE,
    qtrs INT,
    uom STRING,
    segments STRING,
    coreg STRING,
    value FLOAT,
    footnote STRING
    );

    CREATE TABLE IF NOT EXISTS raw_data.sec_submissions (
    adsh STRING,
    cik STRING,
    name STRING,
    sic STRING,
    countryba STRING,
    stprba STRING,
    cityba STRING,
    zipba STRING,
    bas1 STRING,
    bas2 STRING,
    baph STRING,
    countryma STRING,
    stprma STRING,
    cityma STRING,
    zipma STRING,
    mas1 STRING,
    mas2 STRING,
    countryinc STRING,
    stprinc STRING,
    ein STRING,
    former STRING,
    changed DATE,
    afs STRING,
    wksi BOOLEAN,
    fye STRING,
    form STRING,
    period DATE,
    fy INT,
    fp STRING,
    filed DATE,
    accepted TIMESTAMP,
    prevrpt BOOLEAN,
    detail BOOLEAN,
    instance STRING,
    nciks INT,
    aciks STRING
    );

    CREATE TABLE IF NOT EXISTS raw_data.sec_tags (
    tag STRING,
    version STRING,
    custom BOOLEAN,
    abstract BOOLEAN,
    datatype STRING,
    iord STRING,
    crdr STRING,
    tlabel STRING,
    doc STRING
    );

    CREATE TABLE IF NOT EXISTS raw_data.sec_presentation (
    adsh STRING,
    report INT,
    line INT,
    stmt STRING,
    inpth BOOLEAN,
    rfile STRING,
    tag STRING,
    version STRING,
    plabel STRING,
    negating BOOLEAN
    );

    """,
    dag=dag
)

# Task 3: Load txt from S3 → Snowflake (Filters by Year & Quarter)
load_txt_task1 = SnowflakeOperator(
    task_id='load_txt',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    COPY INTO raw_data.sec_numbers
    FROM @sec_txt_stage
    FILES = ('{selected_year}{selected_quarter}/num.txt')
    FILE_FORMAT = raw_data.tsv_file_format;

    COPY INTO raw_data.sec_submissions
    FROM @sec_txt_stage
    FILES = ('{selected_year}{selected_quarter}/sub.txt')
    FILE_FORMAT = raw_data.tsv_file_format;

    COPY INTO raw_data.sec_tags
    FROM @sec_txt_stage
    FILES = ('{selected_year}{selected_quarter}/tag.txt')
    FILE_FORMAT = raw_data.tsv_file_format;

    COPY INTO raw_data.sec_presentation
    FROM @sec_txt_stage
    FILES = ('{selected_year}{selected_quarter}/pre.txt')
    FILE_FORMAT = raw_data.tsv_file_format;

    """,
    dag=dag
)

# Task 4: Run DBT Tests for JSON Validation
dbt_test_task1 = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt && dbt test",
    dag=dag
)

# Task 5: Run DBT inside Airflow
dbt_run_task1 = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run",
    dag=dag
)

# DAG Task Flow: Create Stage → Create Table → Load JSON → Run DBT
create_stage_task1 >> create_table_task1 >> load_txt_task1 >>  dbt_test_task1 >> dbt_run_task1