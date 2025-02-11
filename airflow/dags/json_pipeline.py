"""
Airflow DAG for:
1. Checking & creating Snowflake S3 stage
2. Loading JSON data from S3 into Snowflake
3. Running DBT transformations
"""

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime

# Load Airflow Variables (default to latest quarter)
selected_year = Variable.get("selected_year", default_var="2016")
selected_quarter = Variable.get("selected_quarter", default_var="Q4")

# Fetch S3 Bucket Name from Airflow Variables
aws_s3_bucket = Variable.get("AWS_S3_BUCKET", default_var="my-default-bucket")

# DAG Configuration
default_args = {'owner': 'airflow', 'start_date': datetime(2024, 1, 1), 'retries': 1}
dag = DAG('json_s3_to_snowflake_dbt', default_args=default_args, schedule_interval=None)

# Task 1: Create External Snowflake Stage Using Manual AWS Credentials
create_stage_task = SnowflakeOperator(
    task_id='create_snowflake_stage',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    CREATE OR REPLACE STAGE sec_json_stage
    URL = 's3://{aws_s3_bucket}/sec_json_data/'
    CREDENTIALS = (AWS_KEY_ID='{{{{ conn.aws_default.login }}}}'
                   AWS_SECRET_KEY='{{{{ conn.aws_default.password }}}}');
    """,
    dag=dag
)

# Task 2: Create Snowflake Table (if not exists)
create_table_task = SnowflakeOperator(
    task_id='create_snowflake_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS raw_data.sec_financial_json (
        symbol STRING,
        year INT,
        quarter STRING,
        financial_data VARIANT
    );
    """,
    dag=dag
)

# Task 3: Load JSON from S3 → Snowflake (Filters by Year & Quarter)
load_json_task = SnowflakeOperator(
    task_id='load_json',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    COPY INTO raw_data.sec_financial_json
    FROM @sec_json_stage
    FILES = ('{selected_year}{selected_quarter}.json')
    FILE_FORMAT = raw_data.json_file_format;
    """,
    dag=dag
)

# Task 4: Run DBT Tests for JSON Validation
dbt_test_task = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt && dbt test",
    dag=dag
)

# Task 5: Run DBT inside Airflow
dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run",
    dag=dag
)

# DAG Task Flow: Create Stage → Create Table → Load JSON → Run DBT
create_stage_task >> create_table_task >> load_json_task >>  dbt_test_task >> dbt_run_task