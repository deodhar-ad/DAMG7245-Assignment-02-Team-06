from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime

selected_year = Variable.get("selected_year", default_var="2016")
selected_quarter = Variable.get("selected_quarter", default_var="Q4")

aws_s3_bucket = Variable.get("AWS_S3_BUCKET", default_var="my-default-bucket")

default_args = {'owner': 'airflow', 'start_date': datetime(2024, 1, 1), 'retries': 1}
dag = DAG('create_fact_tables_to_snowflake',default_args=default_args, schedule_interval=None)

# Creating Stage to load JSON file
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

# create staging table if it doesn't exist
create_table_task = SnowflakeOperator(
    task_id='create_snowflake_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE TABLE IF NOT EXISTS raw_data.raw_financial_json (
        v VARIANT
    );
    """,
    dag=dag
)


# loading data into the staging table
load_json_task = SnowflakeOperator(
    task_id='load_json',
    snowflake_conn_id='snowflake_default',
    sql=f"""
    COPY INTO raw_data.raw_financial_json
    FROM @sec_json_stage
    FILES = ('{selected_year}{selected_quarter}.json')
    FILE_FORMAT = raw_data.json_file_format;
    """,
    dag=dag
)

# creating fact tables
create_tables_sql = """
CREATE OR REPLACE TABLE balance_sheet (
    company_name STRING,
    fiscal_year INT,
    fiscal_period STRING,
    total_assets NUMBER,
    total_liabilities NUMBER,
    total_equity NUMBER,
    CONSTRAINT pk_balance_sheet PRIMARY KEY (company_name, fiscal_year, fiscal_period)
);

CREATE OR REPLACE TABLE income_statement (
    company_name STRING,
    fiscal_year INT,
    fiscal_period STRING,
    revenue NUMBER,
    operating_income NUMBER,
    net_income NUMBER,
    CONSTRAINT pk_income_statement PRIMARY KEY (company_name, fiscal_year, fiscal_period)
);

CREATE OR REPLACE TABLE cash_flow (
    company_name STRING,
    fiscal_year INT,
    fiscal_period STRING,
    operating_cash_flow NUMBER,
    investing_cash_flow NUMBER,
    financing_cash_flow NUMBER,
    CONSTRAINT pk_cash_flow PRIMARY KEY (company_name, fiscal_year, fiscal_period)
);
"""

create_fact_tables_task = SnowflakeOperator(
    task_id='create_fact_tables',
    snowflake_conn_id='snowflake_default',
    sql=create_tables_sql,
    dag=dag
)


# DBT Tests for Validation
dbt_test_task = BashOperator(
    task_id="dbt_test",
    bash_command="cd /opt/airflow/dbt  && dbt test",
    dag=dag
)
# dbt_test_task = BashOperator(
#     task_id="dbt_test",
#     bash_command=(
#         "echo \"HOME is $HOME\" && echo \"PATH is $PATH\" && "
#         "echo \"Listing /home/airflow/.local/bin:\" && ls -la /home/airflow/.local/bin && "
#         "echo \"Effective user:\" && id && "
#         "cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test"
#     ),
#     dag=dag,
# )

# Run DBT inside Airflow
dbt_run_task = BashOperator(
    task_id="dbt_run",
    bash_command="cd /opt/airflow/dbt && dbt run",
    dag=dag
)


# task dependencies
create_stage_task >> create_table_task >> load_json_task >> create_fact_tables_task >> dbt_test_task >> dbt_run_task
