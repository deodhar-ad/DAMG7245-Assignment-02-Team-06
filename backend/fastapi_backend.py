from fastapi import FastAPI, HTTPException
import snowflake.connector
import os
import pandas as pd
import io
import csv
from fastapi.responses import StreamingResponse, JSONResponse
from dotenv import load_dotenv

app = FastAPI()

load_dotenv()
# Allowed table names for denormalized data
ALLOWED_TABLES = {"balance_sheet", "income_statement", "cash_flow"}

# Load Snowflake Credentials from Environment Variables
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")


def get_snowflake_connection():
    """
    Establishes a connection to Snowflake.
    """
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            role=SNOWFLAKE_ROLE
        )
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to connect to Snowflake: {str(e)}")


@app.get("/")
def home():
    return {"message": "FastAPI JSON View API is running!"}


# Fetch ALL JSON Data from the STG_DATA_JSON View
@app.get("/json_view/")
def get_json_view_data():
    """
    Fetch all JSON data from the STG_DATA_JSON view.
    """
    query = """
        SELECT * FROM raw_data.STG_DATA_JSON
    """

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        cur.execute(query)
        data = [dict(zip([column[0] for column in cur.description], row)) for row in cur.fetchall()]

        cur.close()
        conn.close()

        if not data:
            raise HTTPException(status_code=404, detail="No data found in the view.")

        return {"json_view_data": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
@app.get("/raw_data")
def get_raw_data():
    pass

@app.get("/denormalized/preview/{table_name}")
def get_denormalized_preview(table_name: str):
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name. Must be one of: balance_sheet, income_statement, cash_flow")
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        query = f"SELECT * FROM {table_name} LIMIT 20"
        cur.execute(query)
        data = cur.fetchall()
        # You can also include column names
        columns = [desc[0] for desc in cur.description]
        return JSONResponse(content={"columns": columns, "data": data})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()

@app.get("/denormalized/download/{table_name}")
def download_denormalized(table_name: str):
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail="Invalid table name. Must be one of: balance_sheet, income_statement, cash_flow")
    
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        query = f"SELECT * FROM {table_name}"
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        
        output = io.StringIO()
        writer = csv.writer(output)
        # Write header row
        writer.writerow(columns)
        writer.writerows(rows)
        output.seek(0)

        headers = {"Content-Disposition": f"attachment; filename={table_name}.csv"}
        return StreamingResponse(output, media_type="text/csv", headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cur.close()
        conn.close()
