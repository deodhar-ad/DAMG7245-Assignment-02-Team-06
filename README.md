# Data Ingestion of SEC Financial Data to Snowflake 

## Team Members -
- Aditi Ashutosh Deodhar 002279575
- Lenin Kumar Gorle 002803806
- Poorvika Girish Babu 002801388

## Project Overview

### Problem Statement
Financial analysts need structured, easily accessible financial data to assess companies' performances. However, SEC data is delivered in raw TSV format, making it difficult to query and analyze efficiently.

### Scope
- Processed output is stored on Snowflake as tables and views based on the year and quarter configured

## Methodology
- Data fetched dynamically from the SEC website
- Financial datasets in TSV format and Selenium Webdriver for data scraping
- Stores raw TSV & processed JSON data
- Designed Airflow pipelines executed to fetch data
- Integrated DBT to airflow to validate the data and insert into Snowflake tables and views

## Technologies Used

### Backend
- Python
- FastAPI

### Frontend
- Streamlit

### Cloud & Devops
- AWS
- Apache Airflow
- Docker

## Architecture Diagram

![image](https://github.com/user-attachments/assets/be58daa7-1193-479f-ad4a-33d3e1c7cf28)

## Codelabs Documentation

## Hosted Applications links 

## Demo 


## Prerequisites
- Python 3.9+
- AWS Account
- Docker Account
- Snowflake Account

## Set Up the Environment
```
   # Clone the environment
     https://github.com/deodhar-ad/DAMG7245-Assignment-02-Team-06.git
     cd DAMG7245-Assignment-01
   # Add all the environmental variables in .env file
   # Add all the libraries needed for backend code (Fast API)
   # docker-compose up -d command to trigger the DAG's to insert data on Snowflake
```

## Project Structure
```
DAMG7245-Assignment-01/
├─ airflow/                 # Airflow Dags
├─ dbt_project/             # DBT code with sql files and tests to validate and insert the data
├─ backend/                 # Fast API's to access the data from Snowflake
├─ frontend/                # Streamlit 
├─ storage/                 # AWS S3 code  
└─ docker-compose.yaml      # Docker file to trigger the DAG
└─ requirements.txt         # Libraries to be installed on render
 ```  





