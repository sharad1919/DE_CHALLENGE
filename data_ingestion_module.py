# -*- coding: utf-8 -*-
"""
Created on Sat May 18 12:57:54 2024

@author: 58947
"""

# Library imports
import pandas as pd
import dbsa
import sqlite3
import json

# Function to ingest CSV file and dump data to snowflake
def csv_file_ingestion(filename: str):
    
    df = pd.read_csv(f"{filename}.csv")
    df = df.astype(str)
    df.columns = [col.upper().replace(' ', '_').replace('-','_') for col in df.columns]
    df = df.where(pd.notnull(df), None)
    columns = df.columns
    table_name = filename.upper()
    
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS IPL_DB.IPL_INFO.{table_name} (\n"
    fflag = 0 
    for col in columns:
        if "Unnamed" in col:
            continue
        if fflag ==0:
            create_table_query += f"{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
            fflag=1
            continue
        create_table_query += f",{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
    create_table_query += "\n);"
    
    print(create_table_query)
    
    truncate_query = f"truncate table IPL_DB.IPL_INFO.{table_name}"
    
    dbsa.execute_sql(create_table_query)
    dbsa.execute_sql(truncate_query)
    dbsa.pd_to_tbl(df,table_name)

# Function to ingest JSON file and dump data to snowflake
def json_file_ingestion(filename: str):
    with open(f"{filename}.json", 'r') as file:
        json_data = json.load(file)
        
    df = pd.json_normalize(json_data)

    # Step 2: Convert all data to string type
    df = df.astype(str)

    # Step 3: Replace 'NaN' with None (which Snowflake interprets as NULL)
    df = df.where(pd.notnull(df), None)

    # Step 4: Standardize column names
    df.columns = [col.upper().replace(' ', '_').replace('-', '_') for col in df.columns]
    columns = df.columns
    table_name = filename.upper()
    create_table_query = f"CREATE TABLE IF NOT EXISTS IPL_DB.IPL_INFO.{table_name} (\n"
    fflag =0 
    for col in columns:
        if "Unnamed" in col:
            continue
        if fflag ==0:
            create_table_query += f"{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
            fflag=1
            continue
        create_table_query += f",{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
    create_table_query += "\n);"
    
    print(create_table_query)
    
    truncate_query = f"truncate table IPL_DB.IPL_INFO.{table_name}"
    
    dbsa.execute_sql(create_table_query)
    dbsa.execute_sql(truncate_query)
    dbsa.pd_to_tbl(df,table_name)
    

# Function to ingest SQLITE file and dump data to snowflake
def sqlite_ingestion(filename: str):
    conn_sqlite = sqlite3.connect('IPL_Deliveries.sqlite')
    query = f"SELECT * FROM {filename}"
    df = pd.read_sql_query(query, conn_sqlite)

    # Close the SQLite connection
    conn_sqlite.close()

    # Step 2: Convert all data to string type
    df = df.astype(str)

    # Step 3: Replace 'NaN' with None (which Snowflake interprets as NULL)
    df = df.where(pd.notnull(df), None)

    # Step 4: Standardize column names
    df.columns = [col.upper().replace(' ', '_').replace('-', '_') for col in df.columns]
    columns = df.columns
    table_name = filename.upper()
    create_table_query = f"CREATE TABLE IF NOT EXISTS IPL_DB.IPL_INFO.{table_name} (\n"
    fflag =0 
    for col in columns:
        if "Unnamed" in col:
            continue
        if fflag ==0:
            create_table_query += f"{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
            fflag=1
            continue
        create_table_query += f",{col.upper().replace(' ','_').replace('-','_')} VARCHAR\n"
    create_table_query += "\n);"
    
    print(create_table_query)
    
    truncate_query = f"truncate table IPL_DB.IPL_INFO.{table_name}"
    
    dbsa.execute_sql(create_table_query)
    dbsa.execute_sql(truncate_query)
    dbsa.pd_to_tbl(df,table_name)

    