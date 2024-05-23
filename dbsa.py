# -*- coding: utf-8 -*-
"""
Created on Sat May 18 12:21:00 2024

@author: 58947
"""

# This file is being used to connect to snowflake database and have functions to execute SQL commands.


#Importing Required Libraries
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from configparser import ConfigParser

# Snowflake Credentials

ctx = connect(
    user='SHARADBHARDWAJ',
    password='Sharad123#',
    account='nhdmegu-te99629',
    database='IPL_DB',
    schema='IPL_INFO',
    warehouse='COMPUTE_WH',
    role='ACCOUNTADMIN'
)

# Creating Snowflake Cursor
cur = ctx.cursor()

# Function to execute sql
def execute_sql(query: str):
    cur.execute(query)
    response = cur.fetchall()
    
    return response

# Function to load data from dataframe to snowflake table
def pd_to_tbl(out_df,tbl_name):
    write_pandas(
                    conn=ctx,
                    df=out_df,
                    table_name=tbl_name,
                    schema='IPL_INFO',
                    database='IPL_DB'
            )
        
    



