# Initial Database setup

-- Creating a new Database for IPL Dataset
CREATE OR REPLACE DATABASE IPL_DB;

--Use Database IPL_DB
USE DATABASE IPL_DB;

-- Create a New Schema for IPL_INFO

CREATE OR REPLACE SCHEMA IPL_DB.IPL_INFO;

USE SCHEMA IPL_INFO;

# Installing Python and Required libraries

pip install fastapi
pip install pandas
pip install sqlite3
pip install uvicorn


# Usage of Code
1. Open Command Prompt
2. Go to the Directory where main.py is stored
3. RUN uvicorn main:app --reload
4. Go to http://127.0.0.1:8000/docs#/

# Endpoint Details.

1.data_ingestion_endpoint
	This endpoint is used to ingest data and dump into snowflake table.
	It takes 2 Parameter
	1. FILE_NAME - Example - Player_Info
	2. TYPE  - csv (Supported are csv, json, sqlite)
	
2. create_matches dimension
	This endpoint is used to create TEAMS, VENUE, CITIES dimension from MATCHES table

3. get_top_n_run_scorer
	This endpoint is used to get to N run scorer of the season
	It takes 2 Parameter
	1. SEASON - IPL-2017
	2. N      - How many too scorer

4 get_winner_player_of_the_match
	This endpoint is used to get winner of IPL with respective Man of the Match