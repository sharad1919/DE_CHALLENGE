# -*- coding: utf-8 -*-
"""
Created on Sat May 18 12:51:24 2024

@author: 58947
"""

# Importing required library
from fastapi import FastAPI
import data_ingestion_module
import create_dimensions
import Player_Analytics


# Initializing Fastapi
app = FastAPI()

# Endpoint to ingest .csv, .json, .sqlite file into table
@app.post("/data_ingestion_endpoint")
def data_ingestion(filename: str, file_type: str):
    
    if file_type =='csv':
        data_ingestion_module.csv_file_ingestion(filename)
    
    elif file_type =='json':
        data_ingestion_module.json_file_ingestion(filename)
    
    elif file_type =='sqlite':
        data_ingestion_module.sqlite_ingestion(filename)

# Create Match Dimension
@app.post("/create_matches_dimensions")
def create_matches_dimensions():
    
    create_dimensions.create_dimension()

# Endpoint to get top N run scorer
@app.get("/get_top_n_run_scorer")
def get_top_run_scorer(season: str,number: int):
    
    return Player_Analytics.get_top_n_scorer(season,number)

# Endpoint to get winner of each IPL season with player of the Match
@app.get("/get_winner_and_player_of_the_match")
def get_winner_and_player_of_the_match():
    return Player_Analytics.get_ipl_winner_and_player_match()

    