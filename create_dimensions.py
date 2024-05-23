# -*- coding: utf-8 -*-
"""
Created on Thu May 23 22:25:58 2024

@author: 58947
"""

# Library Imports
import dbsa

# Function to Create dimensions
def create_dimension():
    
    try:
        sql_create_team_dimension = f"""
                            create or replace table IPL_DB.IPL_INFO.teams as
                            select distinct TEAM1 from IPL_DB.IPL_INFO.matches"""
                            
        sql_create_city_dimension = f"""
                            create or replace table IPL_DB.IPL_INFO.cities as
                            select distinct city from IPL_DB.IPL_INFO.matches"""
        
        sql_create_venue_dimension = f"""
                            create or replace table IPL_DB.IPL_INFO.venues as
                            select distinct venue from IPL_DB.IPL_INFO.matches"""
        
        dbsa.execute_sql(sql_create_team_dimension)
        dbsa.execute_sql(sql_create_city_dimension)
        dbsa.execute_sql(sql_create_venue_dimension)
        
        return "dimensions created successfully"
    except Exception as e:
        return "Errror"+ str(e)