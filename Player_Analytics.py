# -*- coding: utf-8 -*-
"""
Created on Thu May 23 22:34:16 2024

@author: 58947
"""

# Library import
import dbsa


# Get top N run scorer
def get_top_n_scorer(season: str, top_n: int):
    
    sql_to_get_top_run_scorer =f"""
    select BATSMAN,sum(BATSMAN_RUNS) from IPL_DB.IPL_INFO.DELIVERIES a join matches b on a.match_id = b.id
        where season ='{season}'
        group by all order by 2 desc limit {top_n}"""
    
    return dbsa.execute_sql(sql_to_get_top_run_scorer)
    
# Get IPL winner for each season
def get_ipl_winner_and_player_match():
    
    sql = f"""
        select season,winner,player_of_match from matches where season||to_date(date,'DD-MM-YYYY')in (
        select season||max(to_date(date,'DD-MM-YYYY'))  from matches
        group by  season)  order by 1   """
    
    return dbsa.execute_sql(sql)