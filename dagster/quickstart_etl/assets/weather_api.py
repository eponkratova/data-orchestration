#!/usr/bin/env python
# coding: utf-8

#import libraries
import pandas as pd
import requests
from pandas import json_normalize
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset, Definitions
import duckdb

@asset(group_name="weatherapi", compute_kind="Weather API")
def call_api():
    """The functions calls a weather API to extract the current forecast"""
    #passing weather API params
    BASE_URL = "http://api.weatherapi.com/v1/current.json"
    API_KEY = "212a0353103949f68ff83745231112"
    q = "London"
    resp = requests.get(f"{BASE_URL}?key={API_KEY}&q={q}")
    json_response = resp.json()       
    objects = json_normalize(json_response)
    #extracting only required columns
    objects = objects[["location.name", "location.region", "location.lat", "location.lon", 'current.precip_in', "current.humidity",
              "current.cloud", "current.feelslike_c", "current.feelslike_f",  "current.vis_km",  "current.vis_miles" , "current.uv",
              "current.gust_mph", "current.gust_kph"  ]]
    #renaming column names
    objects.columns = ["name", "region", "lat", "lon", "precip_in", "humidity", "cloud", "feelslike_c", "feelslike_f",
                       "vis_km", "vis_miles", "uv", "gust_mph", "gust_kph" ]
    objects.to_csv('weather.csv')

@asset(deps=[call_api], group_name="weatherapi", compute_kind="Database")
def save_data_to_db():
    '''The function saves the output file as a duckdb'''
    #creating a database connection and table
    conn = duckdb.connect('C:/Users/katep/OneDrive/Documents/data-orchestration/data-orchestration/assets/weather_db2.duckdb')
    sql = """
    CREATE OR REPLACE TABLE curr_weather as (
						select name, region, lat, lon, precip_in, humidity, cloud, feelslike_c, feelslike_f, vis_km, vis_miles, uv, gust_mph, gust_kph  
                         from 'C:/Users/katep/OneDrive/Documents/data-orchestration/data-orchestration-/dagster/data-orchestration-dagster/weatherapi/weather.csv'
    				);
		"""
    conn.execute(sql)