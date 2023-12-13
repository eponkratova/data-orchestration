#importing libraries
import io
import pandas as pd
import requests
from pandas.io.json import json_normalize
from mage_ai.data_preparation.shared.secrets import get_secret_value
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

#importing variables
BASE_URL = get_secret_value('BASE_URL')
API_KEY = get_secret_value('API_KEY')
q = get_secret_value('q')

@data_loader
def call_api(*args, **kwargs):
    """The functions calls a weather API to extract the current forecast"""
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
    return objects