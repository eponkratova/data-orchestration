#importing libraries
import duckdb

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(objects, *args, **kwargs):
    """The function creates a duckdb table and stores the output df"""
    #creating a database objects
    conn = duckdb.connect('/home/src/weather_db2.duckdb')
    sql = '''CREATE OR REPLACE TABLE curr_weather (name string, 
                                        region string, 
                                        lat string, 
                                        lon string, 
                                        precip_in numeric,
                                        humidity numeric, 
                                        cloud numeric, 
                                        feelslike_c numeric, 
                                        feelslike_f numeric,
                                        vis_km numeric, 
                                        vis_miles numeric, 
                                        uv numeric, 
                                        gust_mph numeric, 
                                        gust_kph numeric)'''
        
    conn.execute(sql)
    #inserting data to the database
    conn.execute("INSERT INTO curr_weather SELECT * FROM objects")
    conn.close()
    print("Done")
    #testing the output
    #print(conn.execute("SELECT * from curr_weather ").fetchdf())

