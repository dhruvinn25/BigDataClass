from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import snowflake.connector
import requests

import pandas as pd
import requests
from datetime import date, timedelta



def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_password')
    
    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


# @task
# def extract(url):
#     f = requests.get(url)
#     return (f.text)

@task
def extract(url, latitude: float, longitude: float, n: int=60):
    """
    EXTRACCT the past n days of weather for given co-ordinates (latitude and longitude) from given link
    Inputs:
        url: str
        latitude: float
        longitude: float
        n: int
    Returns:
        data: dict
    """

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": n,
        "forecast_days": 0,  # only past weather
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "auto"
    }

    response = requests.get(url, params=params)
    data = response.json()
    return data



# @task
# def transform(text):
#     lines = text.strip().split("\n")
#     records = []
#     for l in lines:  # remove the first row
#         (country, capital) = l.split(",")
#         records.append([country, capital])
#     return records[1:]

@task
def transform(data):
    """
    TRANSFORM given data into records
    Inputs:
        data: dict
    Returns:
        records: pandas.DataFrame
    """
    records = pd.DataFrame({
        "latitude": data["latitude"],
        "longitude": data["longitude"],
        "date": pd.to_datetime(data["daily"]["time"]).date,
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    return records



# @task
# def load(records, target_table):
#     cur = return_snowflake_conn()
#     try:
#         cur.execute("BEGIN;")
#         cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (country varchar primary key, capital varchar);")
#         cur.execute(f"DELETE FROM {target_table}")
#         for r in records:
#             country = r[0].replace("'", "''")
#             capital = r[1].replace("'", "''")
#             print(country, "-", capital)

#             sql = f"INSERT INTO {target_table} (country, capital) VALUES ('{country}', '{capital}')"
#             cur.execute(sql)
#         cur.execute("COMMIT;")
#     except Exception as e:
#         cur.execute("ROLLBACK;")
#         print(e)
#         raise e

@task
def load(table_name, records):
    """
    LOAD records into given table
    Inputs:
        table_name: str
        records: pandas.DataFrame
    """
    connection = return_snowflake_conn()

    table_name = f'{table_name}'

    try:
        connection.execute("BEGIN") # BEGIN TRANSACTION
        connection.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                precipitation FLOAT,
                weather_code INT,
                PRIMARY KEY (latitude, longitude, date)
        )
        """) # CREATE TABLE
        connection.execute(f"DELETE FROM {table_name}") #DELETE ALL RECORDS FROM TABLE
        for _, row in records.iterrows():
            sql = (f"""
                INSERT INTO {table_name} VALUES (
                    {row['latitude']},
                    {row['longitude']},
                    '{row['date']}',
                    {row['temp_max']},
                    {row['temp_min']},
                    {row['precipitation']},
                    {row['weather_code']}
                    )
                """) # INSERT VALUES IN TABLE
            # print(sql)
            connection.execute(sql)
        connection.execute("COMMIT") # COMMIT TRANSACTION
        connection.close()

    except Exception as e:
        connection.execute("ROLLBACK") # ROLLBACK TRANSACTION
        print(e)
        raise e

    

with DAG(
    dag_id = 'WeatherData_v1',
    start_date = datetime(2026,9,15),
    catchup=False,
    tags=['ETL'],
    schedule = '30 2 * * *'
) as dag:
    target_table = "raw.weather_data"
    
    url = "https://api.open-meteo.com/v1/forecast"
    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")
    
    # data = extract(url)
    # lines = transform(data)
    # load(lines, target_table)

    data = extract(url, LATITUDE, LONGITUDE, n=60)
    records = transform(data)
    load(target_table, records)