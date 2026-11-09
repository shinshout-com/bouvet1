# Databricks notebook source
import requests
from datetime import datetime
import pandas as pd

# Get today's date parts for the API URL
now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")

# Hvakosterstrømmen API endpoint for NO2 (Kristiansand/Sandnes/Stavanger region)
zone = "NO2"
url = f"https://www.hvakosterstrommen.no/api/v1/prices/{year}/{month}-{day}_{zone}.json"

print(f"Fetching from: {url} ({zone}: {year}-{month}-{day})")

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    # Convert JSON list to a Pandas dataframe first for easy inspection
    pdf = pd.DataFrame(data)
    display(pdf.head())
else:
    print(f"Failed to fetch data: {response.status_code}")

# COMMAND ----------

import requests
import pandas as pd

lat = 58.8524
lon = 5.7352

# MET requires a unique, descriptive User-Agent with contact info
headers = {
    "User-Agent": "WeatherPowerPipelineStudentProject ribsa4@gmail.com"
}

url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}"

print(f"Fetching from: {url} ({lat}, {lon})")

response = requests.get(url, headers=headers)
print(f"Status Code: {response.status_code}")

if response.status_code == 200:
    weather_data = response.json()
    timeseries = weather_data['properties']['timeseries']
    
    rows = []
    for entry in timeseries:
        time = entry['time']
        details = entry['data']['instant']['details']
        rows.append({
            "time": time,
            "air_temperature": details.get('air_temperature'),
            "wind_speed": details.get('wind_speed'),
            "relative_humidity": details.get('relative_humidity')
        })
        
    weather_pdf = pd.DataFrame(rows)
    display(weather_pdf.head(10))
else:
    print(f"Error details: {response.text}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
import json

# 1. Convert Pandas power data to Spark DF and write to Bronze table
power_spark_df = spark.createDataFrame(pdf)
power_bronze_df = (
    power_spark_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source", lit("hvakosterstrommen"))
)

# Write to Delta table in Bronze layer
power_bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_power_prices")
print("Saved bronze_power_prices successfully.")

# 2. Convert Pandas weather data to Spark DF and write to Bronze table
weather_spark_df = spark.createDataFrame(weather_pdf)
weather_bronze_df = (
    weather_spark_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source", lit("met_locationforecast"))
)

# Write to Delta table in Bronze layer
weather_bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze_weather")
print("Saved bronze_weather successfully.")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, date_trunc

# 1. Clean and standardize power data timestamps (using verified columns)
silver_power = (
    spark.read.table("bronze_power_prices")
    .withColumn("event_time", to_timestamp(col("time_start")))
    .withColumn("price_nok", col("NOK_per_kWh"))
    .select("event_time", "price_nok", "EUR_per_kWh", "EXR")
)

# 2. Clean and standardize weather data timestamps
silver_weather = (
    spark.read.table("bronze_weather")
    .withColumn("event_time", to_timestamp(col("time")))
    .select("event_time", "air_temperature", "wind_speed", "relative_humidity")
)

# 3. Truncate both to hourly level to ensure clean joining
silver_power_hourly = silver_power.withColumn("hour_slot", date_trunc("hour", col("event_time")))
silver_weather_hourly = silver_weather.withColumn("hour_slot", date_trunc("hour", col("event_time")))

# 4. Join the datasets on hour_slot
silver_joined = (
    silver_power_hourly.join(
        silver_weather_hourly,
        on="hour_slot",
        how="inner"
    )
    .select(
        col("hour_slot"),
        col("price_nok"),
        col("air_temperature"),
        col("wind_speed"),
        col("relative_humidity")
    )
    .na.drop()
)

# Write to Silver Delta table
silver_joined.write.format("delta").mode("overwrite").saveAsTable("silver_weather_power_joined")

print(f"Silver table created successfully with {silver_joined.count()} rows!")
display(silver_joined.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, round, avg, count

# 1. Read from our Silver joined table
silver_df = spark.read.table("silver_weather_power_joined")

# 2. Create temperature bins (rounded to nearest integer degree) and aggregate average price
gold_agg = (
    silver_df
    .withColumn("temp_bracket", round(col("air_temperature"), 0))
    .groupBy("temp_bracket")
    .agg(
        avg("price_nok").alias("avg_price_nok"),
        avg("wind_speed").alias("avg_wind_speed"),
        count("*").alias("observation_count")
    )
    .orderBy("temp_bracket")
)

# 3. Write to Gold Delta table
gold_agg.write.format("delta").mode("overwrite").saveAsTable("gold_weather_price_summary")

print("Gold summary table created successfully!")
display(gold_agg)

# COMMAND ----------

