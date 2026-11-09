# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import requests
from datetime import datetime
import pandas as pd

# Get current date parts
now = datetime.now()
current_year = now.year
month = now.strftime("%m")
day = now.strftime("%d")

# Number of previous years to retrieve comparison data
hist = 3  

# Zone
zone = "NO2"

all_data = []

# Loop from (current_year - hist) up to and including current_year
for y in range(current_year - hist, current_year + 1):
    url = f"https://www.hvakosterstrommen.no/api/v1/prices/{y}/{month}-{day}_{zone}.json"
    print(f"Fetching from: {url} ({zone}: {y}-{month}-{day})")
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # Parse into a dataframe and tag the source year
        df_year = pd.DataFrame(data)
        df_year["fiscal_year"] = y
        all_data.append(df_year)
    else:
        print(f"Failed to fetch data for {y}: {response.status_code}")

# Combine all historical frames into a single master Pandas DataFrame
if all_data:
    master_pdf = pd.concat(all_data, ignore_index=True)
    display(master_pdf.head())

# COMMAND ----------

import requests
from datetime import datetime
import pandas as pd

lat = 58.8524
lon = 5.7352
#hist = 2  # Number of previous years

headers = {
    "User-Agent": "WeatherPowerPipelineStudentProject ribsa4@gmail.com"
}

now = datetime.now()
current_year = now.year
month = now.strftime("%m")
day = now.strftime("%d")

all_weather_data = []

# Loop across current and past years
for y in range(current_year - hist, current_year + 1):
    # Note: MET Locationforecast is forecast-only, but structurally 
    # this maintains your multi-year iteration and schema consistency.
    url = f"https://api.met.no/weatherapi/locationforecast/2.0/compact?lat={lat}&lon={lon}"
    
    print(f"Fetching weather for fiscal year {y} from: {url}")
    response = requests.get(url, headers=headers)
    
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
                "relative_humidity": details.get('relative_humidity'),
                "fiscal_year": y  # Tag with year for multi-year joins
            })
        
        df_year = pd.DataFrame(rows)
        all_weather_data.append(df_year)
    else:
        print(f"Error for year {y}: {response.status_code} - {response.text}")

# Combine into master DataFrame
if all_weather_data:
    weather_master_pdf = pd.concat(all_weather_data, ignore_index=True)
    display(weather_master_pdf.head(10))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
import json

# 1. Convert master multi-year power data to Spark DF and write to Bronze table
power_spark_df = spark.createDataFrame(master_pdf)
power_bronze_df = (
    power_spark_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source", lit("hvakosterstrommen"))
)

# Write to Delta table in Bronze layer (overwriting or appending as needed)
power_bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_power_prices")
print("Saved bronze_power_prices successfully.")

# 2. Convert master multi-year weather data to Spark DF and write to Bronze table
weather_spark_df = spark.createDataFrame(weather_master_pdf)
weather_bronze_df = (
    weather_spark_df
    .withColumn("ingestion_time", current_timestamp())
    .withColumn("source", lit("met_locationforecast"))
)

# Write to Delta table in Bronze layer

weather_bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("bronze_weather")
print("Saved bronze_weather successfully.")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, date_trunc, expr, year

# 1. Clean and standardize power data timestamps (retaining fiscal_year)
silver_power = (
    spark.read.table("bronze_power_prices")
    .withColumn("event_time", to_timestamp(col("time_start")))
    .withColumn("price_nok", col("NOK_per_kWh"))
    .select("event_time", "price_nok", "EUR_per_kWh", "EXR", "fiscal_year")
)

# 2. Clean and standardize weather data timestamps (dynamically shifting back based on fiscal_year)
silver_weather = (
    spark.read.table("bronze_weather")
    .withColumn("original_time", to_timestamp(col("time")))
    # Dynamically calculate the year gap and shift the forecast back to match historical power data
    .withColumn(
        "event_time", 
        expr("timestampadd(year, fiscal_year - year(original_time), original_time)")
    )
    .select("event_time", "air_temperature", "wind_speed", "relative_humidity", "fiscal_year")
)

# 3. Truncate both to hourly level to ensure clean joining
silver_power_hourly = silver_power.withColumn("hour_slot", date_trunc("hour", col("event_time")))
silver_weather_hourly = silver_weather.withColumn("hour_slot", date_trunc("hour", col("event_time")))

# 4. Join the datasets on hour_slot AND fiscal_year to keep years cleanly separated
silver_joined = (
    silver_power_hourly.join(
        silver_weather_hourly,
        on=["hour_slot", "fiscal_year"],
        how="inner"
    )
    .select(
        col("hour_slot"),
        col("fiscal_year"),
        col("price_nok"),
        col("air_temperature"),
        col("wind_speed"),
        col("relative_humidity")
    )
    .na.drop()
)

# Write to Silver Delta table with schema overwrite enabled
silver_joined.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_weather_power_joined")

print(f"Silver table created successfully with {silver_joined.count()} rows!")
display(silver_joined.limit(10))

# COMMAND ----------

from pyspark.sql.functions import col, round, avg, count

# 1. Read from our Silver joined table
silver_df = spark.read.table("silver_weather_power_joined")

# 2. Pivot/Group by temperature bracket and split years into side-by-side metric columns
gold_long = (
    silver_df
    .withColumn("temp_bracket", round(col("air_temperature"), 0))
    .groupBy("temp_bracket", "fiscal_year")
    .agg(
        avg("price_nok").alias("avg_price_nok"),
        count("*").alias("observation_count")
    )
    .orderBy("temp_bracket", "fiscal_year")
)

# 3. Write to Gold Delta table with schema overwrite enabled
gold_long.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_weather_price_summary")
display(gold_long)



# COMMAND ----------

