from pyspark.sql import SparkSession
import os

def bronze_to_silver(spark):
    # Download raw Parquet
    bronze_path = "data/bronze/yellow_tripdata_2023-01.parquet"
    if not os.path.exists(bronze_path):
        os.makedirs(os.path.dirname(bronze_path), exist_ok=True)
        import requests
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
        r = requests.get(url)
        open(bronze_path, 'wb').write(r.content)
    
    # Read bronze
    df_bronze = spark.read.parquet(bronze_path)
    
    # Silver: Clean, dedup, filter invalid
    df_silver = df_bronze.dropDuplicates() \
        .filter("passenger_count > 0 AND trip_distance > 0 AND fare_amount > 0") \
        .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
        .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    
    silver_path = "data/silver/yellow_trips_clean.parquet"
    os.makedirs(os.path.dirname(silver_path), exist_ok=True)
    df_silver.write.mode("overwrite").parquet(silver_path)
    
    print("Bronze to Silver completed")
    return df_silver