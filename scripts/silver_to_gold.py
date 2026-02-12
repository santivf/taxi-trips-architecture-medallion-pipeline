from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def silver_to_gold(spark):
    silver_path = "data/silver/yellow_trips_clean.parquet"
    df_silver = spark.read.parquet(silver_path)
    
    # Gold: Aggregations (daily revenue, avg trip distance, top zones)
    df_gold_daily = df_silver.groupBy(date_trunc("day", "pickup_datetime").alias("trip_date")) \
        .agg(
            count("*").alias("total_trips"),
            sum("fare_amount").alias("total_revenue"),
            avg("trip_distance").alias("avg_distance")
        ).orderBy("trip_date")
    
    df_gold_zones = df_silver.groupBy("PULocationID", "DOLocationID") \
        .agg(count("*").alias("trips_count")) \
        .orderBy(desc("trips_count")).limit(10)
    
    gold_daily_path = "data/gold/daily_metrics.parquet"
    gold_zones_path = "data/gold/top_zones.parquet"
    
    os.makedirs("data/gold", exist_ok=True)
    df_gold_daily.write.mode("overwrite").parquet(gold_daily_path)
    df_gold_zones.write.mode("overwrite").parquet(gold_zones_path)
    
    print("Silver to Gold completed")