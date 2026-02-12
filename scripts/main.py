from pyspark.sql import SparkSession
from bronze_to_silver import bronze_to_silver
from silver_to_gold import silver_to_gold

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MedallionPipeline") \
        .master("spark://localhost:7077") \  # Local cluster docker-compose
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    df_silver = bronze_to_silver(spark)
    silver_to_gold(spark)
    
    spark.stop()