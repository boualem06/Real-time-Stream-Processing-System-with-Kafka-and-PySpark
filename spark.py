from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
    # Initialize SparkSession
    spark = (SparkSession.builder
             .appName("Cars_Streaming")
             .master("local[*]")  # Use local Spark execution with all available cores
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")  # Spark-Kafka integration
             .config("spark.jars",
                     "/home/blm/learning/kafka_spark_project/postgresql-42.7.1.jar")  # PostgreSQL driver
             .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
             .getOrCreate())

        # Define schemas for Kafka topics
    cars_schema = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp",  TimestampType(), True),
    StructField("speed", IntegerType(), True),
    StructField("latitude", IntegerType(), True),
    StructField("longitude", IntegerType(), True),
    StructField("fuel_level", IntegerType(), True),
    StructField("engine_temp", IntegerType(), True),
    StructField("battery_voltage", IntegerType(), True),
    StructField("mileage", IntegerType(), True),
    StructField("oil_level", IntegerType(), True),
    StructField("driving_mode", StringType(), True),
    StructField("engine_status", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    ])


    cars_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "vehicles_topic") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), cars_schema).alias("data")) \
        .select("data.*")

     # Data preprocessing: type casting and watermarking
    cars_df = cars_df.withColumn("timestamp", col("timestamp").cast(TimestampType())) \
                      
    cars_df = cars_df.withWatermark("timestamp", "1 minute")
    print(cars_df)

