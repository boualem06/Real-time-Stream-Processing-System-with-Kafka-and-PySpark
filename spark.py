from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, approx_count_distinct, window,avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, DoubleType

if __name__ == "__main__":
    # Initialize SparkSession
        spark = (SparkSession.builder
                .appName("Cars_Streaming")
                .master("local[*]")
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
                .config("spark.jars", "/home/blm/learning/kafka_spark_project/postgresql-42.7.1.jar")
                .config("spark.sql.adaptive.enabled", "false")
                .getOrCreate())

        # Define schema for Kafka topic
        cars_schema = StructType([
                StructField("vehicle_id", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("speed", FloatType(), True),
                StructField("latitude", FloatType(), True),
                StructField("longitude", FloatType(), True),
                StructField("fuel_level", DoubleType(), True),
                StructField("engine_temp", FloatType(), True),
                StructField("battery_voltage", FloatType(), True),
                StructField("mileage", FloatType(), True),
                StructField("oil_level", FloatType(), True),
                StructField("driving_mode", StringType(), True),
                StructField("engine_status", StringType(), True),
                StructField("vehicle_type", StringType(), True),
        ])

        # Read Kafka stream
        cars_df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "vehicles_topic") \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), cars_schema).alias("data")) \
                .select("data.*")

        # Add a watermark for late-arriving data
        # cars_df_with_watermark = cars_df \
        #         .withWatermark("timestamp", "1 minute")  # Set the watermark threshold (1 minute)

        # Perform aggregation
        result_df = cars_df.filter(col("speed")>100)

        # Write the result to the console
        query_raw = result_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .start()

        query_raw.awaitTermination()

        



        # ****************************************************************************
        # get the over speeding cars 
        # cars_df=cars_df.filter(col("speed")>100)  <<<<<<<<<<<<we use append 

        # get low fuel cars 
        # cars_low_fuel=cars_df.filter(col("fuel_level")<10)   <<<<<<<< we use append 

        # get the number of cars and the average speed 
        # result_df = cars_df.agg(
        #         approx_count_distinct("vehicle_id").alias("distinct_cars_count"),
        #         avg("speed").alias("average_speed")
        # )
        #   # Write the result to the console
        # query_raw = result_df.writeStream \
        #         .outputMode("complete") \
        #         .format("console") \
        #         .start()

        # query_raw.awaitTermination()
