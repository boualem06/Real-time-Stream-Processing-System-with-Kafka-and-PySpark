from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, approx_count_distinct, window,avg,to_json ,struct,current_timestamp
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



                # Get overspeeding cars with timestamp
        overspeeding_cars = cars_df.filter(col("speed") > 100) \
                .withColumn("value", to_json(struct("vehicle_id", "speed", "timestamp")))

        # Get low fuel cars with timestamp
        low_fuel_cars = cars_df.filter(col("fuel_level") < 10) \
                .withColumn("value", to_json(struct("vehicle_id", "fuel_level", "timestamp")))



        # Average speed for each car with timestamp
        avg_speed_each_car = cars_df.groupBy("vehicle_id") \
                .agg(avg("speed").alias("avg_speed_each_car")) \
                .withColumn("current_timestamp", current_timestamp()) \
                .withColumn("value", to_json(struct("vehicle_id", "avg_speed_each_car", "current_timestamp")))

        # Average oil level for each car with timestamp
        avg_oil_level_each_car = cars_df.groupBy("vehicle_id") \
                .agg(avg("oil_level").alias("avg_oil_level_each_car")) \
                .withColumn("current_timestamp", current_timestamp()) \
                .withColumn("value", to_json(struct("vehicle_id", "avg_oil_level_each_car", "current_timestamp")))

        # Get the number of cars and average speed
        overall_stats = cars_df.agg(
                approx_count_distinct("vehicle_id").alias("distinct_cars_count"),
                avg("speed").alias("average_speed")
        ).withColumn("value", to_json(struct("distinct_cars_count", "average_speed")))

          # Write the results to Kafka
        def write_to_kafka(df, topic,i):
                return df.selectExpr("CAST(value AS STRING)") \
                        .writeStream \
                        .format("kafka") \
                        .option("kafka.bootstrap.servers", "localhost:9092") \
                        .option("topic", topic) \
                        .option("checkpointLocation", f'/home/blm/learning/cars_stream_project /Real-time-Stream-Processing-System-with-Kafka-and-PySpark/checkpoints/checkpoint1{i}') \
                        .outputMode("update") \
                        .start()

        overspeeding_query = write_to_kafka(overspeeding_cars, "overspeeding_cars",1)
        low_fuel_query = write_to_kafka(low_fuel_cars, "low_fuel_cars",2)
        overall_stats_query = write_to_kafka(overall_stats, "overall_stats",3)
        avg_speed_query = write_to_kafka(avg_speed_each_car, "avg_speed_each_car",4)
        avg_oil_level_query = write_to_kafka(avg_oil_level_each_car, "avg_oil_level_each_car",5)

        spark.streams.awaitAnyTermination()













        # # Perform aggregation
        # # result_df = cars_df.filter(col("speed")>100)
        # car_avg_fuel=cars_df.groupBy("vehicle_id").agg(avg("oil_level").alias("avg_oil_level_each_car"))

        # # Write the result to the console
        # query_raw = car_avg_fuel.writeStream \
        #         .outputMode("complete") \
        #         .format("console") \
        #         .start()

        # query_raw.awaitTermination()

        



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


        # ************** 
        #average speed for each car 
        # car_avg_speed=cars_df.groupBy("vehicle_id").agg(avg("speed").alias("avg_speed_each_car"))

        #***********************
        #avg fuel of each car 
        # car_avg_fuel=cars_df.groupBy("vehicle_id").agg(avg("oil_level").alias("avg_oil_level_each_car"))


