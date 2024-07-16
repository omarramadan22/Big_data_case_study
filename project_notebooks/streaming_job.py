from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .getOrCreate()

# Kafka configuration
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
kafka_topic = "Eslam" 
kafka_username = "JUKQQM4ZM632RECA"
kafka_password = "UUkrPuSttgOC0U9lY3ZansNsKfN9fbxZPFwrGxudDrfv+knTD4rCwK+KdIzVPX0D"

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_password}";') \
    .load()

# Cast the value column to string
json_df = df.selectExpr("CAST(value AS STRING)")

# Write stream to HDFS
query = json_df \
    .writeStream \
    .outputMode("complete") \
    .format("avro") \
    .option("path", "hdfs://user/streaming") \
    .option("checkpointLocation", "hdfs://user/streaming_check") \
    .start()

query.awaitTermination()

# Stop Spark session (if needed)
# spark.stop()  # Uncomment if running as a standalone script
