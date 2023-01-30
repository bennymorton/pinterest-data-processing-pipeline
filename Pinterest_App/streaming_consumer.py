from kafka import KafkaConsumer
from kafka import KafkaAdminClient
from pyspark.sql import SparkSession
import json
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 streaming_consumer.py pyspark-shell'
kafka_topic_name = 'pinterest-topic'
kafka_bootstrap_servers = 'localhost:9092'

consumer = KafkaConsumer(
    "pinterest-topic",
    bootstrap_servers="localhost:9092", 
    value_deserializer=lambda x: json.loads(x.decode("utf-8")))

spark = SparkSession \
    .builder \
    .appName('Kafka') \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data_df = spark \
    .readStream \
    .format('Kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', kafka_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load()

data_df \
    .writeStream \
    .outputMode('append') \
    .format('console') \
    .start() \
    .awaitTermination()


admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

# for msg in consumer:
    # print(msg.value)