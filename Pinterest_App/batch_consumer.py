# Batch processing of data from Kafka topic
# Loading it into an AWS S3 bucket

from kafka import KafkaConsumer
import json
import boto3

consumer = KafkaConsumer(
    "pinterest-topic",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)
s3_resource = boto3.resource('s3')

bucket = s3_resource.Bucket('pinterest-data-d255ea73-811a-4331-98f6-7692459e2620')

for id, msg in enumerate(consumer):
    filename = 'event' + str(id) + '.json'
    bucket.put_object(Body=json.dumps(msg.value), Key=filename)