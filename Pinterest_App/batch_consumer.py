from kafka import KafkaConsumer
from kafka import KafkaAdminClient
import json
import boto3

consumer = KafkaConsumer(
    "pinterest-topic",
    bootstrap_servers="localhost:9092"
    # value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

for msg in consumer:
    object = s3_resource.Object('pinterest-data-d255ea73-811a-4331-98f6-7692459e2620', str(msg.timestamp))
    s3_client.put_object(Body=msg.value, Bucket='pinterest-data-d255ea73-811a-4331-98f6-7692459e2620', Key=str(msg.timestamp))