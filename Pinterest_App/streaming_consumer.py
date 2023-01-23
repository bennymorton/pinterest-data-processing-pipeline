from kafka import KafkaConsumer
from kafka import KafkaAdminClient
import json

consumer = KafkaConsumer(
    "pinterest-topic",
    bootstrap_servers="localhost:9092", 
    value_deserializer=lambda x: json.loads(x.decode("utf-8")))

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="Kafka Administrator"
)

for msg in consumer:
    print(msg.value)