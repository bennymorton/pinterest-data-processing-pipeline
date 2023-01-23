from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

json_data = {"User_ID": "AINOFNS", "Event": "wepbage.open"}

producer.send('KafkaPythonTest', json_data)
producer.flush()