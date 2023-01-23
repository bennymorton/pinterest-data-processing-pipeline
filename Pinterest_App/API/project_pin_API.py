from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

app = FastAPI()

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: dumps(x).encode("utf-8")
)

@app.post("/pin/")
def send_to_kafka(item: Data):
    data = dict(item)
    # print(item)
    # return item
    producer.send('pinterest-topic', data)
    producer.flush()


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
