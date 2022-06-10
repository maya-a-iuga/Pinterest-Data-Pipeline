from ensurepip import bootstrap
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

#Configure producer which will send data to the KafkaPinterestTopic
#value_serializer = converts message sent to topic into bytes
pin_producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    client_id = "Pinterest_producer",
    value_serializer = lambda pinmessage: dumps(pinmessage).encode("ascii")
)


@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    pin_producer.send(topic = "KafkaPinterestTopic", value = data)
    #return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
