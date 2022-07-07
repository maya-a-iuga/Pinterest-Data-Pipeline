from ensurepip import bootstrap
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

# initialise instance of fastAPI
app = FastAPI()

# Pydantic class initialising different attributes to a specified data type
class Data(BaseModel):

    """This class gives information about data obtained from Pinterest
    
    Attributes
    ----------
    category: str
        A string representing the category of the entry
    index : str
        A string representing the index of the entry
    unique_id: str
        A string representing an unique id for each entry
    title: str
        A string representing the title of the entry
    description: str
        A string representing the description of the entry
    follower_count: str
        A string representing the number of followers
    tag_list: str
        A string contaning different tags relating to the entry
    is_image_or_video: str
        A string representing whether entry is a video or an image
    image_src: str
        A string containing the url of the image
    downloaded: str
        A string contaning 0s(for not downloaded) and 1s(for downloaded)
    save_location: str
        A string containing the directory path where entry has been saved to
    """
    
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

# Configure Producer which will send data to the KafkaPinterestTopic
pin_producer = KafkaProducer(
    bootstrap_servers = "localhost:9092",
    client_id = "Pinterest_producer",
    # converts message sent to topic into bytes
    value_serializer = lambda pinmessage: dumps(pinmessage).encode("ascii")
)

# post requests to localhost:8000/pin/
@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    # publish message to the topic
    pin_producer.send(topic = "KafkaPinterestTopic", value = data)


if __name__ == '__main__':
    # use uvicorn to run api -> processes requests asynchronously
    uvicorn.run("Pin_API_&_Kafka_Producer:app", host="localhost", port=8000)
