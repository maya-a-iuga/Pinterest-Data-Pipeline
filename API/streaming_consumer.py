from kafka import KafkaConsumer
from json import loads

#Create consumer to retrieve the messages from the topic
stream_pin_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda pinmessage: loads(pinmessage),
    #ensures messages are read from the beggining
    auto_offset_reset = "earliest"
)

stream_pin_consumer.subscribe(topics = "KafkaPinterestTopic")

for message in stream_pin_consumer:
    print(message)