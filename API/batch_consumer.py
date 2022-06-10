from kafka import KafkaConsumer
from json import loads
from json import dumps
import boto3

#Create consumer to retrieve the messages from the topic
batch_pin_consumer = KafkaConsumer(
    bootstrap_servers = "localhost:9092",
    value_deserializer = lambda pinmessage: loads(pinmessage),
    #ensures messages are read from the beggining
    auto_offset_reset = "earliest"
)

batch_pin_consumer.subscribe(topics = "KafkaPinterestTopic")

#for message in batch_pin_consumer:
 #   print(message)

#send data to S3 bucket
s3_resource = boto3.resource('s3')

bucket_name = "pinterestpipelinebucket"
my_bucket = s3_resource.Bucket(bucket_name)

counter = 0

for message in batch_pin_consumer:
    
    json_object = dumps(message.value)
    s3_filename = 'events/' + str(counter) + '.json'
    my_bucket.put_object(Key = s3_filename , Body = json_object)
    counter += 1