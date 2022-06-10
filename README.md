# Pinterest-Data-Pipeline
An end-to-end data processing pipeline in Python based on Pinterests experiment processing pipeline.

## Data ingestion: Apache Kafka
Firstly, an Apache Kafka topic was created. Then, using kafka-python a Kafka Producer was intitialised. This producer sends events from the Pinterest API to the created topic.

Two Kafka consumers were created: one for batch processing and one for real-time streaming, both receiving. data from the previously created topic. The batch consumer sends each individual event to an AWS S3 bucket (in the form of a json file) for long-term persistent storage
