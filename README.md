# Pinterest-Data-Pipeline
An end-to-end data processing pipeline in Python based on Pinterest's experiment processing pipeline. Implemented based on Lambda architecture to take advantage of both batch and stream-processing.

## Data ingestion: Apache Kafka
Firstly, an Apache Kafka topic was created. Then, using kafka-python a Kafka Producer was initialised. This producer sends events from the Pinterest API to the created topic.

Next, two Kafka consumers were created: one for batch processing and one for real-time streaming. The batch consumer sends individual events to an AWS S3 bucket (in the form of a json file) for long-term persistent storage.

## Batch processing: Process the data using Spark
A Spark session that reads data from the S3 bucket, such that the previously saved json files can be read back and stored in a Spark DataFrame. Using PySpark, different transformations were applied to this DataFrame for data cleaning.

## Batch processing: Send processed data to Apache Cassandra
Apache Cassandra was configured locally to receive data from Spark. The Cassandra table had the same columnar structure as the previously created Spark DataFrame. Using a Cassandra connector, the data was sent from Spark to Cassandra for long-term storage.

## Batch processing: Connect Presto to Cassandra
Presto is a powerful data querying engine that does not provide its own data storage platform. Integrating Presto with Cassandra allows to leverage Presto's powerful data querying engine along with Cassandra's data storage benefits. Thus, Presto was set up to run ad-hoc queries on the batch data.

## Batch processing: Orchestrate batch processing using Airflow
Apache Airflow is a task orchestration tool that allows you to define a series of tasks that are executed in a specific order. Here we use Airflow to trigger the Spark job oncer per day. This includes extracting batch data from S3, transforming data using Spark and sending the Spark DataFrame to Cassandra.

## Batch processing: Monitoring
We used JMX exporter to connect Cassandra with Prometheus. A Grafana dashboard was created to monitor Cassandra's instance's attributes.

<img width="451" alt="Grafana_1" src="https://user-images.githubusercontent.com/104773240/177171445-a7c52c42-1dc0-44fa-b644-8d152cc79140.png">
<img width="465" alt="Grafana_2" src="https://user-images.githubusercontent.com/104773240/177171456-b0ed7fd8-4db2-4ff6-9166-1bd1c5435684.png">
<img width="451" alt="Grafana_3" src="https://user-images.githubusercontent.com/104773240/177171464-47845afd-3a0a-4d8b-829d-434f5701dd42.png">

## Stream processing: Kafka-Spark integration & Spark Streaming.
A Spark streaming DataFrame that reads from the previously created Kafka topic was constructed. Using Spark streaming, the raw data was cleaned, and different feature computations were applied.

## Stream processing: Storage
A local Postgres database with a database that matches the attributes of the processed streaming data was created. The streaming events are then sent out to this Postgres database.

## Stream processing: Monitoring
Prometheus was connected to the local Postgres database (using Postgres exporter) to monitor the data. Finally, using Grafana we created a dashboard which tracks the usage of the Postgres database.

<img width="495" alt="postgres_grafana_1" src="https://user-images.githubusercontent.com/104773240/178438999-962ea168-9c8f-4768-9e2a-cab8d01cddea.png">
<img width="495" alt="postgres_grafana_2" src="https://user-images.githubusercontent.com/104773240/178439028-a4388d2d-333a-4cc9-b04e-77080aea01f7.png">
<img width="507" alt="postgres_grafana_3" src="https://user-images.githubusercontent.com/104773240/178439049-cc10ff05-7795-40f0-bcb7-e51b5002d498.png">
<img width="509" alt="postgres_grafana_4" src="https://user-images.githubusercontent.com/104773240/178439062-b011f2ac-a16d-4706-aca3-b42535cf56d5.png">
