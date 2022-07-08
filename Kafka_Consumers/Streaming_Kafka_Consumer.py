import os
import pyspark.sql.functions as F
import pyspark.streaming
from kafka import KafkaConsumer
from json import loads
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace, max
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.postgresql:postgresql:42.2.10 pyspark-shell'

# Create Consumer to retrieve the messages from the topic
#stream_pin_consumer = KafkaConsumer(
 #   bootstrap_servers = "localhost:9092",
  #  value_deserializer = lambda pinmessage: loads(pinmessage),
    #ensures messages are read from the beggining
   # auto_offset_reset = "earliest"
#)

#stream_pin_consumer.subscribe(topics = "KafkaPinterestTopic")
#for message in stream_pin_consumer:
#    print(message)

# specify the topic we want to stream data from.
kafka_topic_name = "KafkaPinterestTopic"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()


# Select the value part of the kafka message and cast it to a string.
#stream_df = stream_df.selectExpr("CAST(value as STRING)")
stream_df = stream_df.selectExpr("CAST(value as STRING)")

jsonSchema = StructType([StructField("index", IntegerType()),
                        StructField("unique_id", StringType()),
                        StructField("title", StringType()),
                        StructField("description", StringType()),
                        #StructField("poster_name", StringType()),
                        StructField("follower_count", StringType()),
                        StructField("tag_list", StringType()),
                        StructField("poster_name", StringType()),
                        StructField("is_image_or_video", StringType()),
                        StructField("image_src", StringType()),
                        StructField("downloaded", IntegerType()),
                        StructField("save_location", StringType()),
                        StructField("category", StringType()),
                        ])

# convert JSON column to multiple columns
stream_df = stream_df.withColumn("value", F.from_json(stream_df["value"], jsonSchema)).select(col("value.*"))

# replace empty cells with Nones
stream_df = stream_df.replace({'User Info Error': None}, subset = ['follower_count']) \
                     .replace({"No Title Data Available": None}, subset = ['title']) \
                     .replace({'No description available Story format': None}, subset = ['description']) \
                     .replace({'Image src error.': None}, subset = ['image_src'])        

# transforms follower_count column into int type
stream_df = stream_df.withColumn("follower_count", when(col('follower_count').like("%k"), regexp_replace('follower_count', 'k', '000')) \
                     .when(col('follower_count').like("%M"), regexp_replace('follower_count', 'M', '000000'))\
                     .when(col('follower_count').like("%B"), regexp_replace('follower_count', 'B', '000000000')) \
                     .cast("int")) 

# removes 'Save data in' part of the string -> only path left now
stream_df = stream_df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) 

# changes data type to int  
stream_df= stream_df.withColumn("downloaded", stream_df["downloaded"].cast("int")) \
                    .withColumn("index", stream_df["index"].cast("int"))
                    

# replace empty values with null for all columns
stream_df=stream_df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in stream_df.columns])

# renames column for Cassandra table
stream_df = stream_df.withColumnRenamed("index", "ind")

# reorders columns
stream_df =  stream_df.select('ind',
                      'unique_id',
                      'title',
                      'description',
                      'follower_count',
                      'tag_list',
                      'is_image_or_video',
                      'image_src',
                      'downloaded',
                      'save_location',
                      'category'
                      )

# feature computation transformations
feature_df = stream_df.groupBy(stream_df.category).count()  
feature_df_2 = stream_df.select(F.max("follower_count"))


def write_streaming_to_postgres(df, epoch_id):

    df.write \
        .mode('append') \
        .format('jdbc') \
        .option('url', f'jdbc:postgresql://localhost:5432/Streaming_Data') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'spark_streaming') \
        .option('user', os.environ["RDS_USER"]) \
        .option('password', os.environ["PGADMIN_PASSWORD"]) \
        .save()
                
# outputting the messages to the console 
#stream_df.writeStream \
 #   .format("console") \
  #  .outputMode("update") \
   # .start() \
    #.awaitTermination() 

# writing to postgres
stream_df.writeStream \
    .foreachBatch(write_streaming_to_postgres) \
    .start() \
    .awaitTermination() 

#spark.stop()


