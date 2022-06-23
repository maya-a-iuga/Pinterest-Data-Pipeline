
import findspark
import os
findspark.init(os.environ["SPARK_HOME"])
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when, col, regexp_replace, split

# Creating our Spark configuration
conf = SparkConf() \
    .setAppName('S3toSpark') \
    .setMaster('local[*]') #setting master to run locally

sc=SparkContext(conf=conf)

# Configure the setting to read from the S3 bucket
accessKeyId = os.environ["AWS_ACCESS_KEY"]
secretAccessKey = os.environ["AWS_SECRET_ACCESS_KEY"]
hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

# Create our Spark session
spark=SparkSession(sc)

# Read jsons from S3 bucket
df = spark.read.json("s3a://pinterestpipelinebucket/events/*.json") 

# Code to clean out the data
# transforms K and M into corresponding 0s & changes data type from string to integer
df = df.withColumn("follower_count", when(col('follower_count').like("%k"), (regexp_replace('follower_count', 'k', '').cast('int')*1000)) \
    .when(col('follower_count').like("%M"), (regexp_replace('follower_count', 'M', '').cast('int')*1000000))\
    .when(col('follower_count').like("%B"), (regexp_replace('follower_count', 'B', '').cast('int')*1000000000))) \
    .fillna({'follower_count' : '0'}) 
# changes data type from longint to int  
df= df.withColumn("downloaded", df["downloaded"].cast("int")) 
df= df.withColumn("index", df["index"].cast("int"))
# removes 'Save data in' part of the string -> only path left now
df = df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', ''))
# renames column for Cassandra table
df = df.withColumnRenamed("index", "ind")

# send data from Spark to Cassandra
df.write \
  .format("org.apache.spark.sql.cassandra") \
  .mode("overwrite") \
  .option("confirm.truncate", "true") \
  .option("spark.cassandra.connection.host", "127.0.0.1") \
  .option("spark.cassandra.connection.port", "9042") \
  .option("keyspace" , "sparkdb") \
  .option("table", "pin") \
  .save()
