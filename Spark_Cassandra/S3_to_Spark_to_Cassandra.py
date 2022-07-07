
import findspark
import os
findspark.init(os.environ["SPARK_HOME"])
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import when, col, regexp_replace, split

os.environ['PYSPARK_SUBMIT_ARGS']='--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 pyspark-shell'


class Spark_DAG():

    def __init__(self):

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
        self.spark=SparkSession(sc)


    def read_from_s3(self):
        # Read jsons from S3 bucket
        self.df = self.spark.read.json("s3a://pinterestpipelinebucket/events/*.json")
        

    def transform_spark_data(self):

        # transforms follower_count column into int type
        self.df = self.df.withColumn("follower_count", when(col('follower_count').like("%k"), regexp_replace('follower_count', 'k', '000')) \
                            .when(col('follower_count').like("%M"), regexp_replace('follower_count', 'M', '000000'))\
                            .when(col('follower_count').like("%B"), regexp_replace('follower_count', 'B', '000000000')) \
                            .cast("int")) 

        # removes 'Save data in' part of the string -> only path left now
        self.df = self.df.withColumn('save_location', regexp_replace('save_location', 'Local save in ', '')) 

        # changes data type to int  
        self.df= self.df.withColumn("downloaded", self.df["downloaded"].cast("int")) \
                            .withColumn("index", self.df["index"].cast("int")) 

        # replace empty values with null for all columns
        self.df=self.df.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in self.df.columns])

        # renames column for Cassandra table
        self.df = self.df.withColumnRenamed("index", "ind")
        
        # reorders columns
        self.df = self.df.select('ind',
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

    def write_to_cassandra(self):

        # send data from Spark to Cassandra
        self.df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("overwrite") \
        .option("confirm.truncate", "true") \
        .option("spark.cassandra.connection.host", "127.0.0.1") \
        .option("spark.cassandra.connection.port", "9042") \
        .option("keyspace" , "sparkdb") \
        .option("table", "pin") \
        .save()


    def run_spark_session(self):
        self.read_from_s3()
        self.transform_spark_data()
        self.write_to_cassandra()

if __name__ == "__main__":

    spark_session = Spark_DAG()
    spark_session.run_spark_session()