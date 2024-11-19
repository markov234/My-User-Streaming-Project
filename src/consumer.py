
# fix issue with kafka-python library that hasn't been updated to support Python 3.12 (since about November 2023)
import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp, from_json, col
import psycopg2
from dotenv import load_dotenv
import logging

load_dotenv()  # load environment variables from .env file (for sensitive data)

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
load_dotenv()  # load environment variables from .env file (for sensitive data)

def save_to_postgres(df, batch_id):
    try:
        rows = df.toPandas()
        # print(rows['id'].tolist())  # Log to check the actual values being written to Postgres.
        print([tuple(row) for row in rows.itertuples(index=False)])
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO random_users (id, first_name, last_name, gender, 
            address, city, state, country, postcode, email, age, 
            phone, picture) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s)
        """, [tuple(row) for row in rows.itertuples(index=False)] ) # list of tuples.
        # executemany() needs a sequence of sequences. Could also be list of lists I think.
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error in batch {batch_id}: {str(e)}", exc_info=True)

def run_spark_consumer():
    # Update spark-sql-kafka connector as needed for compatibility with Spark and Scala versions.
    # Can set .master("local[*]") to use all available cores. 
    # This means that the Spark session will run locally with as many 
    # worker threads as logical cores on your machine. Faster processing.
    spark = SparkSession.builder \
        .appName("SparkConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # When I add dob and registered_date, I think those should be StringType here.
    # That is because this schema is what the data will look like as it comes out of Kafka broker,
    # and according to randomuser.me, those fields are strings.
    schema = StructType() \
        .add("id", StringType(), True) \
        .add("first_name", StringType(), True) \
        .add("last_name", StringType(), True) \
        .add("gender", StringType(), True) \
        .add("address", StringType(), True) \
        .add("city", StringType(), True) \
        .add("state", StringType(), True) \
        .add("country", StringType(), True) \
        .add("postcode", IntegerType(), True) \
        .add("email", StringType(), True) \
        .add("age", IntegerType(), True) \
        .add("phone", StringType(), True) \
        .add("picture", StringType(), True)

    topic = 'randomusers'
    print("Consuming messages from topic", topic)

    # This code reads messages from the Kafka topic, parses them as json, 
    # and extracts the values into a structured dataframe whose columns 
    # were defined above in the schema.

    # stream_df is a -streaming dataframe- that reads from Kafka topic.
    # It represents a logical plan to read data from the Kafka topic.
    # It's not a streaming query yet, so it doesn't need .awaitTermination().
    # Reads all data from beginning of topic.
    stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    # No data printed to console here. Need to figure out timing with producer 
    # active and consumer active at same or different times.
    # Has to do with .option("startingOffsets", "earliest") line.
    # stream = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("subscribe", topic) \
    #     .load()
    
    print(stream_df)
    # DataFrame[key: binary, value: binary, topic: string, partition: int, 
    # offset: bigint, timestamp: timestamp, timestampType: int]

    # value_df is just a transformed dataframe, not a streaming query,
    # so should be helpful if needing to debug. And does not need .awaitTermination().
    # Can be used to see what the data looks like.
    # Ex:
    # +--------------------+
    # print to console for testing.
    # value_df = stream_df.select(col("value").cast("string")) \
    #     .withColumn("data", from_json(col("value"), schema)) \
    #     .select("data.*") \
    #     .writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .start()
    # value_df.awaitTermination(30) # seconds.
    value_df = stream_df.select(col("value").cast("string")) \
        .withColumn("data", from_json(col("value"), schema)) \
        .select("data.*") \
        .dropDuplicates(["id"]) # in case id is duplicate. But for randomuser.me, id is unique.
    
    # write_df is -streaming query- that writes to Postgres.
    # So it is what needs to be terminated with awaitTermination().
    write_df = value_df.writeStream \
        .foreachBatch(save_to_postgres) \
        .start()

    write_df.awaitTermination() # seconds. 
    # Should be large enough to allow time for connection to topic to be 
    # established and for any messages to be consumed at all. Noted after realizing 
    # that 5 seconds was not long enough. Roughly 30 seconds or more may be enough.
    # If topic has many messages, increase this time.
    # Or set it to None to wait indefinitely, and then cancel with Cntrl+C.
    print("Consumer finished")


if __name__ == '__main__':
    run_spark_consumer()

