# Spark streaming script to:
#   parse kafka streaming messages
#   clean data
#   display rolling aggregations 

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

kafka_topic_name = 'pinterest-topic'
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
    .builder \
    .appName('Kafka') \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

stream_df = spark \
    .readStream \
    .format('Kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', kafka_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load()

# unpack value info from kafka message 
stream_df = stream_df.selectExpr('CAST(value as STRING)')

df_schema = StructType(
    [StructField('index', StringType()),
    StructField('unique_id', StringType()), 
    StructField('title', StringType()),
    StructField('description', StringType()),
    StructField('poster_name', StringType()),
    StructField('follower_count', StringType()),
    StructField('tag_list', StringType()),
    StructField('is_image_or_video', StringType()),
    StructField('image_src', StringType()),
    StructField('downloaded', StringType()),
    StructField('save_location', StringType()),
    StructField('category', StringType())])

# unpack json data into organised dataframe, using the above schema
stream_df = stream_df \
    .withColumn('value',from_json(stream_df.value, df_schema).alias('value')) \
    .select('value.*')

# CLEANING FUNCTIONS
def save_location_strip(stream_df: DataFrame):
    stream_df = stream_df.withColumn('save_location', expr("ltrim('Local save in ', ltrim(save_location))"))
    return stream_df

def boolean_downloaded(stream_df: DataFrame):
    stream_df = stream_df.withColumn('downloaded',col('downloaded').cast(BooleanType()))
    return stream_df

def integer_followercount(stream_df: DataFrame):
    stream_df = stream_df \
        .withColumn('follower_count_unit', 
            when(stream_df.follower_count.contains('k'), 'k')
            .when(stream_df.follower_count.contains('M'), 'm')
            .when(stream_df.follower_count.contains('B'), 'b')
            .otherwise('n')
        )

    stream_df = stream_df \
        .withColumn("follower_count", expr("rtrim('k', rtrim(follower_count))")) \
        .withColumn("follower_count", expr("rtrim('B', rtrim(follower_count))")) \
        .withColumn("follower_count", expr("rtrim('M', rtrim(follower_count))"))

    stream_df = stream_df \
        .withColumn('follower_count',
            when(stream_df.follower_count_unit == 'k', (stream_df.follower_count*1000)) 
            .when(stream_df.follower_count_unit == 'm', (stream_df.follower_count*1000000)) 
            .when(stream_df.follower_count_unit == 'b', (stream_df.follower_count*1000000000)) 
            .otherwise(stream_df.follower_count)
        ).withColumn("follower_count",col('follower_count').cast(IntegerType()))

    stream_df = stream_df.drop(stream_df.follower_count_unit)
    return stream_df

def integer_index(stream_df: DataFrame):
    stream_df = stream_df \
        .withColumn("index",col('index').cast(IntegerType()))
    return stream_df

def tag_list_nones(stream_df: DataFrame):
    stream_df = stream_df \
        .withColumn('tag_list',when(stream_df.tag_list == 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None) \
        .otherwise(stream_df.tag_list))
    return stream_df

def remove_null_rows_and_columns(stream_df: DataFrame):
    stream_df = stream_df.na.drop(subset='tag_list')
    stream_df = stream_df.drop(stream_df.poster_name)
    return stream_df

# WINDOW AGGREGATIONS
# 1. sum of followersl in each category in the last x minutes. percentage?
# 2. number of posts per category
aggregation = stream_df \
    .groupBy(window("category", "10 second")) \
    .count()

def cleaning_and_writing(stream_df: DataFrame, batch_id):
    # calling of cleaning functions
    stream_df = save_location_strip(stream_df)
    stream_df = boolean_downloaded(stream_df)
    stream_df = integer_followercount(stream_df)
    stream_df = integer_index(stream_df)
    stream_df = tag_list_nones(stream_df)
    stream_df = remove_null_rows_and_columns(stream_df)
    print(aggregation.show())
    # write to postgres
    stream_df.write \
        .mode('append') \
        .format('jdbc') \
        .option("url", 'jdbc:postgresql://localhost:5432/pinterest_streaming') \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "experimental_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .save()

stream_df \
    .writeStream \
    .foreachBatch(cleaning_and_writing) \
    .outputMode('append') \
    .start() \
    .awaitTermination()