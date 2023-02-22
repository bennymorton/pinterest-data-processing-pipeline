''' 
Spark streaming programme to:
    - parse kafka streaming messages
    - clean data
    - display rolling aggregations
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.2.10 pyspark-shell'

spark = SparkSession \
    .builder \
    .appName('Kafka') \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_topic_name = 'pinterest-topic'
kafka_bootstrap_servers = 'localhost:9092'

df = spark \
    .readStream \
    .format('Kafka') \
    .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
    .option('subscribe', kafka_topic_name) \
    .option('startingOffsets', 'earliest') \
    .load()

# unpack value info from kafka message 
df = df.selectExpr('CAST(value as STRING)')

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
    StructField('category', StringType())]
    )

# unpack json data into organised dataframe, using the above schema
df = df \
    .withColumn('value',from_json(df.value, df_schema).alias('value')) \
    .select('value.*')

class Clean:
    # def __init__(self, df):
    #     self.df = df

    def cast_column(self, df, column, datatype):
        self.df = df
        self.column = column
        self.datatype = datatype
        self.df = self.df.withColumn(column,col(column).cast(datatype))
        return self.df

    def strip_save_location(self, df):
        '''Remove the unnecessary characters from the 'save_location' column'''
        self.df = df
        self.df = self.df.withColumn('save_location', expr("ltrim('Local save in ', ltrim(save_location))"))
        return self.df

    def regulate_follower_count(self, df):
        '''Regulate the different units of the column 'follower_count',
            then cast to integer'''
        self.df = df

        # create new column 'follower_count_unit' and fill it with whatever unit 'follower_count' is
        self.df = self.df \
            .withColumn('follower_count_unit', 
                when(self.df.follower_count.contains('k'), 'k')
                .when(self.df.follower_count.contains('M'), 'm')
                .when(self.df.follower_count.contains('B'), 'b')
                .otherwise('n')
            )
        
        # trim the unit letter off the 'follower_count'
        self.df = self.df \
            .withColumn("follower_count", expr("rtrim('k', rtrim(follower_count))")) \
            .withColumn("follower_count", expr("rtrim('B', rtrim(follower_count))")) \
            .withColumn("follower_count", expr("rtrim('M', rtrim(follower_count))"))

        # convert 'follower_count' to correct number of followers based on unit column
        self.df = self.df \
            .withColumn('follower_count',
                when(self.df.follower_count_unit == 'k', (self.df.follower_count*1000)) 
                .when(self.df.follower_count_unit == 'm', (self.df.follower_count*1000000)) 
                .when(self.df.follower_count_unit == 'b', (self.df.follower_count*1000000000)) 
                .otherwise(self.df.follower_count)
            )
        
        self.df = self.df.drop(self.df.follower_count_unit)
        return self.df

    def tag_list_nones(self, df):
        '''Remove invalid data from 'tag_list' column'''
        self.df = df
        self.df = self.df \
            .withColumn('tag_list',when(self.df.tag_list == 'N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e', None) \
            .otherwise(self.df.tag_list))
        return self.df

    def remove_null_rows_and_columns(self, df):
        self.df = df
        self.df = self.df.na.drop(subset='tag_list')
        self.df = self.df.drop(self.df.poster_name)
        return self.df

# WINDOW AGGREGATIONS
# 1. sum of followersl in each category in the last x minutes. percentage?
# 2. number of posts per category

def foreach_func(df, batch_id):
    '''Function called in the writestream foreachBatch to clean then send data to postgres sink'''
    clean = Clean()
    df = clean.strip_save_location(df)
    df = clean.regulate_follower_count(df)
    df = clean.cast_column(df, 'follower_count', IntegerType()) 
    df = clean.cast_column(df, 'index', IntegerType()) 
    df = clean.cast_column(df, 'downloaded', BooleanType()) 
    df = clean.tag_list_nones(df)
    df = clean.remove_null_rows_and_columns(df)

    df.write \
        .mode('append') \
        .format('jdbc') \
        .option("url", 'jdbc:postgresql://localhost:5432/pinterest_streaming') \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "experimental_data") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .save()
    
df.writeStream \
    .foreachBatch(foreach_func) \
    .outputMode('append') \
    .start() \
    .awaitTermination()