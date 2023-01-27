import pyspark
import pyspark.sql
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

def spark_batch_process():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazonaws:aws-java-sdk-s3:1.12.196,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

    conf = (
        pyspark.SparkConf()
        .setAppName('S3toSpark')
        .setMaster('local[*]')
    )
    sc = pyspark.SparkContext(conf=conf)

    accessKeyId="AKIA2N4R23AGOU6IEFU7"
    secretAccessKey="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"
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set('fs.s3a.access.key', accessKeyId)
    hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
    hadoopConf.set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') # Allows the package to authenticate with AWS

    spark=pyspark.sql.SparkSession(sc)

    df = spark.read.json('s3a://pinterest-data-d255ea73-811a-4331-98f6-7692459e2620/*.json')

    # super in depth explanation of caching: https://livebook.manning.com/book/spark-in-action-with-examples-in-java/16-cache-and-checkpoint-enhancing-spark-s-performances/v-14/28

    # CLEANING
    # 1. cast 'downloaded' to boolean
    df = df.withColumn("downloaded",col('downloaded').cast(BooleanType()))

    # 2. Strip characters, then cast 'follower_count' to int
    df = (
        df.withColumn('follower_count_unit', 
            when(df.follower_count.contains('k'), 'k')
            .when(df.follower_count.contains('M'), 'm')
            .when(df.follower_count.contains('B'), 'b')
            .otherwise('n'))
    )
    df = (
        df.withColumn("follower_count", expr("rtrim('k', rtrim(follower_count))"))
        .withColumn("follower_count", expr("rtrim('B', rtrim(follower_count))"))
        .withColumn("follower_count", expr("rtrim('M', rtrim(follower_count))"))
    )
    df = (
        df.withColumn('follower_count',
            when(df.follower_count_unit == 'k', (df.follower_count*1000))
            .when(df.follower_count_unit == 'm', (df.follower_count*1000000))
            .when(df.follower_count_unit == 'b', (df.follower_count*1000000000))
            .otherwise(df.follower_count)
        )
        .withColumn("follower_count",col('follower_count').cast(IntegerType()))
    )
    df = df.drop(df.follower_count_unit)

    # 3. cast 'index' as integer
    df = df.withColumn("index",col('index').cast(IntegerType()))

    # 5. remove unnessential characters from 'save_location'
    df = df.withColumn("save_location", expr("ltrim('Local save in ', ltrim(save_location))"))

    print(df.show())