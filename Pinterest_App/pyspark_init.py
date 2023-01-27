import multiprocessing
import pyspark
import os

if __name__ == "__main__":
    cfg = (
        pyspark.SparkConf()
        # Setting the master to run locally and with the maximum amount of cpu coresfor multiprocessing.
        .setMaster(f"local[{multiprocessing.cpu_count()}]")
        # Setting application name
        .setAppName("TestApp")
        # Setting config value via string
        .set("spark.eventLog.enabled", False)
        # Setting environment variables for executors to use
        .setExecutorEnv(pairs=[("VAR3", "value3"), ("VAR4", "value4")])
        # Setting memory if this setting was not set previously
        .setIfMissing("spark.executor.memory", "1g")
    )

    session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

    directory = '/Users/benmorton/Downloads/DatasetToCompleteTheSixSparkExercises/products_parquet'
    session.sql("""CREATE TABLE all_products (
            product_id string,
            product_name string,
            price string
            );""")
    for file in os.listdir(directory):
        if file.endswith(".parquet"):
            filepath = os.path.join(directory, file)
            df = session.read.parquet(filepath)
            df.createOrReplaceTempView('temp_view')
            session.sql('INSERT INTO all_products SELECT * FROM temp_view;')
    all_products = session.sql('SELECT * FROM all_products;')
    print(all_products.show())

    # parquetFileDF2 = session.read.parquet("part-00001-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet")
    # parquetFileDF2.createOrReplaceTempView('parquet_temp_view2')
    # parqSQL2 = session.sql('SELECT * FROM parquet_temp_view2;')

    # print(parqSQL2.show())