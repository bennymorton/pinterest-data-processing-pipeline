# pinterest-data-processing-pipeline

an end-to-end data processing pipeline inspired by Pinterest’s experiment processing pipeline The pipeline is developed using a Lambda architecture.  The batch data is ingested using a FastAPI and Kafka and then stored in an AWS S3 bucket. The batch data is then read from the S3 bucket and processed using Apache Spark. The streaming data is read in real-time from Kafka using Spark Streaming and stored in a PostgreSQL database for analysis and long term storage.
