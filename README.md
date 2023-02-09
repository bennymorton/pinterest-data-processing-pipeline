# Pinterest data processing pipeline

## Summary
An end-to-end data processing pipeline inspired by Pinterest’s experiment processing pipeline.<br>
The pipeline is developed using a Lambda architecture.<br>
The batch data is ingested using a FastAPI and Kafka and then stored in an AWS S3 bucket, which is then read from the S3 bucket and wrangled using Apache Spark. The streaming data is read in real-time from Kafka using Spark Streaming and stored in a PostgreSQL database for analysis and long term storage.

### Pipeline architecture


## Steps
1. 
