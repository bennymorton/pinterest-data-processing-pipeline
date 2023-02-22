# Pinterest data processing pipeline

## Summary
An end-to-end data processing pipeline inspired by Pinterest’s experiment processing pipeline.<br>
The pipeline is developed using a Lambda architecture.<br>
The batch data is ingested using a FastAPI and Kafka and then stored in an AWS S3 bucket, which is then read from the S3 bucket and wrangled using Apache Spark. The streaming data is read in real-time from Kafka using Spark Streaming and stored in a PostgreSQL database for analysis and long term storage.

### Pipeline architecture
![Pipeline architecture diagram](/pipeline_arch.png)

## Development steps
1. Set up virtual environment and install all required packages and libraries.
2. Configure API to listen to user emulation script
3. Batch data ingestion. Adjust API to post data to a Kafka Producer, after creating a topic.
4. Build a Kafka consumer to receive the data 
5. Output the data from the consumer into an AWS S3 bucket
6. Batch process the data using Spark, which consists of the following cleaning transformations:
    - Stripping unnecessary characters from columns that will be cast as integer
    - Remove null rows
    - Casting all columns to correct datatypes
7. Orchestrate this process using Airflow
8. Integrate Kafka and Spark ready for the streaming layer
9. Carry out the same cleaning jobs on the streaming layer, using Spark streaming
10. Send the streaming data to long term storage in a Postgres database

## Recent developments
**Commit update 21/02/23**
- logical renaming of variables and functions in streaming_consumer 
- abstraction of cleaning methods
- added docstrings for each method
