# Project: Snowflake Data Pipeline.

This project demonstrates the setup of a data pipeline using AWS services like S3, Lambda, Glue, and Snowflake for warehouse ingestion.

## Description

The project involves creating an AWS S3 bucket with specific folders for uploading, extracting, and transforming data. A Lambda function is then used to trigger the process of unzipping uploaded files, followed by executing an AWS Glue ETL job to convert the extracted data into Parquet format. Finally, the data is ingested into Snowflake for further analysis.

## Lambda Function

The Lambda function is responsible for unzipping files uploaded to the designated S3 bucket, moving them to an 'extract' folder, and triggering the AWS Glue job.

```python
# Lambda function code
import boto3
from zipfile import ZipFile
import os
import json
import uuid

# Lambda handler function
def lambda_handler(event, context):
    for record in event['Records']:
        file_name_with_directory = record['s3']['object']['key']
        file_name = record['s3']['object']['key'].split('/')[0]
        bucketName=record['s3']['bucket']['name']
        # Download the file from S3
        local_file_full_name='/tmp/{}'.format(file_name)
        s3 = boto3.client('s3')
        s3.download_file(bucketName, file_name_with_directory, local_file_full_name)

        # Unzip the file
        with ZipFile(local_file_full_name, 'r') as f:
            f.extractall('/tmp/unzip{}'.format(uuid.uuid4().hex))

        # Upload extracted files to S3
        file_names=''
        for filename in os.listdir('/tmp/unzip{}'.format(uuid.uuid4().hex)):
            f = os.path.join('/tmp/unzip{}'.format(uuid.uuid4().hex), filename)
            s3.upload_file(f, bucketName, 'extract/{}'.format(filename))
            os.remove(f)
            file_names=file_names+','+'s3://{}/extract/{}'.format(bucketName,filename)

        # Trigger AWS Glue job
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName='snowflakedemo', Arguments={"--VAL1":file_names[1:]})

        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
```

## AWS Glue ETL Job

The AWS Glue job converts the extracted files into Parquet format for ingestion into Snowflake.

```
# Glue job code
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.getOrCreate()

def main():
    args = getResolvedOptions(sys.argv, ["VAL1"])
    file_names=args['VAL1'].split(',')

    # Read CSV files and write to Parquet format
    df = spark.read.csv(file_names, header = True)
    df.repartition(1).write.mode('append').parquet("s3a://sftpusecasetestbjit/transform/")

main()

```

## Snowflake Database Setup

The Snowflake database, stage, and pipeline for warehouse ingestion are set up using SQL commands.

One access key and secret key are needed for accessing the S3 bucket folder to get Parquet files from the AWS S3 bucket into the Snowflake Database.

```
-- Snowflake SQL commands
-- Drop database if exists
drop database if exists s3_to_snowflake;

-- Database Creation
create database if not exists s3_to_snowflake;

-- Use the database
use s3_to_snowflake;

-- Table Creation
create or replace table s3_to_snowflake.PUBLIC.Iris_dataset1 (
    Id number(10,0),
    sepal_length number(10,4),
    sepal_width number(10,4),
    petal_length number(10,4),
    petal_width number(10,4),
    class varchar(200)
);

-- Create the file format
CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = parquet;

-- Create the external stage
create or replace stage s3_to_snowflake.PUBLIC.Snow_stage url="s3://sftpusecasetestbjit/transform/"
credentials=(aws_key_id='<YOUR_AWS_KEY_ID>', aws_secret_key='<YOUR_AWS_SECRET_KEY>')
file_format = sf_tut_parquet_format;

-- Create the Pipe
create or replace pipe s3_to_snowflake.PUBLIC.for_iris_one auto_ingest=true as copy into s3_to_snowflake.PUBLIC.Iris_dataset1
from (select $1:"Id"::number, $1:SEPAL_LENGTH::VARCHAR, $1:SEPAL_WIDTH::VARCHAR, $1:PETAL_LENGTH::VARCHAR, $1:PETAL_WIDTH::VARCHAR, $1:CLASS_NAME::VARCHAR
from @s3_to_snowflake.PUBLIC.Snow_stage);

```

## This README.md file provides comprehensive instructions and explanations for setting up the project and utilizing the AWS services effectively. Adjustments can be made according to your specific project requirements and preferences.