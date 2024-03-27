

# Snowflake-warehouse-with-s3-lambda-glue

# Create AWS S3 bucket with 3 folder name upload, extract and transform
# Create Lambda function for Python3 with bellow code

==================================
import boto3
from zipfile import ZipFile

import os
import json
import uuid
myuuid = str(uuid.uuid4()).replace('-','')
glue_job_name='snowflakedemo'
def lambda_handler(event, context):
    # TODO implement
    for record in event['Records']:
        file_name_with_directory = record['s3']['object']['key']
        file_name = record['s3']['object']['key'].split('/')[0]
        bucketName=record['s3']['bucket']['name']
        print("File Name : ",file_name)
        print("File Name with directory: ",file_name_with_directory)
        print("Bucket Name : ",bucketName)
        local_file_full_name='/tmp/{}'.format(file_name)
        s3 = boto3.client('s3')
        s3.download_file(bucketName, file_name_with_directory, local_file_full_name)
        print("File downloaded successfully")

        with ZipFile(local_file_full_name, 'r') as f:
            #extract in current directory
            f.extractall('/tmp/unzip{}'.format(myuuid))
        file_names=''
        for filename in os.listdir('/tmp/unzip{}'.format(myuuid)):
            f = os.path.join('/tmp/unzip{}'.format(myuuid), filename)
            print("File Name : ",f)
            s3.upload_file(f, bucketName, extract/{}'.format(filename))
            os.remove(f)
            file_names=file_names+','+'s3://{}/extract/{}'.format(bucketName,filename)
        print("Files after unzip :", file_names)
        glue=boto3.client('glue');
        response = glue.start_job_run(JobName = glue_job_name, Arguments={"--VAL1":file_names[1:]})
        print("Glue Job trigger response : ",response)
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }

=============== Lambda code end===================

# ***NB: This Lambda Script will be triggered when you upload a zip file and on upload folder and then unzip it and put into the extract folder and trigger the AWS Glue job.

# Add Lambda trigger with the S3 bucket of BuektName/upload/ for All object create events
# Create an AWS Glue ETL job with the name 'snowflakedemo' and add these script below.


==============================
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


def main():
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ["VAL1"])
    file_names=args['VAL1'].split(',')
    df = spark.read.csv(file_names, header = True)
    df.repartition(1).write.mode('append').parquet("s3a://sftpusecasetestbjit/transform/")

main()
===============================
# ***NB: this AWS Glue job gets extracted files and converts it to Parquet files and triggers the Snowflake Pipeline.

# Now set up Snowflake Database, Stage and Pipeline for Data Warehouse Ingestion. Execute bellow codes one by one

Snowflake Worksheet configurations =================================

--Drop database if exists
drop database if exists s3_to_snowflake;

--Database Creation
create database if not exists s3_to_snowflake;

--Use the database
use s3_to_snowflake;

--Table or Scheema Creation
--create or replace table s3_to_snowflake.PUBLIC.Iris_dataset1 (sepal_length number(10,4), sepal_width number(10,4), petal_length number(10,4), petal_width number(10,4), class varchar(200));

create or replace table s3_to_snowflake.PUBLIC.Iris_dataset1 (Id number(10,0),sepal_length number(10,4),sepal_width number(10,4),petal_length number(10,4)  ,petal_width number(10,4),class varchar(200));

--Create the file format
CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = parquet;

--Create the external stage
create or replace stage s3_to_snowflake.PUBLIC.Snow_stage url="s3://sftpusecasetestbjit/transform/"
credentials=(aws_key_id=''
aws_secret_key='')
file_format = sf_tut_parquet_format;

list @Snow_stage;

--Create the Pipe
create or replace pipe s3_to_snowflake.PUBLIC.for_iris_one auto_ingest=true as copy into s3_to_snowflake.PUBLIC.Iris_dataset1
from
(select $1:"Id"::number,
$1:SEPAL_LENGTH::VARCHAR,
$1:SEPAL_WIDTH::VARCHAR,
$1:PETAL_LENGTH::VARCHAR,
$1:PETAL_WIDTH::VARCHAR,
$1:CLASS_NAME::VARCHAR
from @s3_to_snowflake.PUBLIC.Snow_stage);

show pipes;

select * from s3_to_snowflake.PUBLIC.Iris_dataset1;

=========================================
# ***NB: One access key and secret key needed for accessing the S3 bucket folder to get Parquet file from AWS S3 bucket into Snowflake Database.
