"""
Bronze to Silver Lambda

Purpose:
    Convert raw kaggle CSV files from Bronze S3
    into parquet files in Silver S3.
"""

import os
import urllib.parse

import boto3
import pandas as pd

s3_client = boto3.client("s3")

SILVER_BUCKET = os.environ["S3_SILVER_BUCKET"]
SILVER_PREFIX = os.environ["SILVER_PREFIX"]

def lambda_handler(event, context):
    record = event["Records"][0]

    source_bucket = record["s3"]["bucket"]["name"]
    source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

    filename = source_key.split("/")[-1]
    output_filename = filename.replace(".csv", ".parquet")

    input_path = "/tmp/input.csv"
    output_path = f"/tmp/{output_filename}"

    s3_client.download_file(
        Bucket=source_bucket,
        Key=source_key,
        Filename=input_path,
    )

    df = pd.read_csv(input_path)

    df.to_parquet(
        output_path,
        index=False,
    )

    silver_key = source_key.replace(
        "raw_kaggle_statistics",
        SILVER_PREFIX,
    ).replace(
        ".csv",
        ".parquet",
    )

    s3_client.upload_file(
        Filename=output_path,
        Bucket=SILVER_BUCKET,
        Key=silver_key,
    )

    return {
        "statusCode": 200,
        "source_key": source_key,
        "silver_key": silver_key,
        "record_count": len(df),
    }