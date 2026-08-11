"""
Bronze to Silver Reference Lambda

Purpose:
    Transform Kaggle category reference JSON files
    from Bronze into standardized Parquet files in Silver.
"""

import json
import os
import re
import urllib.parse
from datetime import datetime, timezone

import boto3
import pandas as pd


s3_client = boto3.client("s3")


# Configuration

BRONZE_BUCKET = os.environ["S3_BRONZE_BUCKET"]
SILVER_BUCKET = os.environ["S3_SILVER_BUCKET"]

KAGGLE_SOURCE_PREFIX = os.environ.get(
    "KAGGLE_REFERENCE_SOURCE_PREFIX",
    "youtube/raw_kaggle_reference_data/",
)

KAGGLE_SILVER_PREFIX = os.environ.get(
    "SILVER_KAGGLE_REFERENCE_PREFIX",
    "silver/kaggle_reference_data/",
)


# Lambda Handler

def lambda_handler(event, context):

    objects = get_objects_to_process(event)

    success = []
    failed = []

    for item in objects:

        bucket = item["bucket"]
        key = item["key"]

        try:

            result = transform_reference_file(
                bucket,
                key,
            )

            success.append(result)

        except Exception as error:

            failed.append(
                {
                    "bucket": bucket,
                    "key": key,
                    "error": str(error),
                }
            )

    return {
        "statusCode": (
            200
            if not failed
            else 207
        ),
        "success": success,
        "failed": failed,
    }


# Get Objects

def get_objects_to_process(event):

    records = event.get(
        "Records",
        [],
    )

    if records:

        objects = []

        for record in records:

            bucket = (
                record["s3"]
                ["bucket"]
                ["name"]
            )

            key = urllib.parse.unquote_plus(
                record["s3"]
                ["object"]
                ["key"]
            )

            if key.startswith(
                KAGGLE_SOURCE_PREFIX
            ):

                objects.append(
                    {
                        "bucket": bucket,
                        "key": key,
                    }
                )

        return objects

    return list_reference_objects()


# List Reference Objects

def list_reference_objects():

    objects = []

    paginator = (
        s3_client
        .get_paginator(
            "list_objects_v2"
        )
    )

    pages = paginator.paginate(
        Bucket=BRONZE_BUCKET,
        Prefix=KAGGLE_SOURCE_PREFIX,
    )

    for page in pages:

        for item in page.get(
            "Contents",
            [],
        ):

            key = item["Key"]

            if not key.lower().endswith(
                ".json"
            ):
                continue

            objects.append(
                {
                    "bucket": BRONZE_BUCKET,
                    "key": key,
                }
            )

    return objects


# Transform Reference File

def transform_reference_file(
    bucket,
    key,
):

    response = (
        s3_client
        .get_object(
            Bucket=bucket,
            Key=key,
        )
    )

    raw_content = (
        response["Body"]
        .read()
        .decode("utf-8")
    )

    payload = json.loads(
        raw_content
    )

    items = extract_category_items(
        payload
    )

    region = extract_region(
        key,
        payload,
    )

    if not region:

        raise ValueError(
            f"Unable to determine region from {key}"
        )

    records = []

    processed_at = (
        datetime.now(
            timezone.utc
        )
        .isoformat()
    )

    for item in items:

        snippet = item.get(
            "snippet",
            {},
        )

        records.append(
            {
                "category_id": item.get(
                    "id"
                ),
                "category_title": snippet.get(
                    "title"
                ),
                "assignable": snippet.get(
                    "assignable"
                ),
                "channel_id": snippet.get(
                    "channelId"
                ),
                "source": "kaggle",
                "_source_key": key,
                "_processed_at": processed_at,
            }
        )

    if not records:

        raise ValueError(
            f"No category records found in {key}"
        )

    df = pd.DataFrame(
        records
    )

    df["category_id"] = pd.to_numeric(
        df["category_id"],
        errors="coerce",
    ).astype(
        "Int64"
    )

    df = df.dropna(
        subset=[
            "category_id",
            "category_title",
        ]
    )
    df = df.drop_duplicates(
        subset=[
            "category_id",
        ]
    )

    output_key = build_silver_key(
        key
    )

    output_path = (
        "/tmp/reference.parquet"
    )

    df.to_parquet(
        output_path,
        index=False,
    )

    s3_client.upload_file(
        output_path,
        SILVER_BUCKET,
        output_key,
    )

    return {
        "source_bucket": bucket,
        "source_key": key,
        "silver_bucket": SILVER_BUCKET,
        "silver_key": output_key,
        "region": region.lower(),
        "records": len(df),
    }


# Extract Category Items

def extract_category_items(
    payload,
):

    if isinstance(
        payload,
        list,
    ):
        return payload

    if isinstance(
        payload,
        dict,
    ):

        if isinstance(
            payload.get("items"),
            list,
        ):
            return payload["items"]

        data = payload.get(
            "data"
        )

        if (
            isinstance(data, dict)
            and isinstance(
                data.get("items"),
                list,
            )
        ):
            return data["items"]

    raise ValueError(
        "Unsupported reference JSON structure"
    )


# Extract Region

def extract_region(
    key,
    payload,
):

    if isinstance(
        payload,
        dict,
    ):

        payload_region = payload.get(
            "region"
        )

        if payload_region:
            return str(
                payload_region
            ).lower()

    partition_match = re.search(
        r"region=([A-Za-z]{2})",
        key,
    )

    if partition_match:

        return (
            partition_match
            .group(1)
            .lower()
        )

    filename = (
        key
        .rsplit("/", 1)[-1]
    )

    filename_match = re.match(
        r"([A-Za-z]{2})[_-]",
        filename,
    )

    if filename_match:

        return (
            filename_match
            .group(1)
            .lower()
        )

    return None


# Build Silver Key

def build_silver_key(
    key,
):

    relative_key = key.removeprefix(
        KAGGLE_SOURCE_PREFIX
    )

    parquet_key = os.path.splitext(
        relative_key
    )[0] + ".parquet"

    return (
        KAGGLE_SILVER_PREFIX
        + parquet_key
    )