"""
Bronze to Silver Reference Data Lambda

Purpose:
    Transform raw YouTube API and Kaggle reference JSON files
    from Bronze S3 into standardized Parquet files in Silver S3.

Bronze inputs:
    raw_statistics_reference_data/
    raw_kaggle_reference_data/

Silver outputs:
    silver/api_reference_data/
    silver/kaggle_reference_data/
"""

import json
import os
import re
import urllib.parse
from datetime import datetime, timezone

import boto3
import pandas as pd


s3_client = boto3.client("s3")


# Environment Variables

BRONZE_BUCKET = os.environ["S3_BRONZE_BUCKET"]
SILVER_BUCKET = os.environ["S3_SILVER_BUCKET"]

API_SOURCE_PREFIX = os.environ.get(
    "API_REFERENCE_SOURCE_PREFIX",
    "raw_statistics_reference_data/",
)

KAGGLE_SOURCE_PREFIX = os.environ.get(
    "KAGGLE_REFERENCE_SOURCE_PREFIX",
    "raw_kaggle_reference_data/",
)

API_SILVER_PREFIX = os.environ.get(
    "SILVER_API_REFERENCE_PREFIX",
    "silver/api_reference_data/",
)

KAGGLE_SILVER_PREFIX = os.environ.get(
    "SILVER_KAGGLE_REFERENCE_PREFIX",
    "silver/kaggle_reference_data/",
)


# Lambda Handler

def lambda_handler(event, context):
    """
    Entry point for the Lambda.

    Supports:
        1. S3 event-triggered execution
        2. Step Functions batch execution
    """

    objects = get_objects_to_process(event)

    successful = []
    failed = []

    for bucket, key in objects:
        try:
            result = transform_reference_file(
                bucket=bucket,
                key=key,
            )

            successful.append(result)

        except Exception as error:
            print(
                f"Failed processing {key}: {error}"
            )

            failed.append({
                "bucket": bucket,
                "key": key,
                "error": str(error),
            })

    return {
        "statusCode": 200 if not failed else 207,
        "successful": successful,
        "failed": failed,
        "successful_count": len(successful),
        "failed_count": len(failed),
    }


# Determine Files to Process

def get_objects_to_process(event):
    """
    If Lambda was triggered by S3:
        Process the objects in the S3 event.

    If Lambda was started by Step Functions:
        Find reference JSON files in the Bronze bucket.
    """

    if "Records" in event:
        objects = []

        for record in event["Records"]:
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

            if key.lower().endswith(".json"):
                objects.append(
                    (bucket, key)
                )

        return objects

    return list_reference_objects()


# List Bronze Reference Files

def list_reference_objects():
    """
    Find JSON reference files stored under both
    Bronze reference prefixes.
    """

    prefixes = [
        API_SOURCE_PREFIX,
        KAGGLE_SOURCE_PREFIX,
    ]

    objects = []

    paginator = s3_client.get_paginator(
        "list_objects_v2"
    )

    for prefix in prefixes:
        pages = paginator.paginate(
            Bucket=BRONZE_BUCKET,
            Prefix=prefix,
        )

        for page in pages:
            for item in page.get(
                "Contents",
                [],
            ):
                key = item["Key"]

                if key.lower().endswith(".json"):
                    objects.append(
                        (
                            BRONZE_BUCKET,
                            key,
                        )
                    )

    return objects


# Transform One Reference File

def transform_reference_file(
    bucket,
    key,
):
    """
    Download one reference JSON file,
    normalize it,
    convert it to Parquet,
    and upload it to Silver.
    """

    source_type = get_source_type(key)

    print(
        f"Processing {source_type}: {key}"
    )

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key,
    )

    raw_data = response[
        "Body"
    ].read()

    payload = json.loads(
        raw_data.decode("utf-8")
    )

    items = extract_category_items(
        payload
    )

    region = extract_region(
        key=key,
        payload=payload,
    )

    records = []

    for item in items:
        snippet = item.get(
            "snippet",
            {},
        )

        record = {
            "category_id": item.get("id"),
            "category_title": snippet.get(
                "title"
            ),
            "assignable": snippet.get(
                "assignable"
            ),
            "channel_id": snippet.get(
                "channelId"
            ),
            "region": region,
            "source": source_type,
            "_source_key": key,
            "_processed_at": (
                datetime.now(
                    timezone.utc
                )
            ),
        }

        records.append(record)

    df = pd.DataFrame(records)

    if df.empty:
        raise ValueError(
            f"No category records found in {key}"
        )

    df["category_id"] = pd.to_numeric(
        df["category_id"],
        errors="coerce",
    ).astype("Int64")

    df = df.dropna(
        subset=[
            "category_id",
            "category_title",
        ]
    )

    df = df.drop_duplicates(
        subset=[
            "category_id",
            "region",
        ]
    )

    filename = (
        key
        .split("/")[-1]
        .rsplit(".", 1)[0]
    )

    local_path = (
        f"/tmp/{filename}.parquet"
    )

    df.to_parquet(
        local_path,
        index=False,
    )

    silver_key = build_silver_key(
        source_key=key,
        source_type=source_type,
    )

    s3_client.upload_file(
        Filename=local_path,
        Bucket=SILVER_BUCKET,
        Key=silver_key,
    )

    print(
        f"Written to "
        f"s3://{SILVER_BUCKET}/{silver_key}"
    )

    return {
        "source_key": key,
        "silver_key": silver_key,
        "source": source_type,
        "region": region,
        "record_count": len(df),
    }


# Extract Category Records

def extract_category_items(payload):
    """
    Extract the items[] array from YouTube category JSON.
    """

    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        raise ValueError(
            "Unsupported JSON structure"
        )

    if isinstance(
        payload.get("items"),
        list,
    ):
        return payload["items"]

    if isinstance(
        payload.get("data"),
        dict,
    ):
        data = payload["data"]

        if isinstance(
            data.get("items"),
            list,
        ):
            return data["items"]

    raise ValueError(
        "JSON does not contain an items array"
    )


# Identify Source

def get_source_type(key):

    if key.startswith(
        API_SOURCE_PREFIX
    ):
        return "api"

    if key.startswith(
        KAGGLE_SOURCE_PREFIX
    ):
        return "kaggle"

    raise ValueError(
        f"Unsupported Bronze prefix: {key}"
    )


# Determine Region

def extract_region(
    key,
    payload,
):
    """
    Determine region from JSON metadata,
    S3 partition path,
    or filename.
    """

    if isinstance(payload, dict):
        region = payload.get(
            "region"
        )

        if region:
            return (
                str(region)
                .strip()
                .lower()
            )

    match = re.search(
        r"region=([A-Za-z]{2})",
        key,
    )

    if match:
        return (
            match
            .group(1)
            .lower()
        )

    filename = key.split("/")[-1]

    match = re.match(
        r"([A-Za-z]{2})[_-]",
        filename,
    )

    if match:
        return (
            match
            .group(1)
            .lower()
        )

    raise ValueError(
        f"Could not determine region for {key}"
    )


# Build Silver S3 Key

def build_silver_key(
    source_key,
    source_type,
):
    """
    Preserve the Bronze subdirectory structure while
    moving the file into the correct Silver prefix.
    """

    if source_type == "api":
        relative_key = source_key[
            len(API_SOURCE_PREFIX):
        ]

        destination_prefix = (
            API_SILVER_PREFIX
        )

    else:
        relative_key = source_key[
            len(KAGGLE_SOURCE_PREFIX):
        ]

        destination_prefix = (
            KAGGLE_SILVER_PREFIX
        )

    parquet_key = (
        relative_key
        .rsplit(".", 1)[0]
        + ".parquet"
    )

    return (
        destination_prefix.rstrip("/")
        + "/"
        + parquet_key
    )