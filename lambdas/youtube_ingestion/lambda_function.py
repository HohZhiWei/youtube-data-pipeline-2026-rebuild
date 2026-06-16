"""
YouTube Trending Data Ingestion Lambda

Purpose:
    Fetch trending video data from the YouTube Data API
    and store raw JSON responses in the Bronze layer.
"""

import json
import logging
import os
from datetime import datetime, timezone

import boto3
import requests


logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

API_KEY = os.environ["YOUTUBE_API_KEY"]
BRONZE_BUCKET = os.environ["S3_BUCKET_BRONZE"]
REGIONS = os.environ["YOUTUBE_REGIONS"].split(",")

API_BASE_URL = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50


def fetch_trending_videos(region_code):
    """Retrieve trending videos for given region"""

    endpoint = f"{API_BASE_URL}/videos"

    params = {
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code,
        "maxResults": MAX_RESULTS,
        "key": API_KEY,
    }

    response = requests.get(
        endpoint,
        params=params,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def write_to_s3(data, bucket, key):
    """Write raw JSON data to S3"""

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data),
        ContentType="application/json",
    )


def lambda_handler(event, context):

    timestamp = datetime.now(timezone.utc)

    date_partition = timestamp.strftime("%Y-%m-%d")
    hour_partition = timestamp.strftime("%H")
    ingestion_id = timestamp.strftime("%Y%m%d_%H%M%S")

    results = {
        "success": [],
        "failed": [],
    }

    for region in REGIONS:

        region = region.strip().upper()

        logger.info(f"Processing region: {region}")

        try:
            data = fetch_trending_videos(region)

            s3_key = (
                f"youtube/raw_statistics/"
                f"region={region}/"
                f"date={date_partition}/"
                f"hour={hour_partition}/"
                f"{ingestion_id}.json"
            )

            write_to_s3(
                data=data,
                bucket=BRONZE_BUCKET,
                key=s3_key,
            )

            results["success"].append(
                {
                    "region": region,
                    "record_count": len(data.get("items", [])),
                    "s3_key": s3_key,
                }
            )

        except Exception as error:
            logger.error(f"Failed to process {region}: {error}")

            results["failed"].append(
                {
                    "region": region,
                    "error": str(error),
                }
            )

    return {
        "statusCode": 200 if not results["failed"] else 207,
        "ingestion_id": ingestion_id,
        "results": results,
    }