"""
Upload Kaggle YouTube trending dataset to S3 Bronze.

Purpose:
    Upload raw Kaggle CSV and category JSON files
    into the Bronze layer without transformation.
"""

from pathlib import Path

import boto3


BRONZE_BUCKET = "youtube-data-pipeline-bronze-ap-southeast-2"

LOCAL_CSV_DIR = Path("data/raw/kaggle/csv")
LOCAL_REFERENCE_DIR = Path("data/raw/kaggle/reference")

CSV_S3_PREFIX = "youtube/raw_kaggle_statistics"
REFERENCE_S3_PREFIX = "youtube/raw_kaggle_reference_data"

s3_client = boto3.client("s3")


def extract_region(file_path):
    """Extract region code from Kaggle filename."""

    filename = file_path.name

    if filename.endswith("videos.csv"):
        return filename.replace("videos.csv", "")

    if filename.endswith("_category_id.json"):
        return filename.replace("_category_id.json", "")

    raise ValueError(f"Unsupported Kaggle filename: {filename}")


def main():
    """Upload Kaggle files into S3 Bronze."""

    csv_files = list(LOCAL_CSV_DIR.glob("*.csv"))
    reference_files = list(LOCAL_REFERENCE_DIR.glob("*.json"))

    success = []
    failed = []

    file_groups = [
        {
            "files": csv_files,
            "s3_prefix": CSV_S3_PREFIX,
        },
        {
            "files": reference_files,
            "s3_prefix": REFERENCE_S3_PREFIX,
        },
    ]

    for group in file_groups:
        for local_file in group["files"]:
            try:
                region = extract_region(local_file)

                s3_key = (
                    f"{group['s3_prefix']}/"
                    f"region={region}/"
                    f"{local_file.name}"
                )

                s3_client.upload_file(
                    Filename=str(local_file),
                    Bucket=BRONZE_BUCKET,
                    Key=s3_key,
                )

                success.append(
                    {
                        "file": local_file.name,
                        "region": region,
                        "s3_key": s3_key,
                    }
                )

            except Exception as error:
                failed.append(
                    {
                        "file": local_file.name,
                        "error": str(error),
                    }
                )

    print(f"Successfully uploaded: {len(success)}")
    print(f"Failed uploads: {len(failed)}")

    return {
        "success": success,
        "failed": failed,
    }


if __name__ == "__main__":
    main()