"""
Silver Data Quality Lambda

Purpose:
    Validate Silver statistics and reference datasets
    before allowing the pipeline to continue to Gold.
"""

import os
import time

import boto3


athena_client = boto3.client(
    "athena"
)


# Configuration

ATHENA_DATABASE = os.environ[
    "ATHENA_DATABASE"
]

ATHENA_OUTPUT_S3 = os.environ[
    "ATHENA_OUTPUT_S3"
]

API_STATISTICS_TABLE = os.environ.get(
    "SILVER_API_STATISTICS_TABLE",
    "api_statistics",
)

KAGGLE_STATISTICS_TABLE = os.environ.get(
    "SILVER_KAGGLE_STATISTICS_TABLE",
    "kaggle_statistics",
)

KAGGLE_REFERENCE_TABLE = os.environ.get(
    "SILVER_KAGGLE_REFERENCE_TABLE",
    "kaggle_reference_data",
)


# Lambda Handler

def lambda_handler(
    event,
    context,
):

    checks = []

    checks.extend(
        validate_statistics_table(
            API_STATISTICS_TABLE
        )
    )

    checks.extend(
        validate_statistics_table(
            KAGGLE_STATISTICS_TABLE
        )
    )

    checks.extend(
        validate_reference_table(
            KAGGLE_REFERENCE_TABLE
        )
    )

    failed_checks = [
        check
        for check in checks
        if not check["passed"]
    ]

    data_quality_passed = (
        len(failed_checks) == 0
    )

    return {
        "statusCode": (
            200
            if data_quality_passed
            else 422
        ),
        "data_quality_passed": (
            data_quality_passed
        ),
        "checks": checks,
        "failed_checks": failed_checks,
    }


# Validate Statistics

def validate_statistics_table(
    table_name,
):

    table = qualified_table(
        table_name
    )

    summary_query = f"""
        SELECT
            COUNT(*) AS total_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN video_id IS NULL
                          OR title IS NULL
                          OR channel_title IS NULL
                          OR views IS NULL
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS required_null_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN views < 0
                          OR likes < 0
                          OR dislikes < 0
                          OR comment_count < 0
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS negative_metric_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN region IS NULL
                          OR TRIM(region) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS missing_region_rows

        FROM {table}
    """

    summary = run_query(
        summary_query
    )

    duplicate_query = f"""
        SELECT
            COUNT(*) AS duplicate_groups

        FROM (
            SELECT
                video_id,
                region,
                trending_date,
                views,
                likes,
                dislikes,
                comment_count

            FROM {table}

            GROUP BY
                video_id,
                region,
                trending_date,
                views,
                likes,
                dislikes,
                comment_count

            HAVING COUNT(*) > 1
        )
    """

    duplicates = run_query(
        duplicate_query
    )

    total_rows = to_int(
        summary["total_rows"]
    )

    null_rows = to_int(
        summary["required_null_rows"]
    )

    negative_rows = to_int(
        summary["negative_metric_rows"]
    )

    missing_regions = to_int(
        summary["missing_region_rows"]
    )

    duplicate_groups = to_int(
        duplicates[
            "duplicate_groups"
        ]
    )

    return [
        create_check(
            table_name,
            "row_count",
            total_rows,
            total_rows > 0,
        ),

        create_check(
            table_name,
            "required_null_rows",
            null_rows,
            null_rows == 0,
        ),

        create_check(
            table_name,
            "negative_metric_rows",
            negative_rows,
            negative_rows == 0,
        ),

        create_check(
            table_name,
            "missing_region_rows",
            missing_regions,
            missing_regions == 0,
        ),

        create_check(
            table_name,
            "duplicate_groups",
            duplicate_groups,
            duplicate_groups == 0,
        ),
    ]


# Validate Reference

def validate_reference_table(
    table_name,
):

    table = qualified_table(
        table_name
    )

    summary_query = f"""
        SELECT
            COUNT(*) AS total_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN category_id IS NULL
                          OR category_title IS NULL
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS required_null_rows,

            COALESCE(
                SUM(
                    CASE
                        WHEN region IS NULL
                          OR TRIM(region) = ''
                        THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS missing_region_rows

        FROM {table}
    """

    summary = run_query(
        summary_query
    )

    duplicate_query = f"""
        SELECT
            COUNT(*) AS duplicate_groups

        FROM (
            SELECT
                category_id,
                region

            FROM {table}

            GROUP BY
                category_id,
                region

            HAVING COUNT(*) > 1
        )
    """

    duplicates = run_query(
        duplicate_query
    )

    total_rows = to_int(
        summary["total_rows"]
    )

    null_rows = to_int(
        summary["required_null_rows"]
    )

    missing_regions = to_int(
        summary["missing_region_rows"]
    )

    duplicate_groups = to_int(
        duplicates[
            "duplicate_groups"
        ]
    )

    return [
        create_check(
            table_name,
            "row_count",
            total_rows,
            total_rows > 0,
        ),

        create_check(
            table_name,
            "required_null_rows",
            null_rows,
            null_rows == 0,
        ),

        create_check(
            table_name,
            "missing_region_rows",
            missing_regions,
            missing_regions == 0,
        ),

        create_check(
            table_name,
            "duplicate_groups",
            duplicate_groups,
            duplicate_groups == 0,
        ),
    ]


# Run Athena Query

def run_query(
    query,
):

    response = (
        athena_client
        .start_query_execution(
            QueryString=query,

            QueryExecutionContext={
                "Database": (
                    ATHENA_DATABASE
                ),
            },

            ResultConfiguration={
                "OutputLocation": (
                    ATHENA_OUTPUT_S3
                ),
            },
        )
    )

    query_execution_id = response[
        "QueryExecutionId"
    ]

    wait_for_query(
        query_execution_id
    )

    results = (
        athena_client
        .get_query_results(
            QueryExecutionId=(
                query_execution_id
            )
        )
    )

    rows = (
        results[
            "ResultSet"
        ]
        ["Rows"]
    )

    if len(rows) < 2:

        raise RuntimeError(
            "Athena query returned no result row"
        )

    headers = [
        item.get(
            "VarCharValue"
        )
        for item
        in rows[0]["Data"]
    ]

    values = [
        item.get(
            "VarCharValue"
        )
        for item
        in rows[1]["Data"]
    ]

    return dict(
        zip(
            headers,
            values,
        )
    )


# Wait for Athena

def wait_for_query(
    query_execution_id,
):

    while True:

        response = (
            athena_client
            .get_query_execution(
                QueryExecutionId=(
                    query_execution_id
                )
            )
        )

        status = (
            response[
                "QueryExecution"
            ]
            ["Status"]
            ["State"]
        )

        if status == "SUCCEEDED":
            return

        if status in {
            "FAILED",
            "CANCELLED",
        }:

            reason = (
                response[
                    "QueryExecution"
                ]
                ["Status"]
                .get(
                    "StateChangeReason",
                    status,
                )
            )

            raise RuntimeError(
                reason
            )

        time.sleep(1)


# Check Result

def create_check(
    dataset,
    check,
    value,
    passed,
):

    return {
        "dataset": dataset,
        "check": check,
        "value": value,
        "passed": passed,
    }


# Table Name

def qualified_table(
    table_name,
):

    return (
        f'"{ATHENA_DATABASE}".'
        f'"{table_name}"'
    )


# Convert Integer

def to_int(
    value,
):

    if value is None:
        return 0

    return int(
        value
    )