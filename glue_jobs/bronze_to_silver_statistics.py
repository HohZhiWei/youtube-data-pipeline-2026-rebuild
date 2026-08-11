"""
Glue Job: Bronze to Silver Statistics

Purpose:
    Transform raw YouTube API and Kaggle statistics
    from Bronze into standardized Parquet data in Silver.

Silver outputs:
    silver/api_statistics/
    silver/kaggle_statistics/
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
)


# Job Setup

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_database",
        "bronze_api_statistics_table",
        "bronze_kaggle_statistics_table",
        "silver_bucket",
        "silver_database",
        "silver_api_statistics_table",
        "silver_kaggle_statistics_table",
    ],
)

sc = SparkContext()

glue_context = GlueContext(sc)

spark = glue_context.spark_session

job = Job(glue_context)

job.init(
    args["JOB_NAME"],
    args,
)

logger = glue_context.get_logger()


# Configuration

BRONZE_DATABASE = args[
    "bronze_database"
]

API_BRONZE_TABLE = args[
    "bronze_api_statistics_table"
]

KAGGLE_BRONZE_TABLE = args[
    "bronze_kaggle_statistics_table"
]

SILVER_BUCKET = args[
    "silver_bucket"
]

SILVER_DATABASE = args[
    "silver_database"
]

API_SILVER_TABLE = args[
    "silver_api_statistics_table"
]

KAGGLE_SILVER_TABLE = args[
    "silver_kaggle_statistics_table"
]


API_SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/"
    "silver/api_statistics/"
)

KAGGLE_SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/"
    "silver/kaggle_statistics/"
)


# Read Bronze Table

def read_bronze_table(table_name):

    logger.info(
        f"Reading "
        f"{BRONZE_DATABASE}.{table_name}"
    )

    dynamic_frame = (
        glue_context
        .create_dynamic_frame
        .from_catalog(
            database=BRONZE_DATABASE,
            table_name=table_name,
        )
    )

    return dynamic_frame.toDF()


# Transform API Statistics

def transform_api_statistics(df):

    logger.info(
        "Transforming YouTube API statistics"
    )

    if "region" in df.columns:
        region_column = F.col(
            "region"
        )

    else:
        region_column = F.regexp_extract(
            F.input_file_name(),
            r"region=([A-Za-z]{2})",
            1,
        )

    if "date" in df.columns:
        date_column = F.col(
            "date"
        )

    else:
        date_column = F.regexp_extract(
            F.input_file_name(),
            r"date=([0-9]{4}-[0-9]{2}-[0-9]{2})",
            1,
        )

    df = (
        df
        .withColumn(
            "_region",
            region_column,
        )
        .withColumn(
            "_ingestion_date",
            date_column,
        )
        .select(
            F.explode_outer(
                "items"
            ).alias(
                "item"
            ),
            "_region",
            "_ingestion_date",
        )
    )

    df = df.select(

        F.col(
            "item.id"
        ).cast(
            StringType()
        ).alias(
            "video_id"
        ),

        F.to_date(
            "_ingestion_date"
        ).alias(
            "trending_date"
        ),

        F.col(
            "item.snippet.title"
        ).cast(
            StringType()
        ).alias(
            "title"
        ),

        F.col(
            "item.snippet.channelTitle"
        ).cast(
            StringType()
        ).alias(
            "channel_title"
        ),

        F.col(
            "item.snippet.categoryId"
        ).cast(
            LongType()
        ).alias(
            "category_id"
        ),

        F.col(
            "item.snippet.publishedAt"
        ).cast(
            StringType()
        ).alias(
            "publish_time"
        ),

        F.col(
            "item.snippet.tags"
        ).cast(
            StringType()
        ).alias(
            "tags"
        ),

        F.col(
            "item.statistics.viewCount"
        ).cast(
            LongType()
        ).alias(
            "views"
        ),

        F.col(
            "item.statistics.likeCount"
        ).cast(
            LongType()
        ).alias(
            "likes"
        ),

        F.lit(
            0
        ).cast(
            LongType()
        ).alias(
            "dislikes"
        ),

        F.col(
            "item.statistics.commentCount"
        ).cast(
            LongType()
        ).alias(
            "comment_count"
        ),

        F.col(
            "item.snippet.thumbnails.default.url"
        ).cast(
            StringType()
        ).alias(
            "thumbnail_link"
        ),

        F.lit(
            False
        ).cast(
            BooleanType()
        ).alias(
            "comments_disabled"
        ),

        F.lit(
            False
        ).cast(
            BooleanType()
        ).alias(
            "ratings_disabled"
        ),

        F.lit(
            False
        ).cast(
            BooleanType()
        ).alias(
            "video_error_or_removed"
        ),

        F.col(
            "item.snippet.description"
        ).cast(
            StringType()
        ).alias(
            "description"
        ),

        F.col(
            "_region"
        ).cast(
            StringType()
        ).alias(
            "region"
        ),
    )

    return clean_statistics(
        df=df,
        source="api",
    )


# Transform Kaggle Statistics

def transform_kaggle_statistics(df):

    logger.info(
        "Transforming Kaggle statistics"
    )

    if "region" not in df.columns:

        df = df.withColumn(
            "region",
            F.regexp_extract(
                F.input_file_name(),
                r"(?i)/([A-Za-z]{2})videos\.csv$",
                1,
            ),
        )

    df = df.select(

        F.col(
            "video_id"
        ).cast(
            StringType()
        ).alias(
            "video_id"
        ),

        F.col(
            "trending_date"
        ).cast(
            StringType()
        ).alias(
            "trending_date"
        ),

        F.col(
            "title"
        ).cast(
            StringType()
        ).alias(
            "title"
        ),

        F.col(
            "channel_title"
        ).cast(
            StringType()
        ).alias(
            "channel_title"
        ),

        F.col(
            "category_id"
        ).cast(
            LongType()
        ).alias(
            "category_id"
        ),

        F.col(
            "publish_time"
        ).cast(
            StringType()
        ).alias(
            "publish_time"
        ),

        F.col(
            "tags"
        ).cast(
            StringType()
        ).alias(
            "tags"
        ),

        F.col(
            "views"
        ).cast(
            LongType()
        ).alias(
            "views"
        ),

        F.col(
            "likes"
        ).cast(
            LongType()
        ).alias(
            "likes"
        ),

        F.col(
            "dislikes"
        ).cast(
            LongType()
        ).alias(
            "dislikes"
        ),

        F.col(
            "comment_count"
        ).cast(
            LongType()
        ).alias(
            "comment_count"
        ),

        F.col(
            "thumbnail_link"
        ).cast(
            StringType()
        ).alias(
            "thumbnail_link"
        ),

        F.col(
            "comments_disabled"
        ).cast(
            BooleanType()
        ).alias(
            "comments_disabled"
        ),

        F.col(
            "ratings_disabled"
        ).cast(
            BooleanType()
        ).alias(
            "ratings_disabled"
        ),

        F.col(
            "video_error_or_removed"
        ).cast(
            BooleanType()
        ).alias(
            "video_error_or_removed"
        ),

        F.col(
            "description"
        ).cast(
            StringType()
        ).alias(
            "description"
        ),

        F.col(
            "region"
        ).cast(
            StringType()
        ).alias(
            "region"
        ),
    )

    df = df.withColumn(
        "trending_date",

        F.when(
            F.col(
                "trending_date"
            ).rlike(
                r"^\d{2}\.\d{2}\.\d{2}$"
            ),

            F.to_date(
                F.col(
                    "trending_date"
                ),
                "yy.dd.MM",
            ),

        ).otherwise(

            F.to_date(
                F.col(
                    "trending_date"
                )
            )
        ),
    )

    return clean_statistics(
        df=df,
        source="kaggle",
    )


# Clean Statistics

def clean_statistics(
    df,
    source,
):

    df = df.filter(
        F.col(
            "video_id"
        ).isNotNull()
    )

    df = df.withColumn(
        "region",

        F.lower(
            F.trim(
                F.col(
                    "region"
                )
            )
        ),
    )

    numeric_columns = [
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]

    for column_name in numeric_columns:

        df = df.withColumn(
            column_name,

            F.coalesce(
                F.col(
                    column_name
                ),

                F.lit(
                    0
                ).cast(
                    LongType()
                ),
            ),
        )

    df = df.withColumn(
        "like_ratio",

        F.when(
            F.col(
                "views"
            ) > 0,

            F.round(
                F.col(
                    "likes"
                )
                / F.col(
                    "views"
                )
                * 100,
                4,
            ),

        ).otherwise(
            F.lit(
                0.0
            )
        ),
    )

    df = df.withColumn(
        "engagement_rate",

        F.when(
            F.col(
                "views"
            ) > 0,

            F.round(
                (
                    F.col(
                        "likes"
                    )
                    + F.col(
                        "dislikes"
                    )
                    + F.col(
                        "comment_count"
                    )
                )
                / F.col(
                    "views"
                )
                * 100,
                4,
            ),

        ).otherwise(
            F.lit(
                0.0
            )
        ),
    )

    dedupe_columns = [
        "video_id",
        "region",
        "trending_date",
        "views",
        "likes",
        "dislikes",
        "comment_count",
    ]

    df = df.dropDuplicates(
        dedupe_columns
    )

    df = df.withColumn(
        "source",
        F.lit(
            source
        ),
    )

    df = df.withColumn(
        "_processed_at",
        F.current_timestamp(),
    )

    return df


# Validate Silver Data

def validate_silver(
    df,
    dataset_name,
):

    total_rows = df.count()

    null_video_ids = (
        df
        .filter(
            F.col(
                "video_id"
            ).isNull()
        )
        .count()
    )

    negative_views = (
        df
        .filter(
            F.col(
                "views"
            ) < 0
        )
        .count()
    )

    invalid_dates = (
        df
        .filter(
            F.col(
                "trending_date"
            ).isNull()
        )
        .count()
    )

    logger.info(
        f"{dataset_name}: "
        f"rows={total_rows}, "
        f"null_video_ids={null_video_ids}, "
        f"negative_views={negative_views}, "
        f"invalid_dates={invalid_dates}"
    )


# Write Silver Data

def write_silver(
    df,
    path,
    table_name,
):

    logger.info(
        f"Writing Silver table "
        f"{table_name} to {path}"
    )

    dynamic_frame = DynamicFrame.fromDF(
        df,
        glue_context,
        table_name,
    )

    sink = glue_context.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[
            "region",
            "trending_date",
        ],
    )

    sink.setCatalogInfo(
        catalogDatabase=SILVER_DATABASE,
        catalogTableName=table_name,
    )

    sink.setFormat(
        "glueparquet",
        compression="snappy",
    )

    sink.writeFrame(
        dynamic_frame
    )


# Run Transformation

api_raw = read_bronze_table(
    API_BRONZE_TABLE
)

kaggle_raw = read_bronze_table(
    KAGGLE_BRONZE_TABLE
)


api_silver = transform_api_statistics(
    api_raw
)

kaggle_silver = transform_kaggle_statistics(
    kaggle_raw
)


validate_silver(
    api_silver,
    "API statistics",
)

validate_silver(
    kaggle_silver,
    "Kaggle statistics",
)


write_silver(
    api_silver,
    API_SILVER_PATH,
    API_SILVER_TABLE,
)

write_silver(
    kaggle_silver,
    KAGGLE_SILVER_PATH,
    KAGGLE_SILVER_TABLE,
)


logger.info(
    "Bronze to Silver statistics transformation complete"
)


job.commit()