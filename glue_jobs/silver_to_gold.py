"""
Glue Job: Silver to Gold

Purpose:
    Read standardized Silver statistics and reference data
    and create analytical Gold datasets.

Gold outputs:
    gold/trending_analytics/
    gold/channel_analytics/
    gold/category_analytics/
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# Job Setup

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "silver_database",
        "silver_api_statistics_table",
        "silver_kaggle_statistics_table",
        "silver_api_reference_table",
        "silver_kaggle_reference_table",
        "gold_bucket",
        "gold_database",
        "gold_trending_table",
        "gold_channel_table",
        "gold_category_table",
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

SILVER_DATABASE = args[
    "silver_database"
]

GOLD_BUCKET = args[
    "gold_bucket"
]

GOLD_DATABASE = args[
    "gold_database"
]

API_STATISTICS_TABLE = args[
    "silver_api_statistics_table"
]

KAGGLE_STATISTICS_TABLE = args[
    "silver_kaggle_statistics_table"
]

API_REFERENCE_TABLE = args[
    "silver_api_reference_table"
]

KAGGLE_REFERENCE_TABLE = args[
    "silver_kaggle_reference_table"
]

GOLD_TRENDING_TABLE = args[
    "gold_trending_table"
]

GOLD_CHANNEL_TABLE = args[
    "gold_channel_table"
]

GOLD_CATEGORY_TABLE = args[
    "gold_category_table"
]


TRENDING_PATH = (
    f"s3://{GOLD_BUCKET}/"
    "gold/trending_analytics/"
)

CHANNEL_PATH = (
    f"s3://{GOLD_BUCKET}/"
    "gold/channel_analytics/"
)

CATEGORY_PATH = (
    f"s3://{GOLD_BUCKET}/"
    "gold/category_analytics/"
)


# Read Silver Table

def read_silver_table(
    table_name,
):

    logger.info(
        f"Reading "
        f"{SILVER_DATABASE}.{table_name}"
    )

    dynamic_frame = (
        glue_context
        .create_dynamic_frame
        .from_catalog(
            database=SILVER_DATABASE,
            table_name=table_name,
        )
    )

    return dynamic_frame.toDF()


# Combine Statistics

def combine_statistics():

    api_statistics = read_silver_table(
        API_STATISTICS_TABLE
    )

    kaggle_statistics = read_silver_table(
        KAGGLE_STATISTICS_TABLE
    )

    statistics = (
        api_statistics
        .unionByName(
            kaggle_statistics,
            allowMissingColumns=True,
        )
    )

    statistics = statistics.dropDuplicates(
        [
            "video_id",
            "region",
            "trending_date",
            "views",
            "likes",
            "dislikes",
            "comment_count",
        ]
    )

    return statistics


# Combine Reference Data

def combine_reference_data():

    api_reference = read_silver_table(
        API_REFERENCE_TABLE
    )

    kaggle_reference = read_silver_table(
        KAGGLE_REFERENCE_TABLE
    )

    reference = (
        api_reference
        .unionByName(
            kaggle_reference,
            allowMissingColumns=True,
        )
    )

    reference = (
        reference
        .groupBy(
            "region",
            "category_id",
        )
        .agg(
            F.first(
                "category_title",
                ignorenulls=True,
            ).alias(
                "category_title"
            )
        )
    )

    return reference


# Trending Analytics

def create_trending_analytics(
    statistics,
):

    return (
        statistics
        .groupBy(
            "region",
            "trending_date",
        )
        .agg(
            F.countDistinct(
                "video_id"
            ).alias(
                "trending_video_count"
            ),

            F.sum(
                "views"
            ).alias(
                "total_views"
            ),

            F.round(
                F.avg(
                    "views"
                ),
                2,
            ).alias(
                "average_views"
            ),

            F.sum(
                "likes"
            ).alias(
                "total_likes"
            ),

            F.sum(
                "comment_count"
            ).alias(
                "total_comments"
            ),

            F.round(
                F.avg(
                    "engagement_rate"
                ),
                4,
            ).alias(
                "average_engagement_rate"
            ),

            F.max(
                "views"
            ).alias(
                "highest_video_views"
            ),
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp(),
        )
    )


# Channel Analytics

def create_channel_analytics(
    statistics,
):

    return (
        statistics
        .filter(
            F.col(
                "channel_title"
            ).isNotNull()
        )
        .groupBy(
            "region",
            "channel_title",
        )
        .agg(
            F.count(
                "*"
            ).alias(
                "trending_appearances"
            ),

            F.countDistinct(
                "video_id"
            ).alias(
                "unique_trending_videos"
            ),

            F.sum(
                "views"
            ).alias(
                "total_views"
            ),

            F.sum(
                "likes"
            ).alias(
                "total_likes"
            ),

            F.sum(
                "comment_count"
            ).alias(
                "total_comments"
            ),

            F.round(
                F.avg(
                    "views"
                ),
                2,
            ).alias(
                "average_views"
            ),

            F.round(
                F.avg(
                    "engagement_rate"
                ),
                4,
            ).alias(
                "average_engagement_rate"
            ),
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp(),
        )
    )


# Category Analytics

def create_category_analytics(
    statistics,
    reference,
):

    statistics_with_category = (
        statistics
        .join(
            reference,
            on=[
                "region",
                "category_id",
            ],
            how="left",
        )
        .withColumn(
            "category_title",
            F.coalesce(
                F.col(
                    "category_title"
                ),
                F.lit(
                    "Unknown"
                ),
            ),
        )
    )

    return (
        statistics_with_category
        .groupBy(
            "region",
            "category_id",
            "category_title",
        )
        .agg(
            F.count(
                "*"
            ).alias(
                "trending_appearances"
            ),

            F.countDistinct(
                "video_id"
            ).alias(
                "unique_trending_videos"
            ),

            F.sum(
                "views"
            ).alias(
                "total_views"
            ),

            F.sum(
                "likes"
            ).alias(
                "total_likes"
            ),

            F.sum(
                "comment_count"
            ).alias(
                "total_comments"
            ),

            F.round(
                F.avg(
                    "views"
                ),
                2,
            ).alias(
                "average_views"
            ),

            F.round(
                F.avg(
                    "engagement_rate"
                ),
                4,
            ).alias(
                "average_engagement_rate"
            ),
        )
        .withColumn(
            "_processed_at",
            F.current_timestamp(),
        )
    )


# Write Gold Data

def write_gold(
    df,
    table_name,
    path,
    partition_keys,
):

    logger.info(
        f"Writing Gold table "
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
        partitionKeys=partition_keys,
    )

    sink.setCatalogInfo(
        catalogDatabase=GOLD_DATABASE,
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

statistics = combine_statistics()

reference = combine_reference_data()


trending_analytics = (
    create_trending_analytics(
        statistics
    )
)

channel_analytics = (
    create_channel_analytics(
        statistics
    )
)

category_analytics = (
    create_category_analytics(
        statistics,
        reference,
    )
)


write_gold(
    trending_analytics,
    GOLD_TRENDING_TABLE,
    TRENDING_PATH,
    [
        "region",
        "trending_date",
    ],
)

write_gold(
    channel_analytics,
    GOLD_CHANNEL_TABLE,
    CHANNEL_PATH,
    [
        "region",
    ],
)

write_gold(
    category_analytics,
    GOLD_CATEGORY_TABLE,
    CATEGORY_PATH,
    [
        "region",
    ],
)


logger.info(
    "Silver to Gold transformation complete"
)


job.commit()