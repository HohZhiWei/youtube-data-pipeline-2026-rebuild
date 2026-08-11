"""
Glue Job: Bronze -> Silver Statistics

Transforms:
1. YouTube API statistics JSON -> Silver Parquet
2. Kaggle statistics CSV -> Silver Parquet

The Silver statistics paths are rebuilt on every run so repeated
pipeline executions do not append duplicate copies.
"""

import sys

from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "bronze_database",
        "bronze_api_statistics_table",
        "bronze_kaggle_statistics_table",
        "bronze_bucket",
        "silver_bucket",
        "silver_database",
        "silver_api_statistics_table",
        "silver_kaggle_statistics_table",
    ],
)


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()


BRONZE_DATABASE = args["bronze_database"]

BRONZE_API_STATISTICS_TABLE = args[
    "bronze_api_statistics_table"
]

BRONZE_KAGGLE_STATISTICS_TABLE = args[
    "bronze_kaggle_statistics_table"
]

BRONZE_BUCKET = args["bronze_bucket"]

SILVER_BUCKET = args["silver_bucket"]
SILVER_DATABASE = args["silver_database"]

SILVER_API_STATISTICS_TABLE = args[
    "silver_api_statistics_table"
]

SILVER_KAGGLE_STATISTICS_TABLE = args[
    "silver_kaggle_statistics_table"
]


KAGGLE_BRONZE_PATH = (
    f"s3://{BRONZE_BUCKET}/youtube/raw_kaggle_statistics/"
)

API_SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/silver/api_statistics/"
)

KAGGLE_SILVER_PATH = (
    f"s3://{SILVER_BUCKET}/silver/kaggle_statistics/"
)


logger.info(
    f"Bronze API table: "
    f"{BRONZE_DATABASE}.{BRONZE_API_STATISTICS_TABLE}"
)

logger.info(
    f"Bronze Kaggle path: {KAGGLE_BRONZE_PATH}"
)

logger.info(
    f"Silver API path: {API_SILVER_PATH}"
)

logger.info(
    f"Silver Kaggle path: {KAGGLE_SILVER_PATH}"
)


def read_api_statistics():
    logger.info(
        "Reading API statistics from Bronze Glue Catalog..."
    )

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=BRONZE_DATABASE,
        table_name=BRONZE_API_STATISTICS_TABLE,
        transformation_ctx="bronze_api_statistics",
    )

    return dynamic_frame.toDF()


def read_kaggle_statistics():
    logger.info(
        f"Reading Kaggle statistics from {KAGGLE_BRONZE_PATH}"
    )

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .csv(KAGGLE_BRONZE_PATH)
    )


def transform_api_statistics(df):
    logger.info("Transforming API statistics...")

    exploded = df.select(
        F.lower(
            F.col("region")
        ).alias("region"),

        F.col("date").alias(
            "trending_date"
        ),

        F.col("hour").alias(
            "_source_hour"
        ),

        F.explode_outer(
            F.col("items")
        ).alias("item"),
    )

    exploded = exploded.withColumn(
        "_item_json",
        F.to_json(F.col("item")),
    )

    api_df = exploded.select(
        F.get_json_object(
            F.col("_item_json"),
            "$.id",
        ).alias("video_id"),

        F.col("trending_date"),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.title",
        ).alias("title"),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.channelTitle",
        ).alias("channel_title"),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.categoryId",
        ).cast("long").alias("category_id"),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.publishedAt",
        ).alias("publish_time"),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.tags",
        ).alias("tags"),

        F.get_json_object(
            F.col("_item_json"),
            "$.statistics.viewCount",
        ).cast("long").alias("views"),

        F.get_json_object(
            F.col("_item_json"),
            "$.statistics.likeCount",
        ).cast("long").alias("likes"),

        F.lit(0)
        .cast("long")
        .alias("dislikes"),

        F.get_json_object(
            F.col("_item_json"),
            "$.statistics.commentCount",
        ).cast("long").alias("comment_count"),

        F.lit(False).alias(
            "comments_disabled"
        ),

        F.lit(False).alias(
            "ratings_disabled"
        ),

        F.lit(False).alias(
            "video_error_or_removed"
        ),

        F.get_json_object(
            F.col("_item_json"),
            "$.snippet.description",
        ).alias("description"),

        F.col("region"),

        F.col("_source_hour"),
    )

    return api_df


def transform_kaggle_statistics(df):
    logger.info("Transforming Kaggle statistics...")

    kaggle_df = df.select(
        F.col("video_id")
        .cast("string")
        .alias("video_id"),

        F.date_format(
            F.to_date(
                F.col("trending_date").cast("string"),
                "yy.dd.MM",
            ),
            "yyyy-MM-dd",
        ).alias("trending_date"),

        F.col("title")
        .cast("string")
        .alias("title"),

        F.col("channel_title")
        .cast("string")
        .alias("channel_title"),

        F.col("category_id")
        .cast("long")
        .alias("category_id"),

        F.col("publish_time")
        .cast("string")
        .alias("publish_time"),

        F.col("tags")
        .cast("string")
        .alias("tags"),

        F.col("views")
        .cast("long")
        .alias("views"),

        F.col("likes")
        .cast("long")
        .alias("likes"),

        F.col("dislikes")
        .cast("long")
        .alias("dislikes"),

        F.col("comment_count")
        .cast("long")
        .alias("comment_count"),

        F.col("comments_disabled")
        .cast("boolean")
        .alias("comments_disabled"),

        F.col("ratings_disabled")
        .cast("boolean")
        .alias("ratings_disabled"),

        F.col("video_error_or_removed")
        .cast("boolean")
        .alias("video_error_or_removed"),

        F.col("description")
        .cast("string")
        .alias("description"),

        F.lower(
            F.col("region").cast("string")
        ).alias("region"),
    )

    return kaggle_df


def clean_statistics(df, source):
    logger.info(
        f"Cleaning {source} statistics..."
    )

    df = (
        df
        .withColumn(
            "video_id",
            F.trim(F.col("video_id")),
        )
        .withColumn(
            "title",
            F.trim(F.col("title")),
        )
        .withColumn(
            "channel_title",
            F.trim(F.col("channel_title")),
        )
        .withColumn(
            "region",
            F.lower(
                F.trim(F.col("region"))
            ),
        )
    )

    df = df.filter(
        F.col("video_id").isNotNull()
        & F.col("title").isNotNull()
        & F.col("channel_title").isNotNull()
        & F.col("region").isNotNull()
        & F.col("trending_date").isNotNull()
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
                F.col(column_name).cast("long"),
                F.lit(0).cast("long"),
            ),
        )

    df = df.withColumn(
        "like_ratio",
        F.when(
            F.col("views") > 0,
            F.round(
                (
                    F.col("likes")
                    / F.col("views")
                )
                * 100,
                4,
            ),
        ).otherwise(
            F.lit(0.0)
        ),
    )

    df = df.withColumn(
        "engagement_rate",
        F.when(
            F.col("views") > 0,
            F.round(
                (
                    (
                        F.col("likes")
                        + F.col("dislikes")
                        + F.col("comment_count")
                    )
                    / F.col("views")
                )
                * 100,
                4,
            ),
        ).otherwise(
            F.lit(0.0)
        ),
    )

    df = df.withColumn(
        "source",
        F.lit(source),
    )

    return df


def deduplicate_api_statistics(df):
    logger.info("Deduplicating API statistics...")

    window = (
        Window
        .partitionBy(
            "video_id",
            "region",
            "trending_date",
        )
        .orderBy(
            F.col(
                "_source_hour"
            ).desc_nulls_last()
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number",
            "_source_hour",
        )
    )


def deduplicate_kaggle_statistics(df):
    logger.info(
        "Deduplicating Kaggle statistics..."
    )

    return df.dropDuplicates(
        [
            "video_id",
            "region",
            "trending_date",
        ]
    )


def add_processing_metadata(df):
    return (
        df
        .withColumn(
            "_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "_job_name",
            F.lit(args["JOB_NAME"]),
        )
    )


def write_silver(
    df,
    output_path,
    table_name,
    transformation_context,
):
    logger.info(
        f"Purging previous Silver output: "
        f"{output_path}"
    )

    glueContext.purge_s3_path(
        output_path,
        {
            "retentionPeriod": 0,
        },
    )

    logger.info(
        f"Writing "
        f"{SILVER_DATABASE}.{table_name}"
    )

    dynamic_frame = DynamicFrame.fromDF(
        df,
        glueContext,
        transformation_context,
    )

    sink = glueContext.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=[
            "region",
            "trending_date",
        ],
        transformation_ctx=(
            f"{transformation_context}_sink"
        ),
    )

    sink.setCatalogInfo(
        catalogDatabase=SILVER_DATABASE,
        catalogTableName=table_name,
    )

    sink.setFormat(
        "glueparquet",
        compression="snappy",
    )

    sink.writeFrame(dynamic_frame)

    logger.info(
        f"Finished writing "
        f"{SILVER_DATABASE}.{table_name}"
    )


logger.info(
    "Starting Bronze to Silver statistics transformation."
)


api_raw = read_api_statistics()

logger.info(
    f"API Bronze parent records: "
    f"{api_raw.count()}"
)

api_statistics = transform_api_statistics(
    api_raw
)

api_statistics = clean_statistics(
    api_statistics,
    source="api",
)

api_statistics = deduplicate_api_statistics(
    api_statistics
)

api_statistics = add_processing_metadata(
    api_statistics
)

api_count = api_statistics.count()

logger.info(
    f"API Silver rows: {api_count}"
)


kaggle_raw = read_kaggle_statistics()

logger.info(
    f"Kaggle Bronze rows: "
    f"{kaggle_raw.count()}"
)

kaggle_statistics = transform_kaggle_statistics(
    kaggle_raw
)

kaggle_statistics = clean_statistics(
    kaggle_statistics,
    source="kaggle",
)

kaggle_statistics = deduplicate_kaggle_statistics(
    kaggle_statistics
)

kaggle_statistics = add_processing_metadata(
    kaggle_statistics
)

kaggle_count = kaggle_statistics.count()

logger.info(
    f"Kaggle Silver rows: "
    f"{kaggle_count}"
)


write_silver(
    df=api_statistics,
    output_path=API_SILVER_PATH,
    table_name=SILVER_API_STATISTICS_TABLE,
    transformation_context="api_statistics_silver",
)

write_silver(
    df=kaggle_statistics,
    output_path=KAGGLE_SILVER_PATH,
    table_name=SILVER_KAGGLE_STATISTICS_TABLE,
    transformation_context="kaggle_statistics_silver",
)


logger.info(
    "Bronze to Silver statistics transformation completed."
)

job.commit()