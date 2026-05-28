from datetime import datetime, timezone
import os
import logging
import uuid

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    ArrayType
)

from dotenv import load_dotenv


if os.getenv("ENV", "dev") != "prod":
    load_dotenv()


S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
SILVER_PREFIX = os.getenv("SILVER_PREFIX")

if not all([S3_BUCKET, AWS_REGION, BRONZE_PREFIX, SILVER_PREFIX]):
    raise ValueError("Missing required environment variables.")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/raw_apache_airflow_issues/yyyy=2026/mm=03/dd=30/"
repo_metadata_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/repo_metadata/"
issues_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/issues/"
pulls_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/pulls/"


bronze_issues_schema = StructType([
    StructField("owner", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("endpoint", StringType(), True),

    StructField("params", StructType([
        StructField("per_page", LongType(), True),
        StructField("page", LongType(), True),
        StructField("state", StringType(), True),
        StructField("since", StringType(), True),
    ]), True),

    StructField("url", StringType(), True),
    StructField("count", LongType(), True),

    StructField("data", ArrayType(
        StructType([
            StructField("url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("id", LongType(), True),
            StructField("node_id", StringType(), True),
            StructField("number", LongType(), True),
            StructField("title", StringType(), True),

            StructField("user", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),

            StructField("state", StringType(), True),
            StructField("locked", BooleanType(), True),
            StructField("comments", LongType(), True),
            StructField("created_at", StringType(), True),
            StructField("updated_at", StringType(), True),
            StructField("closed_at", StringType(), True),
            StructField("author_association", StringType(), True),
            StructField("draft", BooleanType(), True),
            StructField("body", StringType(), True),
            StructField("state_reason", StringType(), True),

            StructField("closed_by", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("type", StringType(), True),
            ]), True),

            StructField("pull_request", StructType([
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("diff_url", StringType(), True),
                StructField("patch_url", StringType(), True),
                StructField("merged_at", StringType(), True),
            ]), True),

            StructField("reactions", StructType([
                StructField("total_count", LongType(), True),
                StructField("+1", LongType(), True),
                StructField("-1", LongType(), True),
                StructField("laugh", LongType(), True),
                StructField("hooray", LongType(), True),
                StructField("confused", LongType(), True),
                StructField("heart", LongType(), True),
                StructField("rocket", LongType(), True),
                StructField("eyes", LongType(), True),
            ]), True),
        ])
    ), True)
])


def create_spark_session():
    return (
        SparkSession.builder
        .appName("issues_prs_transform")
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.hadoop:hadoop-aws:3.4.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                "io.delta:delta-spark_4.1_2.13:4.1.0"
            ])
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        
        .getOrCreate()
    )


def read_bronze_data(spark):
    logger.info(f"Reading bronze issues data from {bronze_path}")
    return (
        spark.read
        .option("recursiveFileLookup", "true")
        .schema(bronze_issues_schema)
        .json(bronze_path)
    )


def read_repo_metadata_silver(spark):
    logger.info(f"Reading silver repo metadata from {repo_metadata_silver_path}")
    return spark.read.format("delta").load(repo_metadata_silver_path)


def transform_issues_and_prs(bronze_df, run_ts):
    base_df = (
        bronze_df
        .withColumn("issue_record", F.explode("data"))
        .select(
            F.to_timestamp("fetched_at").alias("bronze_ingested_at"),
            F.col("fetched_at"),
            F.concat_ws("/", F.col("owner"), F.col("repo")).alias("repo_full_name"),

            F.col("issue_record.url").alias("api_url"),
            F.col("issue_record.html_url").alias("html_url"),
            F.col("issue_record.id").cast("long").alias("issue_id"),
            F.col("issue_record.number").cast("long").alias("issue_number"),
            F.col("issue_record.title").alias("issue_title"),

            F.col("issue_record.user.id").cast("long").alias("user_id"),
            F.col("issue_record.user.login").alias("user_login"),
            F.col("issue_record.user.type").alias("user_type"),
            (F.col("issue_record.user.type") == "Bot").alias("is_bot"),

            F.col("issue_record.state").alias("issue_state"),
            F.to_timestamp("issue_record.created_at").alias("created_at"),
            F.to_timestamp("issue_record.updated_at").alias("updated_at"),
            F.to_timestamp("issue_record.closed_at").alias("closed_at"),

            F.col("issue_record.author_association").alias("author_type"),
            F.col("issue_record.closed_by.id").cast("long").alias("closed_by_user_id"),
            F.col("issue_record.closed_by.login").alias("closed_by_login"),

            F.col("issue_record.comments").cast("long").alias("comments_count"),
            F.col("issue_record.body").alias("body"),
            F.col("issue_record.state_reason").alias("state_reason"),

            F.col("issue_record.pull_request.url").alias("pull_request_api_url"),
            F.col("issue_record.pull_request.html_url").alias("pull_request_html_url"),
            F.to_timestamp("issue_record.pull_request.merged_at").alias("pull_request_merged_at"),
            F.col("issue_record.pull_request").isNotNull().alias("is_pull_request"),

            F.col("issue_record.reactions.total_count").cast("long").alias("reactions_total_count"),
        )
        .withColumn("silver_ingested_at", F.lit(run_ts).cast("timestamp"))
        .withColumn("snapshot_date", F.to_date("bronze_ingested_at"))
    )

    issues_df = base_df.filter(~F.col("is_pull_request"))
    pulls_df = base_df.filter(F.col("is_pull_request"))

    return issues_df, pulls_df


def rename_pull_columns(pulls_df):
    return (
        pulls_df
        .withColumnRenamed("issue_id", "pull_id")
        .withColumnRenamed("issue_number", "pull_number")
        .withColumnRenamed("issue_title", "pull_title")
        .withColumnRenamed("issue_state", "pull_state")
    )


def dedupe_issue_like_df(df, id_col):
    window_spec = (
        Window
        .partitionBy("repo_full_name", id_col, "snapshot_date")
        .orderBy(F.col("bronze_ingested_at").desc())
    )

    return (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def enrich_with_repo_id(df, repo_metadata_df):
    repo_lookup_df = (
        repo_metadata_df
        .select("repo_id", "repo_full_name")
        .dropDuplicates(["repo_full_name"])
    )

    return (
        df.alias("x")
        .join(
            repo_lookup_df.alias("r"),
            on=F.col("x.repo_full_name") == F.col("r.repo_full_name"),
            how="left"
        )
        .select(
            F.col("x.*"),
            F.col("r.repo_id")
        )
    )


def run_data_quality_checks(df, job_run_id, table_name, id_col):
    row_count = df.count()
    logger.info(f"job_run_id={job_run_id} table={table_name} row_count_pre_write={row_count}")

    if row_count == 0:
        logger.warning(f"job_run_id={job_run_id} table={table_name} is empty.")
        return row_count

    required_columns = [id_col, "repo_full_name", "html_url", "snapshot_date"]

    for col_name in required_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        logger.info(f"job_run_id={job_run_id} table={table_name} null_count_{col_name}={null_count}")

        if null_count > 0:
            raise ValueError(
                f"Data quality failed: table={table_name}, column={col_name}, null_count={null_count}"
            )

    duplicate_count = (
        df.groupBy("repo_full_name", id_col, "snapshot_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    logger.info(f"job_run_id={job_run_id} table={table_name} duplicate_groups={duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(
            f"Data quality failed: table={table_name}, duplicate_groups={duplicate_count}"
        )

    missing_repo_id_count = df.filter(F.col("repo_id").isNull()).count()
    logger.info(f"job_run_id={job_run_id} table={table_name} missing_repo_id_count={missing_repo_id_count}")

    return row_count


def write_delta(df, path, table_name, process_date):
    logger.info(f"Writing {table_name} silver data to {path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{process_date}'")
        .partitionBy("snapshot_date")
        .save(path)
    )


def main():
    spark = None
    job_run_id = str(uuid.uuid4())
    job_start_time = datetime.now(timezone.utc)

    logger.info(
        f"job_run_id={job_run_id} status=STARTED "
        f"start_time={job_start_time.isoformat()}"
    )

    try:
        spark = create_spark_session()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        bronze_df = read_bronze_data(spark)
        repo_metadata_df = read_repo_metadata_silver(spark)

        bronze_row_count = bronze_df.count()
        logger.info(f"job_run_id={job_run_id} bronze_row_count={bronze_row_count}")

        run_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

        issues_df, pulls_df = transform_issues_and_prs(bronze_df, run_ts)

        issues_df = dedupe_issue_like_df(issues_df, "issue_id")
        pulls_df = rename_pull_columns(pulls_df)
        pulls_df = dedupe_issue_like_df(pulls_df, "pull_id")

        issues_df = enrich_with_repo_id(issues_df, repo_metadata_df)
        pulls_df = enrich_with_repo_id(pulls_df, repo_metadata_df)

        issues_count = run_data_quality_checks(
            issues_df, job_run_id, "issues", "issue_id"
        )
        pulls_count = run_data_quality_checks(
            pulls_df, job_run_id, "pulls", "pull_id"
        )

        write_delta(issues_df, issues_silver_path, "issues")
        write_delta(pulls_df, pulls_silver_path, "pulls")

        job_end_time = datetime.now(timezone.utc)

        logger.info(
            f"job_run_id={job_run_id} status=SUCCESS "
            f"start_time={job_start_time.isoformat()} "
            f"end_time={job_end_time.isoformat()} "
            f"bronze_row_count={bronze_row_count} "
            f"issues_row_count_written={issues_count} "
            f"pulls_row_count_written={pulls_count}"
        )

    except Exception as e:
        job_end_time = datetime.now(timezone.utc)
        logger.exception(
            f"job_run_id={job_run_id} status=FAILED "
            f"start_time={job_start_time.isoformat()} "
            f"end_time={job_end_time.isoformat()} "
            f"error={str(e)}"
        )
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()