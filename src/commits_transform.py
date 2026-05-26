from datetime import datetime, timezone
import os
import logging
import pyspark.sql.functions as F
import uuid

from pyspark.sql.window import Window
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    explode,
    to_timestamp,
    to_date,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
)

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    ArrayType, MapType
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable


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


bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/raw_apache_airflow_commits/yyyy=2026/mm=03/dd=30/"
silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/commits/"
repo_metadata_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/repo_metadata/"


'''
bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/repo_metadata/*/*/yyyy=2026/mm=03/dd=30/" # with wildcards characters
silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/repo_metadata/"
'''

bronze_commit_schema = StructType([
    StructField("owner", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("endpoint", StringType(), True),

    StructField("params", StructType([
        StructField("per_page", LongType(), True),
        StructField("page", LongType(), True),
        StructField("since", StringType(), True),
    ]), True),

    StructField("url", StringType(), True),
    StructField("count", LongType(), True),

    StructField("data", ArrayType(
        StructType([
            StructField("sha", StringType(), True),
            StructField("node_id", StringType(), True),

            StructField("commit", StructType([
                StructField("author", StructType([
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("date", StringType(), True),
                ]), True),

                StructField("committer", StructType([
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True),
                    StructField("date", StringType(), True),
                ]), True),

                StructField("message", StringType(), True),

                StructField("tree", StructType([
                    StructField("sha", StringType(), True),
                    StructField("url", StringType(), True),
                ]), True),

                StructField("url", StringType(), True),
                StructField("comment_count", LongType(), True),

                StructField("verification", StructType([
                    StructField("verified", BooleanType(), True),
                    StructField("reason", StringType(), True),
                    StructField("signature", StringType(), True),
                    StructField("payload", StringType(), True),
                    StructField("verified_at", StringType(), True),
                ]), True),
            ]), True),

            StructField("url", StringType(), True),
            StructField("html_url", StringType(), True),
            StructField("comments_url", StringType(), True),

            StructField("author", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("node_id", StringType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("user_view_type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),

            StructField("committer", StructType([
                StructField("login", StringType(), True),
                StructField("id", LongType(), True),
                StructField("node_id", StringType(), True),
                StructField("avatar_url", StringType(), True),
                StructField("gravatar_id", StringType(), True),
                StructField("url", StringType(), True),
                StructField("html_url", StringType(), True),
                StructField("followers_url", StringType(), True),
                StructField("following_url", StringType(), True),
                StructField("gists_url", StringType(), True),
                StructField("starred_url", StringType(), True),
                StructField("subscriptions_url", StringType(), True),
                StructField("organizations_url", StringType(), True),
                StructField("repos_url", StringType(), True),
                StructField("events_url", StringType(), True),
                StructField("received_events_url", StringType(), True),
                StructField("type", StringType(), True),
                StructField("user_view_type", StringType(), True),
                StructField("site_admin", BooleanType(), True),
            ]), True),

            StructField("parents", ArrayType(
                StructType([
                    StructField("sha", StringType(), True),
                    StructField("url", StringType(), True),
                    StructField("html_url", StringType(), True),
                ])
            ), True),
        ])
    ), True)
])





def read_bronze_data(spark):
    logger.info(f"Reading bronze commits data from {bronze_path}")
    return(
        spark.read
        .option("recursiveFileLookup", "true")
        .schema(bronze_commit_schema)
        .json(bronze_path)
        )

def read_repo_metadata_silver(spark):
    logger.info(f"Reading silver repo metadata from {repo_metadata_silver_path}")

    return (
        spark.read
        .format("delta")
        .load(repo_metadata_silver_path)
    )


def transform_commits(bronze_df, run_ts):
    return (
        bronze_df
        # .filter(F.col("endpoint") == "commits")
        .withColumn("commit_record", F.explode("data"))
        .select(
            F.col("commit_record.sha").alias("commit_id"),
            F.col("commit_record.commit.message").alias("message"),
            F.col("commit_record.html_url").alias("url"),
            F.col("commit_record.author.id").cast("long").alias("user_id"),
            F.to_timestamp("fetched_at").alias("bronze_ingested_at"),
            F.to_timestamp("commit_record.commit.author.date").alias("committed_at"),
            F.concat_ws("/", F.col("owner"), F.col("repo")).alias("repo_full_name"),
            F.col("commit_record.commit.author.name").alias("git_author_name"),
            F.col("commit_record.commit.author.email").alias("git_author_email"),
            F.col("commit_record.author.login").alias("author_login")
        )
        .withColumn("silver_ingested_at", F.lit(run_ts).cast("timestamp"))
        .withColumn("snapshot_date", F.to_date("bronze_ingested_at"))
    )




# .dropDuplicates(["repo_full_name", "commit_id"])
# just this u cannot choose which row to keep, thus below 

# here we also include snapshot_date in deduping coz there can be updates to the same commit over time 
def dedupe_commits(df):
    window_spec = (
        Window
        .partitionBy("repo_full_name", "commit_id","snapshot_date")
        .orderBy(F.col("bronze_ingested_at").desc())
    )

    return (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )




def enrich_with_repo_id(commits_df, repo_metadata_df):
    repo_lookup_df = (
        repo_metadata_df
        .select(
            F.col("repo_id"),
            F.col("repo_full_name"),
            F.col("snapshot_date")
        )
        .dropDuplicates(["repo_full_name", "snapshot_date"])
    # in silver metadata table, there shouldn't be any dupes but this is just a nice measure 
    )

    enriched_df = (
        commits_df.alias("c")
        .join(
            repo_lookup_df.alias("r"),
            on=[
                F.col("c.repo_full_name") == F.col("r.repo_full_name"),
                F.col("c.snapshot_date") == F.col("r.snapshot_date"),
            ],
            how="left"
        )
        .select(
            F.col("c.*"),
            F.col("r.repo_id")
        )
    )

    return enriched_df


def run_data_quality_checks(df, job_run_id):
    silver_row_count = df.count()
    logger.info(f"job_run_id={job_run_id} silver_row_count_pre_write={silver_row_count}")

    if silver_row_count == 0:
        raise ValueError("Data quality check failed: silver commits DataFrame is empty.")

    required_columns = ["commit_id", "repo_full_name", "url", "snapshot_date"]

    for col_name in required_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        logger.info(f"job_run_id={job_run_id} null_count_{col_name}={null_count}")

        if null_count > 0:
            raise ValueError(
                f"Data quality check failed: column {col_name} has {null_count} null values."
            )

    duplicate_count = (
        df.groupBy("repo_full_name", "commit_id","snapshot_date")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    logger.info(f"job_run_id={job_run_id} duplicate_commit_groups={duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(
            f"Data quality check failed: found {duplicate_count} duplicate commit groups."
        )

    missing_repo_id_count = df.filter(F.col("repo_id").isNull()).count()
    logger.info(f"job_run_id={job_run_id} missing_repo_id_count={missing_repo_id_count}")

    return silver_row_count


def write_silver_data(df):
    logger.info(f"Writing silver commits data to {silver_path}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .format("delta")
        .save(silver_path)
    )


def main():
    spark = None
    job_run_id = str(uuid.uuid4())
    job_start_time = datetime.now(timezone.utc)
    job_status = "STARTED"

    logger.info(
        f"job_run_id={job_run_id} status={job_status} "
        f"start_time={job_start_time.isoformat()}"
    )
    try:
        
        spark = (
            SparkSession.builder
            .appName("repo_metadata_transform")

            # Required packages
            .config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.hadoop:hadoop-aws:3.4.1",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                    "io.delta:delta-spark_4.1_2.13:4.1.0"
                ])
            )

            # S3A filesystem
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )

            # AWS credential provider chain
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )

            # Delta Lake
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension"
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )

            .getOrCreate()
        )
        

        # spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")



        bronze_df = read_bronze_data(spark)
        repo_metadata_df = read_repo_metadata_silver(spark)

        bronze_row_count = bronze_df.count()
        logger.info(f"job_run_id={job_run_id} bronze_file_count={bronze_row_count}")

        run_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

        silver_commits_df = transform_commits(bronze_df, run_ts)
        silver_commits_df_deduped = dedupe_commits(silver_commits_df)
        silver_commits_df_enriched = enrich_with_repo_id(
            silver_commits_df_deduped,
            repo_metadata_df
        )

        silver_row_count = run_data_quality_checks(
            silver_commits_df_enriched,
            job_run_id
        )

        write_silver_data(silver_commits_df_enriched)

        job_end_time = datetime.now(timezone.utc)
        job_status = "SUCCESS"

        logger.info(
            f"job_run_id={job_run_id} status={job_status} "
            f"start_time={job_start_time.isoformat()} "
            f"end_time={job_end_time.isoformat()} "
            f"bronze_file_count={bronze_row_count} "
            f"silver_row_count_written={silver_row_count}"
        )

        logger.info("Commits transform completed successfully.")

    except Exception as e:
        job_end_time = datetime.now(timezone.utc)
        job_status = "FAILED"

        logger.exception(
            f"job_run_id={job_run_id} status={job_status} "
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


