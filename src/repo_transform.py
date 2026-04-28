import os
import logging
import pyspark.sql.functions as F
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    ArrayType, MapType
)


# expected bronze json schema for repo metadata endpoint
bronze_repo_metadata_schema = StructType([
    StructField("owner", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("endpoint", StringType(), True),

    StructField("data", StructType([
        StructField("id", LongType(), True),
        StructField("node_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("private", BooleanType(), True),

        StructField("owner", StructType([
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
            StructField("site_admin", BooleanType(), True)
        ]), True),

        StructField("html_url", StringType(), True),
        StructField("description", StringType(), True),
        StructField("fork", BooleanType(), True),
        StructField("url", StringType(), True),
        StructField("forks_url", StringType(), True),
        StructField("keys_url", StringType(), True),
        StructField("collaborators_url", StringType(), True),
        StructField("teams_url", StringType(), True),
        StructField("hooks_url", StringType(), True),
        StructField("issue_events_url", StringType(), True),
        StructField("events_url", StringType(), True),
        StructField("assignees_url", StringType(), True),
        StructField("branches_url", StringType(), True),
        StructField("tags_url", StringType(), True),
        StructField("blobs_url", StringType(), True),
        StructField("git_tags_url", StringType(), True),
        StructField("git_refs_url", StringType(), True),
        StructField("trees_url", StringType(), True),
        StructField("statuses_url", StringType(), True),
        StructField("languages_url", StringType(), True),
        StructField("stargazers_url", StringType(), True),
        StructField("contributors_url", StringType(), True),
        StructField("subscribers_url", StringType(), True),
        StructField("subscription_url", StringType(), True),
        StructField("commits_url", StringType(), True),
        StructField("git_commits_url", StringType(), True),
        StructField("comments_url", StringType(), True),
        StructField("issue_comment_url", StringType(), True),
        StructField("contents_url", StringType(), True),
        StructField("compare_url", StringType(), True),
        StructField("merges_url", StringType(), True),
        StructField("archive_url", StringType(), True),
        StructField("downloads_url", StringType(), True),
        StructField("issues_url", StringType(), True),
        StructField("pulls_url", StringType(), True),
        StructField("milestones_url", StringType(), True),
        StructField("notifications_url", StringType(), True),
        StructField("labels_url", StringType(), True),
        StructField("releases_url", StringType(), True),
        StructField("deployments_url", StringType(), True),

        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("pushed_at", StringType(), True),

        StructField("git_url", StringType(), True),
        StructField("ssh_url", StringType(), True),
        StructField("clone_url", StringType(), True),
        StructField("svn_url", StringType(), True),
        StructField("homepage", StringType(), True),

        StructField("size", LongType(), True),
        StructField("stargazers_count", LongType(), True),
        StructField("watchers_count", LongType(), True),
        StructField("language", StringType(), True),

        StructField("has_issues", BooleanType(), True),
        StructField("has_projects", BooleanType(), True),
        StructField("has_downloads", BooleanType(), True),
        StructField("has_wiki", BooleanType(), True),
        StructField("has_pages", BooleanType(), True),
        StructField("has_discussions", BooleanType(), True),

        StructField("forks_count", LongType(), True),
        StructField("mirror_url", StringType(), True),
        StructField("archived", BooleanType(), True),
        StructField("disabled", BooleanType(), True),
        StructField("open_issues_count", LongType(), True),

        StructField("license", StructType([
            StructField("key", StringType(), True),
            StructField("name", StringType(), True),
            StructField("spdx_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("node_id", StringType(), True)
        ]), True),

        StructField("allow_forking", BooleanType(), True),
        StructField("is_template", BooleanType(), True),
        StructField("web_commit_signoff_required", BooleanType(), True),
        StructField("has_pull_requests", BooleanType(), True),
        StructField("pull_request_creation_policy", StringType(), True),
        StructField("topics", ArrayType(StringType()), True),
        StructField("visibility", StringType(), True),
        StructField("forks", LongType(), True),
        StructField("open_issues", LongType(), True),
        StructField("watchers", LongType(), True),
        StructField("default_branch", StringType(), True),

        StructField("permissions", StructType([
            StructField("admin", BooleanType(), True),
            StructField("maintain", BooleanType(), True),
            StructField("push", BooleanType(), True),
            StructField("triage", BooleanType(), True),
            StructField("pull", BooleanType(), True)
        ]), True),

        StructField("custom_properties", MapType(StringType(), StringType()), True),

        StructField("organization", StructType([
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
            StructField("site_admin", BooleanType(), True)
        ]), True),

        StructField("network_count", LongType(), True),
        StructField("subscribers_count", LongType(), True)
    ]), True)
])

# load env vars
load_dotenv()  
S3_BUCKET = os.getenv("S3_BUCKET")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
SILVER_PREFIX = os.getenv("SILVER_PREFIX")

# fail fast if env vars are missing
if not all([S3_BUCKET, BRONZE_PREFIX, SILVER_PREFIX]):
    raise ValueError("Missing required environment variables.")

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# s3a is the spark connector for s3, make sure hadoop-aws and aws-java-sdk dependencies are included in your Spark setup
# bronze path is partitioned by date eg: .../repo_metadata/apache/airflow/yyyy=2026/mm=04/dd=06/sthsthsthsthsth.json
bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/repo_metadata/*/*/yyyy=2026/mm=04/dd=06/" # with wildcards characters
silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/repo_metadata/"

'''
bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/repo_metadata/"
# looks for all json file under path recursively
# if recursiveFileLookup set to true, do not provide filename pattern with wildcards (eg *.json) 
# otherwise it will not read files in subdirectories
# for now yes, coz i had both flat files and partiitons 
return (
    spark.read
    .option("recursiveFileLookup", "true")
    .json(bronze_path)
    )
'''
'''
# with wildcards characters 
bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/repo_metadata/*/*/yyyy=2026/mm=04/dd=06/"
If u want today's date
today = datetime.now(timezone.utc)
bronze_path = (
    f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/repo_metadata/*/*/"
    f"yyyy={today.year}/mm={today.month:02d}/dd={today.day:02d}/"
)
'''

# if we used year=2026/month=04/day=06/hour=12
# sprak auto-parses these as columns
def read_bronze_data(spark):
    # read bronze data with explicit schema for stability
    logger.info(f"Reading bronze data from {bronze_path}")
    # recurisve mode disables inferring yyyy,mm,dd as partition columns
    # but we have fetched_at in the json to derive snapshot date
    return(
        spark.read
        .option("recursiveFileLookup", "true")
        .schema(bronze_repo_metadata_schema)
        .json(bronze_path)
        )

def transform_repo_metadata(bronze_df, run_ts):
    return (
        bronze_df
        .select(
            F.col("data.id").alias("repo_id"),
            F.col("data.full_name").alias("repo_full_name"),
            F.col("data.html_url").alias("repo_url"),
            F.col("data.description").alias("description"),

            F.col("data.owner.id").alias("owner_id"),
            F.col("data.owner.login").alias("owner_login"),
            
            F.to_timestamp("data.created_at").alias("created_at"),
            F.to_timestamp("data.updated_at").alias("updated_at"),
            F.to_timestamp("data.pushed_at").alias("pushed_at"),

            F.col("data.homepage").alias("homepage_url"),
            F.col("data.size").cast("long").alias("size_kb"),
            F.col("data.language").alias("main_language"),
            
            F.col("data.organization.id").alias("org_id"),
            F.col("data.network_count").cast("long").alias("network_count"),
            F.col("data.open_issues_count").cast("long").alias("open_issues_count"),
            F.col("data.stargazers_count").cast("long").alias("stargazers_count"),
            F.col("data.subscribers_count").cast("long").alias("subscribers_count"),

            F.to_timestamp("fetched_at").alias("bronze_ingested_at")
        )
        .withColumn("silver_ingested_at", F.lit(run_ts).cast("timestamp"))
        .withColumn("snapshot_date", F.to_date("bronze_ingested_at"))

    )


def dedupe(df):
        
    window_spec = (
    Window
    .partitionBy("repo_id", "snapshot_date")
    .orderBy(F.col("bronze_ingested_at").desc())
    )

    deduped_df = (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    return deduped_df
    
def write_silver_data(df):
    # overwrite by snapshot date if run multiple times a day 
    logger.info(f"Writing silver data to {silver_path}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("snapshot_date")
        .format("parquet")
        .save(silver_path)
    )
    # when u partition by snapshot_date, it will create subdirectories like snapshot_date=2026-04-06/ and put the parquet files there.
    # no snapshot_date column in parquet, but when it's read back, snapshot_date will be a column again 

def run_data_quality_checks(df, job_run_id):
    # check if silver DataFrame is empty
    silver_row_count = df.count()
    logger.info(f"job_run_id={job_run_id} silver_row_count_pre_write={silver_row_count}")

    if silver_row_count == 0:
        raise ValueError("Data quality check failed: silver DataFrame is empty.")

    # check critical columns for nulls
    required_columns = ["repo_id", "repo_full_name", "repo_url", "snapshot_date"]
    for col_name in required_columns:
        null_count = df.filter(F.col(col_name).isNull()).count()
        logger.info(f"job_run_id={job_run_id} null_count_{col_name}={null_count}")

        if null_count > 0:
            raise ValueError(
                f"Data quality check failed: column {col_name} has {null_count} null values."
            )

    # check numeric metrics for negative values 
    negative_metric_count = (
        df.filter(
            (F.col("size_kb") < 0) |
            (F.col("network_count") < 0) |
            (F.col("open_issues_count") < 0) |
            (F.col("stargazers_count") < 0) |
            (F.col("subscribers_count") < 0)
        ).count()
    )

    logger.info(f"job_run_id={job_run_id} negative_metric_row_count={negative_metric_count}")

    if negative_metric_count > 0:
        raise ValueError(
            f"Data quality check failed: found {negative_metric_count} rows with negative numeric metrics."
        )

    return silver_row_count

def main(): 
    spark = None
    job_run_id = str(uuid.uuid4())
    job_start_time = datetime.now(timezone.utc)
    job_status = "STARTED"
    logger.info(f"job_run_id={job_run_id} status={job_status} start_time={job_start_time.isoformat()}")
    try:
        # spark = SparkSession.builder.appName("repo_metadata_transform").getOrCreate()
        spark = (
            SparkSession.builder
            .appName("repo_metadata_transform")
            .config(
                "spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.262"
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
            )
            .getOrCreate()
        )
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        bronze_df = read_bronze_data(spark)

        # log bronze row count for monitoring
        bronze_row_count = bronze_df.count()
        logger.info(f"job_run_id={job_run_id} bronze_row_count={bronze_row_count}")

        # timestamp for silver run 
        run_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]

        silver_repo_metadata_df = transform_repo_metadata(bronze_df, run_ts)

        silver_repo_metadata_df_deduped = dedupe(silver_repo_metadata_df)

        silver_row_count = run_data_quality_checks(silver_repo_metadata_df_deduped, job_run_id)

        write_silver_data(silver_repo_metadata_df_deduped)

        job_end_time = datetime.now(timezone.utc)
        job_status = "SUCCESS"

        logger.info(
            f"job_run_id={job_run_id} status={job_status} "
            f"start_time={job_start_time.isoformat()} "
            f"end_time={job_end_time.isoformat()} "
            f"bronze_row_count={bronze_row_count} "
            f"silver_row_count_written={silver_row_count}"
        )

        logger.info("Repo metadata transform completed successfully.")
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



'''
# if u want only latest record per repo per day 
from pyspark.sql.window import Window
import pyspark.sql.functions as F

window_spec = Window.partitionBy("repo_id", "snapshot_date").orderBy(F.col("bronze_ingested_at").desc())

daily_latest_df = (
    silver_df
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
)'''