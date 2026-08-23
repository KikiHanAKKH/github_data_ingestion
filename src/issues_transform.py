from datetime import datetime, timezone
import os
import logging
import uuid
import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import StorageLevel
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, BooleanType,
    ArrayType
)
from delta.tables import DeltaTable

from dotenv import load_dotenv


if os.getenv("ENV", "dev") != "prod":
    load_dotenv()


S3_BUCKET = os.getenv("S3_BUCKET")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")
SILVER_PREFIX = os.getenv("SILVER_PREFIX")

if not all([S3_BUCKET, BRONZE_PREFIX, SILVER_PREFIX]):
    raise ValueError("Missing required environment variables.")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from datetime import datetime


def build_bronze_path(
    process_date: str,
    owner: str | None = None,
    repo: str | None = None,
) -> str:
    dt = datetime.strptime(process_date, "%Y-%m-%d")

    date_path = (
        f"yyyy={dt.year}/"
        f"mm={dt.month:02d}/"
        f"dd={dt.day:02d}/"
    )

    if owner and repo:
        # One specific repo for one date
        return (
            f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/issues/"
            f"{owner}/{repo}/{date_path}"
        )

    if owner or repo:
        raise ValueError("Provide both --owner and --repo, or neither.")

    # All owners and repos for one date
    return (
        f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/issues/"
        f"*/*/{date_path}"
    )


# bronze_path = f"s3a://{S3_BUCKET}/{BRONZE_PREFIX}/raw_apache_airflow_issues/yyyy=2026/mm=03/dd=30/"
repo_metadata_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/repo_metadata/"
issues_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/issues/"
pulls_silver_path = f"s3a://{S3_BUCKET}/{SILVER_PREFIX}/pulls/"

# SCD2 change-detection columns (post-rename_pull_columns names for pulls).
# Deliberately excludes high-churn fields (comments_count, body) that would
# otherwise create a new version on almost every fetch.
ISSUES_TRACKED_COLUMNS = ["issue_title", "issue_state", "closed_at", "state_reason"]
PULLS_TRACKED_COLUMNS = ["pull_title", "pull_state", "closed_at", "state_reason", "pull_request_merged_at"]


bronze_issues_schema = StructType([
    StructField("owner", StringType(), True),
    StructField("repo", StringType(), True),
    StructField("fetched_at", StringType(), True),
    StructField("batch_timestamp", StringType(), True),
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
    # JAR packages and S3A/Delta config are supplied by spark-submit
    # (see dags/github_transform_dag.py's SPARK_PACKAGES/SPARK_CONF) —
    # nothing to configure here, just attach to the already-configured session.
    return (
        SparkSession.builder
        .appName("issues_prs_transform")
        .getOrCreate()
    )

def read_bronze_data(spark, process_date, owner: str | None = None, repo: str | None = None,):
    bronze_path = build_bronze_path(process_date, owner, repo)
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
            F.to_timestamp("batch_timestamp").alias("bronze_batch_timestamp"),
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
        .withColumn("batch_date", F.to_date("bronze_batch_timestamp"))
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


def dedupe_df(df, id_col):
    window_spec = (
        Window
        .partitionBy("repo_full_name", id_col, "batch_date")
        .orderBy(F.col("bronze_ingested_at").desc())
    )

    return (
        df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def enrich_with_repo_id(commits_df, repo_metadata_df):
    window_spec = (
        Window
        .partitionBy("repo_full_name")
        .orderBy(F.col("batch_date").desc())
    )

    repo_lookup_df = (
        repo_metadata_df
        .select(
            "repo_id",
            "repo_full_name",
            "batch_date"
        )
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .drop("rn", "batch_date")
    )

    return (
        commits_df.alias("c")
        .join(
            F.broadcast(repo_lookup_df).alias("r"),
            on=F.col("c.repo_full_name") == F.col("r.repo_full_name"),
            how="left"
        )
        .select(
            F.col("c.*"),
            F.col("r.repo_id")
        )
    )


def run_data_quality_checks(df, job_run_id, table_name, id_col):
    required_columns = [id_col, "repo_id", "repo_full_name", "html_url", "batch_date"]

    # Single scan of df for row count + null counts (repo_id included, since
    # it's part of the SCD2 entity key — a null repo_id must be a hard failure).
    metrics = (
        df.agg(
            F.count("*").alias("row_count"),

            *[
                F.sum(
                    F.when(F.col(col_name).isNull(), 1).otherwise(0)
                ).alias(f"{col_name}_null_count")
                for col_name in required_columns
            ],
        )
        .first()
    )

    row_count = metrics["row_count"]
    logger.info(f"job_run_id={job_run_id} table={table_name} row_count_pre_write={row_count}")

    if row_count == 0:
        logger.warning(f"job_run_id={job_run_id} table={table_name} is empty.")
        return row_count

    for col_name in required_columns:
        null_count = metrics[f"{col_name}_null_count"]
        logger.info(f"job_run_id={job_run_id} table={table_name} null_count_{col_name}={null_count}")

        if null_count > 0:
            raise ValueError(
                f"Data quality failed: table={table_name}, column={col_name}, null_count={null_count}"
            )

    # Duplicate check needs a groupBy, so it stays a separate aggregation.
    # Entity key is (repo_id, id_col) — dedupe_df already guarantees at most
    # one row per entity within a single run's batch, this just validates it.
    duplicate_count = (
        df.groupBy("repo_id", id_col)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    logger.info(f"job_run_id={job_run_id} table={table_name} duplicate_groups={duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(
            f"Data quality failed: table={table_name}, duplicate_groups={duplicate_count}"
        )

    return row_count




def write_silver_data(spark, source_df, path, id_col, tracked_columns):
    # SCD2: keep every meaningful historical version instead of overwriting
    # in place. Versioned by GitHub's own updated_at (not batch_date,
    # which only reflects when *our pipeline* happened to fetch it), and
    # only a change in `tracked_columns` creates a new version.
    if not DeltaTable.isDeltaTable(spark, path):
        (
            source_df
            .withColumn("valid_from", F.col("updated_at"))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write
            .format("delta")
            .save(path)
        )
        return

    target = DeltaTable.forPath(spark, path)
    current_df = target.toDF().filter(F.col("is_current"))

    compare_cols = ["repo_id", id_col, "updated_at"] + tracked_columns
    joined = (
        source_df.alias("s")
        .join(
            current_df.select(*compare_cols).alias("t"),
            on=[
                F.col("s.repo_id") == F.col("t.repo_id"),
                F.col(f"s.{id_col}") == F.col(f"t.{id_col}"),
            ],
            how="left",
        )
    )
    joined.persist(StorageLevel.MEMORY_AND_DISK)

    is_new_entity = F.col("t.updated_at").isNull()
    is_newer = F.col("s.updated_at") > F.col("t.updated_at")

    has_changed = F.lit(False)
    for col_name in tracked_columns:
        has_changed = has_changed | ~F.col(f"s.{col_name}").eqNullSafe(F.col(f"t.{col_name}"))

    needs_new_version = is_new_entity | (is_newer & has_changed)

    # Step 1: expire current rows being superseded by a newer, changed
    # version. A single MERGE can't both update this matched row AND insert
    # a separate new row from the same source record, so this is a distinct
    # operation from the insert below.
    rows_to_expire = (
        joined
        .filter(~is_new_entity & is_newer & has_changed)
        .select(
            F.col("s.repo_id").alias("repo_id"),
            F.col(f"s.{id_col}").alias(id_col),
            F.col("s.updated_at").alias("new_updated_at"),
        )
    )

    (
        target.alias("target")
        .merge(
            rows_to_expire.alias("src"),
            f"""
            target.repo_id = src.repo_id
            AND target.{id_col} = src.{id_col}
            AND target.is_current = true
            """
        )
        .whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "valid_to": F.col("src.new_updated_at"),
        })
        .execute()
    )

    # Step 2: append new current-version rows — brand-new entities and
    # superseding versions of changed entities. Stale (older updated_at) or
    # unchanged rows are excluded from both filters, so they're simply
    # dropped here — never written at all.
    rows_to_insert = (
        joined
        .filter(needs_new_version)
        .select("s.*")
        .withColumn("valid_from", F.col("updated_at"))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    (
        rows_to_insert.write
        .format("delta")
        .mode("append")
        .save(path)
    )

    joined.unpersist()

'''
def write_silver_data(df, path, table_name, process_date):
    logger.info(f"Writing {table_name} silver data to {path}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"batch_date = '{process_date}'")
        .partitionBy("batch_date")
        .save(path)
    )

from delta.tables import DeltaTable


def merge_silver_data(
    spark,
    df,
    path,
    table_name,
    id_col,
):
    logger.info(f"Merging {table_name} silver data into {path}")

    # First-ever run: Delta table does not exist yet
    if not DeltaTable.isDeltaTable(spark, path):
        logger.info(
            f"{table_name} Delta table does not exist yet. "
            "Creating initial table."
        )

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )

        return

    # Table already exists
    delta_table = DeltaTable.forPath(spark, path)

    (
        delta_table.alias("target")
        .merge(
            df.alias("source"),
            f"""
            target.repo_id = source.repo_id
            AND target.{id_col} = source.{id_col}
            """
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"{table_name} Delta merge completed.")
'''

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--process-date", required=True)
    parser.add_argument("--owner")
    parser.add_argument("--repo")

    args = parser.parse_args()

    process_date = args.process_date
    owner = args.owner
    repo = args.repo

    spark = None
    job_run_id = str(uuid.uuid4())
    job_start_time = datetime.now(timezone.utc)

    logger.info(
        f"job_run_id={job_run_id} status=STARTED "
        f"start_time={job_start_time.isoformat()}"
    )

    try:
        spark = create_spark_session()

        bronze_df = read_bronze_data(spark, process_date,owner, repo)
        repo_metadata_df = read_repo_metadata_silver(spark)

        bronze_row_count = bronze_df.count()
        logger.info(f"job_run_id={job_run_id} bronze_row_count={bronze_row_count}")

        # run_ts = spark.sql("SELECT current_timestamp() AS ts").collect()[0]["ts"]
        
        issues_df, pulls_df = transform_issues_and_prs(bronze_df, job_start_time)

        issues_df = dedupe_df(issues_df, "issue_id")
        pulls_df = rename_pull_columns(pulls_df)
        pulls_df = dedupe_df(pulls_df, "pull_id")

        issues_df = enrich_with_repo_id(issues_df, repo_metadata_df)
        pulls_df = enrich_with_repo_id(pulls_df, repo_metadata_df)
        issues_df.persist(StorageLevel.MEMORY_AND_DISK)
        pulls_df.persist(StorageLevel.MEMORY_AND_DISK)
        # issues_df.explain("formatted")  # TEMP: remove after checking the plan (broadcast join / caching)
        # pulls_df.explain("formatted")  # TEMP: remove after checking the plan (broadcast join / caching)

        issues_count = run_data_quality_checks(
            issues_df, job_run_id, "issues", "issue_id"
        )
        pulls_count = run_data_quality_checks(
            pulls_df, job_run_id, "pulls", "pull_id"
        )

        write_silver_data(spark, issues_df, issues_silver_path, "issue_id", ISSUES_TRACKED_COLUMNS)
        write_silver_data(spark, pulls_df, pulls_silver_path, "pull_id", PULLS_TRACKED_COLUMNS)
        issues_df.unpersist()
        pulls_df.unpersist()

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