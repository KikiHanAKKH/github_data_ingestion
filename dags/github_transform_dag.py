import os
from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Param, dag, get_current_context, task

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

# Shared across all three transform jobs — kept in one place here instead of
# duplicated inside each script's SparkSession builder, since spark-submit
# configures the JVM/classpath before the script even starts.
SPARK_PACKAGES = ",".join([
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "io.delta:delta-spark_2.13:4.0.0",
])
SPARK_CONF = {
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.sources.partitionOverwriteMode": "dynamic",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


@dag(
    dag_id="github_silver_transforms",
    start_date=datetime(2026, 3, 19),
    schedule=None,  # triggered by github_bronze_extracts on success; still triggerable on demand
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["github", "silver", "delta", "transforms"],
    params={
        "process_date": Param(None, type=["string", "null"]),  # optional override, defaults to the run's ds
        "owner": Param(None, type=["string", "null"]),          # optional: scope commits/issues to a single repo
        "repo": Param(None, type=["string", "null"]),
    },
)
def github_silver_transforms():

    @task
    def build_args():
        context = get_current_context()
        params = context["params"]
        process_date = params.get("process_date") or context["ds"]
        owner = params.get("owner")
        repo = params.get("repo")

        args = ["--process-date", process_date]
        if owner and repo:
            args += ["--owner", owner, "--repo", repo]
        elif owner or repo:
            raise ValueError("Provide both 'owner' and 'repo', or neither.")

        return args

    args = build_args()

    transform_repo_metadata = SparkSubmitOperator(
        task_id="transform_repo_metadata",
        application=os.path.join(SRC_DIR, "repo_transform.py"),
        application_args=args,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        conn_id="spark_default",
    )

    transform_commits = SparkSubmitOperator(
        task_id="transform_commits",
        application=os.path.join(SRC_DIR, "commits_transform.py"),
        application_args=args,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        conn_id="spark_default",
    )

    transform_issues = SparkSubmitOperator(
        task_id="transform_issues",
        application=os.path.join(SRC_DIR, "issues_transform.py"),
        application_args=args,
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        conn_id="spark_default",
    )

    transform_repo_metadata >> [transform_commits, transform_issues]


dag = github_silver_transforms()