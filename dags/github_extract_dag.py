from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag, get_current_context, task
from airflow.utils.types import DagRunType

from src.extract import (
    load_repos,
    s3_checkpoint_write,
    upload_repo_metadata,
    fetch_paginated_and_upload_sequential,
    fetch_paginated_and_upload_parallel,
    REPO_JSON_PATH,
)


@dag(
    dag_id="github_bronze_extracts",
    start_date=datetime(2026, 3, 19),
    schedule="0 2 * * 0",  # every Sunday at 2 AM
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["github", "bronze", "extract", "dynamic-mapping"],
    params={
        "owner": Param(None, type=["string", "null"]),  # optional: scope to a single repo
        "repo": Param(None, type=["string", "null"]),
    },
)
def github_bronze_extracts():

    @task
    def get_repos():
        context = get_current_context()
        params = context["params"]
        owner = params.get("owner")
        repo = params.get("repo")

        repos = load_repos(REPO_JSON_PATH)

        if owner and repo:
            repos = [r for r in repos if r["owner"] == owner and r["repo"] == repo]
            if not repos:
                raise ValueError(f"No repo {owner}/{repo} found in repos.json")
        elif owner or repo:
            raise ValueError("Provide both 'owner' and 'repo', or neither.")

        return repos

    @task
    def process_repo(repo_info: dict):
        context = get_current_context()
        run_timestamp = context["data_interval_end"]
        is_scheduled = context["dag_run"].run_type == DagRunType.SCHEDULED

        owner = repo_info["owner"]
        repo = repo_info["repo"]

        # Airflow's own interval is the source of truth for every run type
        # (scheduled, backfill, manual) — since/until always reflect it.
        since_override = context["data_interval_start"].isoformat()
        until_override = context["data_interval_end"].isoformat()
        write_checkpoint = is_scheduled

        print(f"Processing {owner}/{repo}... (run_type={context['dag_run'].run_type})")

        upload_repo_metadata(owner, repo, run_timestamp)

        fetch_paginated_and_upload_sequential(
            owner=owner,
            repo=repo,
            endpoint="issues",
            incremental=True,
            run_timestamp=run_timestamp,
            since_override=since_override,
        )

        if write_checkpoint:
            s3_checkpoint_write(
                owner,
                repo,
                endpoint="issues",
                iso_ts=run_timestamp
            )

        fetch_paginated_and_upload_parallel(
            owner=owner,
            repo=repo,
            endpoint="commits",
            incremental=True,
            run_timestamp=run_timestamp,
            since_override=since_override,
            until_override=until_override,
        )

        if write_checkpoint:
            s3_checkpoint_write(
                owner,
                repo,
                endpoint="commits",
                iso_ts=run_timestamp
            )

        return {"owner": owner, "repo": repo, "status": "done", "run_type": str(context["dag_run"].run_type)}

    @task
    def build_silver_conf():
        context = get_current_context()
        return {
            "process_date": context["data_interval_end"].strftime("%Y-%m-%d"),
            "owner": context["params"].get("owner"),
            "repo": context["params"].get("repo"),
        }

    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_silver_transforms",
        trigger_dag_id="github_silver_transforms",
        conf=build_silver_conf(),
        wait_for_completion=False,
    )

    repos = get_repos()
    results = process_repo.expand(repo_info=repos)
    results >> trigger_silver


dag = github_bronze_extracts()