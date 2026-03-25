from datetime import datetime, timedelta

from airflow.sdk import dag, task

from src.extract import (
    load_repos,
    upload_repo_metadata,
    fetch_paginated_and_upload_sequential,
    fetch_paginated_and_upload_parallel,
    REPO_JSON_PATH,
)


@dag(
    dag_id="github_weekly_ingestion_taskflow",
    start_date=datetime(2026, 3, 19),
    schedule="0 2 * * 0",  # every Sunday at 2 AM
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["github", "bronze", "taskflow", "dynamic-mapping"],
)
def github_weekly_ingestion_taskflow():

    @task
    def get_repos():
        return load_repos(REPO_JSON_PATH)

    @task
    def process_repo(repo_info: dict):
        owner = repo_info["owner"]
        repo = repo_info["repo"]

        print(f"Processing {owner}/{repo}...")

        upload_repo_metadata(owner, repo)

        fetch_paginated_and_upload_sequential(
            owner=owner,
            repo=repo,
            endpoint="issues",
            incremental=True,
        )

        fetch_paginated_and_upload_parallel(
            owner=owner,
            repo=repo,
            endpoint="commits",
            incremental=True,
        )

        return {"owner": owner, "repo": repo, "status": "done"}

    repos = get_repos()
    process_repo.expand(repo_info=repos)


dag = github_weekly_ingestion_taskflow()