import os, requests, json, uuid, boto3
import time
from typing import Optional 
from datetime import datetime, timezone 
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs


# in local dev u should do this, in production env vars are 
from dotenv import load_dotenv
load_dotenv()


S3_BUCKET = os.getenv("S3_BUCKET")        
AWS_REGION = os.getenv("AWS_REGION")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")

REPO_JSON_PATH = os.path.join(
        os.path.dirname(__file__),
        "repos.json"
    )

if not all([S3_BUCKET, AWS_REGION, GITHUB_TOKEN, BRONZE_PREFIX]):
    raise ValueError("Missing required environment variables.")

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_headers() -> dict:
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    return headers



def upload_to_s3(obj_bytes: bytes, key: str) -> None:
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=obj_bytes)



def s3_checkpoint_read(owner: str, repo: str, endpoint: str) -> Optional[str]:
    """
    Read checkpoint (ISO timestamp) from S3. Returns ISO string or None.
    """
    key = f"{BRONZE_PREFIX}/checkpoints/{owner}/{repo}/{endpoint}.json"
    try:
        res = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = res["Body"].read().decode("utf-8")
        obj = json.loads(body)
        return obj.get("last_run",None)
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None




def s3_checkpoint_write(owner: str, repo: str, endpoint: str, iso_ts: str) -> None:
    key = f"{BRONZE_PREFIX}/checkpoints/{owner}/{repo}/{endpoint}.json"
    payload = {"last_run": iso_ts}
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(payload).encode("utf-8"))

def s3_repo_metadata_write(owner: str, repo: str, data: dict, now: datetime) -> None:
    key = (
        f"{BRONZE_PREFIX}/repo_metadata/{owner}/{repo}/"
        f"yyyy={now.year:04d}/mm={now.month:02d}/dd={now.day:02d}/"
        f"hh={now.hour:02d}/repo_metadata_{uuid.uuid4()}.json"
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(data).encode("utf-8")
    )

    print(f"Uploaded repo metadata to s3://{S3_BUCKET}/{key}")



def safe_request(url: str, headers: dict, params=None, retries=3):
    for attempt in range(retries):
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=30
            )

            # Rate limit handling
            if response.status_code in (429, 403):
                remaining = response.headers.get("X-RateLimit-Remaining")
                reset_ts = response.headers.get("X-RateLimit-Reset")

                if remaining == "0" and reset_ts:
                    now_ts = int(time.time())
                    sleep_seconds = max(int(reset_ts) - now_ts + 5, 5)
                    print(f"Rate limit hit. Sleeping for {sleep_seconds} seconds...")
                    time.sleep(sleep_seconds)
                    continue

            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise


def fetch_page_and_upload(owner: str, repo: str, endpoint: str, page: Optional[int] = None, next_url: Optional[str] = None, params: dict = None, incremental: bool = True,since_value: Optional[str] = None) -> dict:
    """
    Fetch one page from GitHub and upload it to S3.

    Returns metadata about the page:
    {
        "page": int,
        "count": int,
        "next_url": str | None,
        "response_url": str
    }
    """
    params = params.copy() if params else {}
    params.setdefault("per_page", 100)
    if page:
        params["page"] = page
    if endpoint == "issues":
        params.setdefault("state", "all")
    if since_value and "since" not in params:
        params["since"] = since_value
    
    if not next_url:
        url = f"https://api.github.com/repos/{owner}/{repo}/{endpoint}"
        response = safe_request(url, headers=get_headers(), params=params)
    else:
        url = next_url
        response = safe_request(url, headers=get_headers(), params=None)

    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    data = response.json()

    payload = {
        "owner": owner,
        "repo": repo,
        "fetched_at": fetched_at,
        "endpoint": endpoint,
        "params": params,
        "url": response.url,
        "count": len(data),
        "data": data,
    }

    key = (
        f"{BRONZE_PREFIX}/raw_{owner}_{repo}_{endpoint}/"
        f"yyyy={now.year:04d}/mm={now.month:02d}/dd={now.day:02d}/"
        f"hh={now.hour:02d}/{endpoint}_page_{page}_{uuid.uuid4()}.json"
    )

    upload_to_s3(json.dumps(payload).encode("utf-8"), key)
    print(f"Uploaded {endpoint} page {page} to s3://{S3_BUCKET}/{key} (count={len(data)})")

    next_url = response.links.get("next", {}).get("url")

    return {
        "page": page,
        "count": len(data),
        "next_url": next_url,
        "response_url": response.url,
    }



def fetch_paginated_and_upload_sequential(owner: str, repo: str, endpoint: str, params: dict = None, incremental: bool = True) -> None:
    page = 1
    total_items = 0
    since_value = s3_checkpoint_read(owner, repo, endpoint) if incremental else None
    url = None
    while True:
        result = fetch_page_and_upload(
            owner=owner,
            repo=repo,
            endpoint=endpoint,
            page=page,
            next_url=url,
            params=params,
            incremental=incremental,
            since_value=since_value
        )

        total_items += result["count"]
        url = result["next_url"]
        params = None

        if not url:
            break

        page += 1

    finished_at = datetime.now(timezone.utc).isoformat()
    s3_checkpoint_write(owner, repo, endpoint, finished_at)
    print(f"Checkpoint for {owner}/{repo} {endpoint} updated to {finished_at}")
    print(f"Finished sequential fetch for {owner}/{repo} {endpoint}: total_items={total_items}")



def get_total_pages(owner: str, repo: str, endpoint: str, params: dict = None,incremental = False, since_value: Optional[str] = None) -> Optional[int]:
    params = params.copy() if params else {}
    params.setdefault("per_page", 100)
    params["page"] = 1
    if endpoint == "issues":
        params.setdefault("state", "all")
    

    
    if since_value and incremental and "since" not in params:
        params["since"] = since_value

    url = f"https://api.github.com/repos/{owner}/{repo}/{endpoint}"
    response = safe_request(url, headers=get_headers(), params=params)

    if "last" in response.links:
        last_url = response.links["last"]["url"]
        parsed = urlparse(last_url)
        page_str = parse_qs(parsed.query).get("page", ["1"])[0]
        return int(page_str)

    if "next" in response.links:
        return None

    data = response.json()
    return 1 if data else 0


# parallel only works for commits coz issues it doesn't hav a 'last' in its header, so we can't  know how many pages numbers we need to fetch 
def fetch_paginated_and_upload_parallel(owner: str, repo: str, endpoint: str, params: dict = None, incremental: bool = True, max_workers: int = 8) -> None:
    since_value = s3_checkpoint_read(owner, repo, endpoint) if incremental else None
    total_pages = get_total_pages(
        owner=owner,
        repo=repo,
        endpoint=endpoint,
        params=params,
        incremental=incremental,
        since_value = since_value
    )

    print(f"{owner}/{repo} {endpoint}: total_pages={total_pages}")

    if total_pages == 0:
        print(f"No data found for {owner}/{repo} {endpoint}")
        return
    
    if total_pages is None:
        print(f"Cannot determine total pages for {owner}/{repo} {endpoint}, falling back to sequential fetch")
        fetch_paginated_and_upload_sequential(owner, repo, endpoint, params, incremental)
        return 

    completed_pages = 0
    total_items = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(fetch_page_and_upload, owner=owner, repo=repo, endpoint=endpoint, page=page, params=params, incremental=incremental, since_value=since_value): page
            for page in range(1, total_pages + 1)
        }

        for future in as_completed(futures):
            page = futures[future]
            try:
                result = future.result()
                completed_pages += 1
                total_items += result["count"]

                elapsed = time.time() - start_time
                avg_time = elapsed / completed_pages
                remaining_pages = total_pages - completed_pages
                eta_seconds = remaining_pages * avg_time

                print(
                    f"{owner}/{repo} {endpoint}: "
                    f"{completed_pages}/{total_pages} pages done | "
                    f"last_page={page} | eta={eta_seconds:.1f}s"
                )
            except Exception as e:
                print(f"Failed page {page} for {owner}/{repo} {endpoint}: {e}")
                raise

    finished_at = datetime.now(timezone.utc).isoformat()
    s3_checkpoint_write(owner, repo, endpoint, finished_at)
    print(f"Checkpoint for {endpoint} updated to {finished_at}")
    print(f"Finished parallel fetch for {owner}/{repo} {endpoint}: total_items={total_items}")


def load_repos(filepath: str) -> list[dict]:
    with open(filepath, "r") as f:
        return json.load(f)
    

def upload_repo_metadata(owner: str, repo: str) -> None:
    response = safe_request(f"https://api.github.com/repos/{owner}/{repo}", headers=get_headers())
    now = datetime.now(timezone.utc)
    payload = {
        "owner": owner,
        "repo": repo,
        "fetched_at": now.isoformat(),
        "endpoint": "repo_metadata",
        "data": response.json()
    }
    s3_repo_metadata_write(owner, repo, payload, now)

def main():
    
    # for backfills, we can use parallel fetching for commits
    # IMPORTANT: check first if the response has 'last' header, only use parallel when there is 'last' in the response header or it won't work coz we need total pages number 
    #fetch_paginated_and_upload_sequential("pallets", "flask", endpoint="commits", incremental=False)
    
    repos = load_repos(REPO_JSON_PATH)
    
    for repo_info in repos:
        owner = repo_info["owner"]
        repo = repo_info["repo"]

        print(f"Processing {owner}/{repo}...")
        upload_repo_metadata(owner, repo)
        #fetch_paginated_and_upload_sequential(owner, repo, endpoint="issues", incremental=False)
        #fetch_paginated_and_upload_parallel(owner, repo, endpoint="commits", incremental=False)       


def testing() -> dict: 
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    
    response = requests.get("https://api.github.com/repos/apache/airflow", headers=headers, timeout=60)
    response.raise_for_status()
    
    print(json.dumps(dict(response.headers), indent=2))
    return(response.json())






    

if __name__ == "__main__":
    main()