import os, requests, json, uuid, boto3
from typing import Optional 
from datetime import datetime, timezone 

# in local dev u should do this, in production env vars are 
from dotenv import load_dotenv
load_dotenv()


S3_BUCKET = os.getenv("S3_BUCKET")        
AWS_REGION = os.getenv("AWS_REGION")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
S3_PREFIX = os.getenv("S3_PREFIX")


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
    key = f"{S3_PREFIX}/checkpoints/{owner}/{repo}/{endpoint}.json"
    try:
        res = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = res["Body"].read().decode("utf-8")
        obj = json.loads(body)
        return obj.get("last_run")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception:
        return None




def s3_checkpoint_write(owner: str, repo: str, endpoint: str, iso_ts: str) -> None:
    key = f"{S3_PREFIX}/checkpoints/{owner}/{repo}/{endpoint}.json"
    payload = {"last_run": iso_ts}
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(payload).encode("utf-8"))




def parse_link_header(link_header: Optional[str]) -> dict:
    """
    Parse the Link header into a dict of rel -> url
    Example Link header:
    <https://api.github.com/resource?page=2>; rel="next", <...>; rel="last"
    Returns: {"next": "...", "last": "..."}
    """
    if not link_header:
        return {}
    parts = link_header.split(",")
    links = {}
    for part in parts:
        section = part.strip().split(";")
        if len(section) < 2:
            continue
        url_part = section[0].strip()
        rel_part = section[1].strip()
        # url_part: <https://...>
        url = url_part[url_part.find("<")+1 : url_part.rfind(">")]
        # rel_part: rel="next"
        rel = rel_part.split("=")[1].strip('"')
        links[rel] = url
    return links


def fetch_paginated_and_upload(owner: str, repo: str, endpoint: str,params: dict = None, use_since: bool = True):
    """
    Generic fetcher that follows Link header and uploads each page to S3 as a separate JSON file.
    If use_since and a checkpoint exists, adds 'since' param (ISO string) to the params.
    """
    params = params.copy() if params else {}
    params.setdefault("per_page", 100)
    params.setdefault("state", "all")

    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    # attempt to read checkpoint
    last_run = s3_checkpoint_read(owner, repo, endpoint)
    if last_run and use_since:
        # GitHub supports 'since' for many list endpoints (issues, commits).
        params["since"] = last_run

    # initial URL
    base_url = f"https://api.github.com/repos/{owner}/{repo}"
    url = f"{base_url}/{endpoint}"
    while url:
        response = requests.get(url, headers=get_headers(), params=params if "?" not in url else None, timeout=30)
        # handle rate-limit politely: raise and let caller / Airflow retry
        response.raise_for_status()

        data = response.json()
        # prepare payload (metadata + raw page)
        payload = {
            "owner": owner,
            "repo": repo,
            "fetched_at": fetched_at,
            "endpoint": endpoint,
            "params": params,
            "url": url,
            "count": len(data),
            "data": data,
        }

        # S3 key: keep tidy partitioning
        key = (
            f"{S3_PREFIX}/raw_{owner}_{repo}_{endpoint}/"
            f"yyyy={now.year:04d}/mm={now.month:02d}/dd={now.day:02d}/"
            f"hh={now.hour:02d}/{endpoint}_page_{uuid.uuid4()}.json"
        )

        upload_to_s3(json.dumps(payload).encode("utf-8"), key)
        print(f"Uploaded {endpoint} page to s3://{S3_BUCKET}/{key} (count={len(data)})")

        # parse link header for next url
        links = parse_link_header(response.headers.get("Link"))
        next_url = links.get("next")
        if next_url:
            url = next_url
            # when following link header, don't pass params again (they're in the URL)
            params = {}
        else:
            url = None

    # write checkpoint (we set to current run time)
    s3_checkpoint_write(owner, repo, endpoint, fetched_at)
    print(f"Checkpoint for {endpoint} updated to {fetched_at}")





def main():
    #testing("https://api.github.com/repos/apache/airflow")

    owner = "tensorflow"
    repo = "tensorflow" 
    endpoint = "commits"
    fetch_paginated_and_upload(owner, repo, endpoint,use_since=False)






def testing(url:str) -> dict:
    """
    Fetch repository metadata from the GitHub REST API.

    Args:
        owner: GitHub username or organization.
        repo: Repository name.

    Returns:
        Parsed JSON response as a dictionary.

    Raises:
        requests.HTTPError: If the request fails.
    """
    
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    
    print(json.dumps(dict(response.headers), indent=2))
    return(response.json())






    

if __name__ == "__main__":
    main()