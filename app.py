import os
import time
import shutil
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

import requests
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware


HOMECARE_URL = (
    "https://www.cqc.org.uk/search/all?query=&location-query=&radius="
    "&display=csv&sort=relevance&last-published=week"
    "&filters[]=archived:active"
    "&filters[]=lastPublished:all"
    "&filters[]=more_services:all"
    "&filters[]=overallRating:Not%20rated"
    "&filters[]=overallRating:Inadequate"
    "&filters[]=overallRating:Requires%20improvement"
    "&filters[]=services:homecare-agencies"
    "&filters[]=specialisms:all"
)

CAREHOMES_URL = (
    "https://www.cqc.org.uk/search/all?query=&location-query=&radius="
    "&display=csv&sort=relevance&last-published=week"
    "&filters[]=archived:active"
    "&filters[]=careHomes:all"
    "&filters[]=lastPublished:all"
    "&filters[]=more_services:all"
    "&filters[]=overallRating:Not%20rated"
    "&filters[]=overallRating:Inadequate"
    "&filters[]=overallRating:Requires%20improvement"
    "&filters[]=services:care-home"
    "&filters[]=specialisms:all"
)


def get_env_settings() -> Dict[str, Any]:
    data_dir = os.environ.get("DATA_DIR", "/data")
    run_token = os.environ.get("RUN_TOKEN", "")
    port = int(os.environ.get("PORT", "10000"))
    return {"DATA_DIR": data_dir, "RUN_TOKEN": run_token, "PORT": port}


settings = get_env_settings()
os.makedirs(settings["DATA_DIR"], exist_ok=True)

app = FastAPI(title="CQC CSV Downloader Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def require_token_header(x_auth_token: Optional[str]) -> None:
    if not settings["RUN_TOKEN"]:
        print("[WARN] RUN_TOKEN is not set; all authenticated endpoints will reject requests.")
        raise HTTPException(status_code=500, detail="RUN_TOKEN is not configured on the server")

    if not x_auth_token or x_auth_token != settings["RUN_TOKEN"]:
        raise HTTPException(status_code=401, detail="Invalid or missing X-Auth-Token")


def require_token_query(token: Optional[str]) -> None:
    if not settings["RUN_TOKEN"]:
        print("[WARN] RUN_TOKEN is not set; all authenticated endpoints will reject requests.")
        raise HTTPException(status_code=500, detail="RUN_TOKEN is not configured on the server")

    if not token or token != settings["RUN_TOKEN"]:
        raise HTTPException(status_code=401, detail="Invalid or missing token")


def _timestamp_utc() -> str:
    # Format: YYYYMMDD_HHMMSSZ
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%d_%H%M%SZ")


def _build_filename(prefix: str) -> str:
    ts = _timestamp_utc()
    safe_prefix = prefix.strip().replace(" ", "").lower()
    return f"cqc_{safe_prefix}_{ts}.csv"


def _cleanup_old_files(directory: str, days: int = 30) -> None:
    """Delete CSV files older than `days` days. Best-effort; logs only."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    try:
        for entry in os.scandir(directory):
            if not entry.is_file():
                continue
            name = entry.name
            if not (name.startswith("cqc_") and name.endswith(".csv")):
                continue
            try:
                mtime = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
                if mtime < cutoff:
                    print(f"[CLEANUP] Deleting old file: {entry.path}")
                    os.remove(entry.path)
            except Exception as e:  # noqa: BLE001
                print(f"[CLEANUP] Failed to inspect/delete {entry.path}: {e}")
    except FileNotFoundError:
        return
    except Exception as e:  # noqa: BLE001
        print(f"[CLEANUP] Unexpected error scanning directory {directory}: {e}")


def _is_csv_like(content_type: str, first_chunk: bytes) -> bool:
    ct = (content_type or "").lower()
    if "csv" in ct:
        return True

    # Heuristic: check for commas/newlines, and ensure it's not obvious HTML
    sample = first_chunk[:1024].lower()
    if b"<html" in sample or b"<!doctype html" in sample:
        return False
    if b"," in sample and (b"\n" in sample or b"\r" in sample):
        return True
    return False


def download_csv_with_retries(url: str, target_path: str, timeout: int = 30, max_retries: int = 5) -> Dict[str, Any]:
    """Download CSV from URL with retries and basic validation."""
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/csv,application/octet-stream;q=0.9,*/*;q=0.8",
    }

    attempt = 0
    last_error: Optional[str] = None

    while attempt < max_retries:
        attempt += 1
        try:
            print(f"[DOWNLOAD] Attempt {attempt}/{max_retries} for {url}")
            with session.get(url, headers=headers, stream=True, timeout=timeout) as resp:
                status = resp.status_code
                if status in {429, 500, 502, 503, 504}:
                    last_error = f"Server returned {status}"
                    raise requests.RequestException(last_error)

                if status != 200:
                    msg = f"Unexpected status code {status}"
                    print(f"[DOWNLOAD] {msg}")
                    return {"ok": False, "error": msg}

                content_type = resp.headers.get("Content-Type", "")

                tmp_path = f"{target_path}.part"
                total_bytes = 0
                first_chunk: Optional[bytes] = None
                try:
                    with open(tmp_path, "wb") as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if not chunk:
                                continue
                            if first_chunk is None:
                                first_chunk = chunk
                                if not _is_csv_like(content_type, first_chunk):
                                    msg = (
                                        "Response does not appear to be CSV; "
                                        f"Content-Type={content_type!r}"
                                    )
                                    print(f"[DOWNLOAD] Validation failed: {msg}")
                                    raise ValueError(msg)
                            f.write(chunk)
                            total_bytes += len(chunk)

                    if first_chunk is None:
                        msg = "Empty response body"
                        print(f"[DOWNLOAD] {msg}")
                        return {"ok": False, "error": msg}

                    shutil.move(tmp_path, target_path)
                    print(f"[DOWNLOAD] Saved {total_bytes} bytes to {target_path}")
                    return {"ok": True, "bytes": total_bytes}
                except ValueError as ve:
                    # Validation error; do not retry
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    return {"ok": False, "error": str(ve)}
                except Exception as e:  # noqa: BLE001
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    last_error = f"Error while streaming content: {e}"
                    print(f"[DOWNLOAD] {last_error}")
                    # fall through to retry
        except requests.RequestException as e:
            last_error = f"Request error: {e}"
            print(f"[DOWNLOAD] {last_error}")

        if attempt < max_retries:
            backoff = 2 ** (attempt - 1)
            print(f"[DOWNLOAD] Retrying in {backoff} seconds...")
            time.sleep(backoff)

    error_msg = last_error or "Failed to download after retries"
    return {"ok": False, "error": error_msg}


@app.get("/health")
async def health() -> Dict[str, Any]:
    return {"ok": True}


@app.post("/run")
async def run_downloads(request: Request, x_auth_token: Optional[str] = Header(default=None)) -> JSONResponse:
    require_token_header(x_auth_token)

    data_dir = settings["DATA_DIR"]
    os.makedirs(data_dir, exist_ok=True)

    print("[RUN] Starting CSV downloads for CQC exports")

    homecare_filename = _build_filename("homecare")
    carehomes_filename = _build_filename("carehomes")

    homecare_path = os.path.join(data_dir, homecare_filename)
    carehomes_path = os.path.join(data_dir, carehomes_filename)

    # Run downloads sequentially for simplicity and clarity
    homecare_result = download_csv_with_retries(HOMECARE_URL, homecare_path)
    carehomes_result = download_csv_with_retries(CAREHOMES_URL, carehomes_path)

    # Best-effort cleanup of old files
    _cleanup_old_files(data_dir, days=30)

    results: Dict[str, Any] = {
        "homecare": {
            "file": homecare_filename if homecare_result.get("ok") else None,
            "bytes": homecare_result.get("bytes"),
            "url": f"/files/{homecare_filename}" if homecare_result.get("ok") else None,
            "error": homecare_result.get("error"),
        },
        "carehomes": {
            "file": carehomes_filename if carehomes_result.get("ok") else None,
            "bytes": carehomes_result.get("bytes"),
            "url": f"/files/{carehomes_filename}" if carehomes_result.get("ok") else None,
            "error": carehomes_result.get("error"),
        },
    }

    ok_homecare = bool(homecare_result.get("ok"))
    ok_carehomes = bool(carehomes_result.get("ok"))

    if ok_homecare and ok_carehomes:
        status = "ok"
    elif ok_homecare or ok_carehomes:
        status = "partial"
    else:
        status = "error"

    resp_body = {
        "status": status,
        "saved_to": data_dir,
        "results": results,
    }

    print(f"[RUN] Completed with status={status}")
    return JSONResponse(content=resp_body)


@app.get("/files/{filename}")
async def get_file(filename: str, token: Optional[str] = None) -> FileResponse:
    """
    Download a previously saved CSV file.

    Authentication: provide ?token=RUN_TOKEN query parameter.
    """
    require_token_query(token)

    # Security: only allow simple filenames, no directory traversal
    safe_name = os.path.basename(filename)
    if safe_name != filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    data_dir = settings["DATA_DIR"]
    file_path = os.path.join(data_dir, safe_name)

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    print(f"[FILES] Serving file {file_path}")
    return FileResponse(
        file_path,
        media_type="text/csv",
        filename=safe_name,
    )


if __name__ == "__main__":
    # For local development only; in production use gunicorn with UvicornWorker.
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=settings["PORT"],
        reload=True,
    )

