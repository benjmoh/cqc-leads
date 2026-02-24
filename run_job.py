import os
import sys
import time
from datetime import datetime, timezone
from typing import Tuple

import requests


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


def timestamp_utc() -> str:
    """Return UTC timestamp formatted as YYYYMMDD_HHMMSSZ."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y%m%d_%H%M%SZ")


def build_filename(prefix: str) -> str:
    ts = timestamp_utc()
    return f"cqc_{prefix}_{ts}.csv"


def is_csv_like(sample: bytes) -> bool:
    """
    Validate that the first part of the response looks like CSV, not HTML.

    - Reject if first 500 characters contain "<html" or "<!doctype".
    - Require at least one comma and one newline in the same slice.
    """
    snippet = sample[:500].lower()

    if b"<html" in snippet or b"<!doctype" in snippet:
        return False

    # Decode for easier inspection of commas/newlines
    try:
        text = snippet.decode("utf-8", errors="ignore")
    except Exception:  # noqa: BLE001
        return False

    return ("," in text) and ("\n" in text or "\r" in text)


def download_csv(
    url: str,
    target_path: str,
    *,
    timeout: int = 30,
    max_retries: int = 5,
) -> Tuple[bool, str, int, int]:
    """
    Download a CSV file with retries and basic validation.

    Returns (success, message, bytes_downloaded, line_count).
    """
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
    last_error = "Unknown error"

    while attempt < max_retries:
        attempt += 1
        print(f"[JOB] Starting download attempt {attempt}/{max_retries} for {url}")

        try:
            resp = session.get(url, headers=headers, stream=True, timeout=timeout)
        except requests.RequestException as e:
            last_error = f"Request error: {e}"
            print(f"[JOB] {last_error}")

            # Retry on network/timeouts
            if attempt < max_retries:
                backoff = 2 ** (attempt - 1)
                print(f"[JOB] Retrying in {backoff} seconds...")
                time.sleep(backoff)
                continue
            break

        status_code = resp.status_code
        print(f"[JOB] HTTP status {status_code} for {url}")

        # Retry on transient status codes
        if status_code in {429, 500, 502, 503, 504}:
            last_error = f"Transient HTTP error {status_code}"
            print(f"[JOB] {last_error}")
            if attempt < max_retries:
                backoff = 2 ** (attempt - 1)
                print(f"[JOB] Retrying in {backoff} seconds...")
                time.sleep(backoff)
                continue
            break

        if status_code != 200:
            last_error = f"Unexpected HTTP status {status_code}"
            print(f"[JOB] {last_error}")
            break

        tmp_path = f"{target_path}.part"
        total_bytes = 0
        line_count = 0
        sample_bytes = b""
        validated = False

        try:
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if not chunk:
                        continue

                    # Collect up to 500 chars for validation
                    if not validated:
                        sample_bytes += chunk
                        if len(sample_bytes) >= 500:
                            if not is_csv_like(sample_bytes):
                                last_error = "Response does not appear to be CSV (failed validation)"
                                print(f"[JOB] {last_error}")
                                resp.close()
                                raise ValueError(last_error)
                            validated = True

                    # Count lines on the fly
                    text_chunk = chunk.decode("utf-8", errors="ignore")
                    line_count += text_chunk.count("\n")

                    f.write(chunk)
                    total_bytes += len(chunk)

            # If we never reached 500 bytes, still validate what we have
            if not validated:
                if not is_csv_like(sample_bytes):
                    last_error = "Response does not appear to be CSV (failed validation on small body)"
                    print(f"[JOB] {last_error}")
                    raise ValueError(last_error)

            os.replace(tmp_path, target_path)
            print(
                f"[JOB] Downloaded {total_bytes} bytes, {line_count} lines, saved to {target_path}",
            )
            return True, "ok", total_bytes, line_count

        except ValueError as e:
            last_error = str(e)
            print(f"[JOB] Validation error: {last_error}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            break
        except Exception as e:  # noqa: BLE001
            last_error = f"Error while downloading or saving file: {e}"
            print(f"[JOB] {last_error}")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

            # Retry on generic I/O issues
            if attempt < max_retries:
                backoff = 2 ** (attempt - 1)
                print(f"[JOB] Retrying in {backoff} seconds...")
                time.sleep(backoff)
                continue
            break

    return False, last_error, 0, 0


def main() -> int:
    data_dir = os.environ.get("DATA_DIR", "./data")
    os.makedirs(data_dir, exist_ok=True)

    print("[JOB] CQC CSV cron job starting")
    print(f"[JOB] Using DATA_DIR={data_dir}")

    homecare_filename = build_filename("homecare")
    carehomes_filename = build_filename("carehomes")

    homecare_path = os.path.join(data_dir, homecare_filename)
    carehomes_path = os.path.join(data_dir, carehomes_filename)

    # Download homecare CSV
    print(f"[JOB] Downloading homecare CSV to {homecare_path}")
    homecare_ok, homecare_msg, homecare_bytes, homecare_lines = download_csv(
        HOMECARE_URL,
        homecare_path,
    )

    # Download carehomes CSV
    print(f"[JOB] Downloading carehomes CSV to {carehomes_path}")
    carehomes_ok, carehomes_msg, carehomes_bytes, carehomes_lines = download_csv(
        CAREHOMES_URL,
        carehomes_path,
    )

    # Summary logs
    print("[JOB] Summary:")
    print(
        f"  Homecare: ok={homecare_ok}, bytes={homecare_bytes}, "
        f"lines={homecare_lines}, path={homecare_path}, msg={homecare_msg}",
    )
    print(
        f"  Carehomes: ok={carehomes_ok}, bytes={carehomes_bytes}, "
        f"lines={carehomes_lines}, path={carehomes_path}, msg={carehomes_msg}",
    )

    if homecare_ok and carehomes_ok:
        print("[JOB] All downloads completed successfully")
        return 0

    print("[JOB] One or more downloads failed")
    return 1


if __name__ == "__main__":
    sys.exit(main())

