import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

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

AIRTABLE_BASE_ID = "apphWtLxQpxaYaJhX"
AIRTABLE_TABLE_NAME = "Leads"
AIRTABLE_API_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
UNIQUE_ID_FIELD = "CQC Location ID (for office use only)"


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


def parse_csv_file(path: str) -> List[Dict[str, str]]:
    """Parse a CSV file into a list of dict rows."""
    rows: List[Dict[str, str]] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    except FileNotFoundError:
        print(f"[JOB] CSV file not found for parsing: {path}")
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Error while parsing CSV {path}: {e}")
    return rows


def fetch_existing_airtable_ids(token: str) -> Set[str]:
    """
    Fetch all existing records from Airtable and build a set of IDs
    from the UNIQUE_ID_FIELD column.
    """
    headers = {
        "Authorization": f"Bearer {token}",
    }
    existing_ids: Set[str] = set()
    params: Dict[str, str] = {}
    offset: str | None = None

    print("[JOB] Fetching existing Airtable records to build de-duplication set")

    while True:
        if offset:
            params["offset"] = offset
        else:
            params.pop("offset", None)

        try:
            resp = requests.get(AIRTABLE_API_URL, headers=headers, params=params, timeout=30)
        except requests.RequestException as e:
            print(f"[JOB] Error fetching Airtable records: {e}")
            raise

        if resp.status_code != 200:
            print(f"[JOB] Failed to fetch Airtable records, status={resp.status_code}, body={resp.text}")
            raise RuntimeError(f"Failed to fetch Airtable records: {resp.status_code}")

        data = resp.json()
        records = data.get("records", [])

        for rec in records:
            fields = rec.get("fields", {})
            value = fields.get(UNIQUE_ID_FIELD)
            if isinstance(value, str) and value.strip():
                existing_ids.add(value.strip())

        offset = data.get("offset")
        if not offset:
            break

    print(f"[JOB] Found {len(existing_ids)} existing Airtable IDs")
    return existing_ids


def upload_new_records_to_airtable(
    token: str,
    new_rows: List[Dict[str, str]],
) -> int:
    """
    Upload new rows to Airtable in batches of 10.

    Returns the number of successfully created records.
    """
    if not new_rows:
        print("[JOB] No new rows to upload to Airtable")
        return 0

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    created_count = 0
    batch_size = 10

    print(f"[JOB] Uploading {len(new_rows)} new records to Airtable in batches of {batch_size}")

    for i in range(0, len(new_rows), batch_size):
        batch = new_rows[i : i + batch_size]
        payload = {"records": [{"fields": row} for row in batch]}

        try:
            resp = requests.post(AIRTABLE_API_URL, headers=headers, json=payload, timeout=30)
        except requests.RequestException as e:
            print(f"[JOB] Error uploading batch to Airtable: {e}")
            raise

        if resp.status_code not in (200, 201):
            print(
                "[JOB] Failed to upload batch to Airtable, "
                f"status={resp.status_code}, body={resp.text}",
            )
            raise RuntimeError(f"Failed to upload batch to Airtable: {resp.status_code}")

        data = resp.json()
        batch_created = len(data.get("records", []))
        created_count += batch_created
        print(f"[JOB] Uploaded batch of {batch_created} records to Airtable")

    print(f"[JOB] Finished uploading to Airtable, total created={created_count}")
    return created_count


def sync_rows_to_airtable(all_rows: List[Dict[str, str]]) -> bool:
    """
    De-duplicate rows against Airtable and append only new ones.

    Logs:
    - Total rows parsed
    - Existing records found
    - New rows added
    - Skipped duplicates
    """
    token = os.environ.get("AIRTABLE_TOKEN")
    if not token:
        print("[JOB] AIRTABLE_TOKEN is not set; cannot sync to Airtable")
        return False

    total_rows = len(all_rows)
    print(f"[JOB] Preparing to sync {total_rows} rows to Airtable")

    try:
        existing_ids = fetch_existing_airtable_ids(token)
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Aborting: failed to fetch existing Airtable IDs: {e}")
        return False

    existing_count = 0
    duplicate_in_batch_count = 0
    new_rows: List[Dict[str, str]] = []
    seen_new_ids: Set[str] = set()

    for row in all_rows:
        unique_value = (row.get(UNIQUE_ID_FIELD) or "").strip()
        if not unique_value:
            # Skip rows without an ID, they cannot be deduplicated.
            duplicate_in_batch_count += 1
            continue

        if unique_value in existing_ids:
            existing_count += 1
            continue

        if unique_value in seen_new_ids:
            duplicate_in_batch_count += 1
            continue

        seen_new_ids.add(unique_value)
        new_rows.append(row)

    print(f"[JOB] Total rows parsed: {total_rows}")
    print(f"[JOB] Existing records found in Airtable: {existing_count}")
    print(f"[JOB] Skipped duplicates or rows without ID: {duplicate_in_batch_count}")
    print(f"[JOB] New rows to add to Airtable: {len(new_rows)}")

    try:
        created_count = upload_new_records_to_airtable(token, new_rows)
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Error while uploading new rows to Airtable: {e}")
        return False

    print(f"[JOB] New rows successfully added to Airtable: {created_count}")
    return True


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

    if not (homecare_ok and carehomes_ok):
        print("[JOB] One or more downloads failed")
        return 1

    print("[JOB] Both CSV downloads completed successfully, starting Airtable sync")

    # Parse CSVs and combine rows
    homecare_rows = parse_csv_file(homecare_path)
    carehomes_rows = parse_csv_file(carehomes_path)
    combined_rows = homecare_rows + carehomes_rows

    print(
        f"[JOB] Parsed {len(homecare_rows)} homecare rows and "
        f"{len(carehomes_rows)} carehomes rows (combined {len(combined_rows)})",
    )

    # Sync combined rows to Airtable
    airtable_ok = sync_rows_to_airtable(combined_rows)
    if not airtable_ok:
        print("[JOB] Airtable sync failed")
        return 1

    print("[JOB] All downloads and Airtable sync completed successfully")
    return 0


if __name__ == "__main__":
    sys.exit(main())

