import csv
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Tuple

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

# Primary unique identifier field in Airtable for de-duplication / upsert
UNIQUE_ID_FIELD = "CQC Location ID"
# Backwards-compatible fallback field name (older schema)
UNIQUE_ID_FIELD_FALLBACK = "CQC Location ID (for office use only)"

# CQC provider details endpoint (for number of sites)
CQC_PROVIDER_URL_TEMPLATE = "https://api.service.cqc.org.uk/public/v1/providers/{provider_id}"

# Field names used for enrichment in Airtable (must match Airtable column headers)
FIELD_NUMBER_OF_SITES = "Number of Sites"
FIELD_COMPANY_NUMBER = "Company Number"
FIELD_REGISTERED_ADDRESS = "Registered Office Address"
FIELD_ACTIVE_DIRECTORS = "Active Directors"
FIELD_ACTIVE_SECRETARIES = "Active Secretaries"


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
    """Parse a CSV file into a list of dict rows, skipping completely empty rows."""
    rows: List[Dict[str, str]] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip rows where all values are empty/whitespace
                if not any(str(value).strip() for value in row.values() if value is not None):
                    continue
                rows.append(row)
    except FileNotFoundError:
        print(f"[JOB] CSV file not found for parsing: {path}")
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Error while parsing CSV {path}: {e}")
    return rows


def fetch_existing_airtable_ids(token: str) -> Dict[str, str]:
    """
    Fetch all existing records from Airtable and build a set of IDs
    from the UNIQUE_ID_FIELD column.
    """
    headers = {
        "Authorization": f"Bearer {token}",
    }
    # Map from unique identifier value -> Airtable record ID
    existing_ids: Dict[str, str] = {}
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
            record_id = rec.get("id")
            fields = rec.get("fields", {})
            # Support both current and legacy field names
            value = fields.get(UNIQUE_ID_FIELD) or fields.get(UNIQUE_ID_FIELD_FALLBACK)
            if record_id and isinstance(value, str) and value.strip():
                existing_ids[value.strip()] = record_id

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


def update_records_in_airtable(
    token: str,
    records_to_update: List[Dict[str, Dict[str, str]]],
) -> int:
    """
    Update existing Airtable records in batches of 10 using PATCH.

    Each item in records_to_update must be:
      {"id": "<record_id>", "fields": { ... }}
    """
    if not records_to_update:
        print("[JOB] No existing records to update in Airtable")
        return 0

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    updated_count = 0
    batch_size = 10

    print(f"[JOB] Updating {len(records_to_update)} existing records in Airtable in batches of {batch_size}")

    for i in range(0, len(records_to_update), batch_size):
        batch = records_to_update[i : i + batch_size]
        payload = {"records": batch}

        try:
            resp = requests.patch(AIRTABLE_API_URL, headers=headers, json=payload, timeout=30)
        except requests.RequestException as e:
            print(f"[JOB] Error updating batch in Airtable: {e}")
            raise

        if resp.status_code not in (200, 201):
            print(
                "[JOB] Failed to update batch in Airtable, "
                f"status={resp.status_code}, body={resp.text}",
            )
            raise RuntimeError(f"Failed to update batch in Airtable: {resp.status_code}")

        data = resp.json()
        batch_updated = len(data.get("records", []))
        updated_count += batch_updated
        print(f"[JOB] Updated batch of {batch_updated} records in Airtable")

    print(f"[JOB] Finished updating Airtable, total updated={updated_count}")
    return updated_count


def get_provider_id_from_row(row: Dict[str, str]) -> str:
    """
    Attempt to extract a provider identifier from the CSV row.
    """
    candidates = [
        "CQC Provider ID",
        "Provider ID",
        "providerId",
        "ProviderId",
        "Provider ID (for office use only)",
        "CQC Provider ID (for office use only)",
    ]
    for key in candidates:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def get_company_search_name_from_row(row: Dict[str, str]) -> str:
    """
    Attempt to derive a suitable name for Companies House search.
    """
    candidates = [
        "Provider Name",
        "Name",
        "Location Name",
        "Organisation Name",
    ]
    for key in candidates:
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def get_provider_site_count(
    provider_id: str,
    session: requests.Session,
    subscription_key: str,
    cache: Dict[str, int],
) -> Tuple[bool, int]:
    """
    Get number of sites (locations) for given provider from CQC API.
    Results are cached in-memory for the duration of the job.
    """
    if provider_id in cache:
        return True, cache[provider_id]

    url = CQC_PROVIDER_URL_TEMPLATE.format(provider_id=provider_id)
    headers = {
        "Ocp-Apim-Subscription-Key": subscription_key,
    }

    try:
        resp = session.get(url, headers=headers, timeout=10)
    except requests.RequestException as e:
        print(f"[JOB] Error fetching CQC provider {provider_id}: {e}")
        return False, 0

    if resp.status_code != 200:
        print(
            f"[JOB] Failed to fetch CQC provider {provider_id}, "
            f"status={resp.status_code}",
        )
        return False, 0

    data = resp.json()
    location_ids = data.get("locationIds") or []
    try:
        num_sites = len(location_ids)
    except TypeError:
        num_sites = 0

    cache[provider_id] = num_sites
    return True, num_sites


def enrich_row_with_companies_house(
    row: Dict[str, str],
    session: requests.Session,
    api_key: str | None,
) -> None:
    """
    Enrich a row with Companies House data.

    If the Companies House API call fails for any reason, the row is still used,
    with enrichment fields set to 'NOT FOUND' or blank.
    """
    # Defaults
    row.setdefault(FIELD_COMPANY_NUMBER, "NOT FOUND")
    row.setdefault(FIELD_REGISTERED_ADDRESS, "")
    row.setdefault(FIELD_ACTIVE_DIRECTORS, "NOT FOUND")
    row.setdefault(FIELD_ACTIVE_SECRETARIES, "NOT FOUND")

    if not api_key:
        print("[JOB] COMPANIES_HOUSE_API_KEY not set; skipping Companies House enrichment")
        return

    name = get_company_search_name_from_row(row)
    if not name:
        print("[JOB] No suitable name found for Companies House search; skipping enrichment for this row")
        return

    auth = (api_key, "")

    # Step 1: search for company
    search_params = {"q": name, "items_per_page": 1}
    try:
        search_resp = session.get(
            "https://api.company-information.service.gov.uk/search/companies",
            params=search_params,
            auth=auth,
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"[JOB] Companies House search error for '{name}': {e}")
        return

    if search_resp.status_code != 200:
        print(
            f"[JOB] Companies House search failed for '{name}', "
            f"status={search_resp.status_code}",
        )
        return

    search_data = search_resp.json()
    items = search_data.get("items") or []
    if not items:
        print(f"[JOB] Companies House search returned no results for '{name}'")
        return

    company_number = items[0].get("company_number")
    if not company_number:
        print(f"[JOB] Companies House search result missing company_number for '{name}'")
        return

    row[FIELD_COMPANY_NUMBER] = company_number

    # Step 2: company details (registered office address)
    try:
        company_resp = session.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}",
            auth=auth,
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"[JOB] Companies House company details error for '{company_number}': {e}")
        return

    if company_resp.status_code == 200:
        company_data = company_resp.json()
        address = company_data.get("registered_office_address") or {}
        address_parts = [
            address.get("address_line_1"),
            address.get("address_line_2"),
            address.get("locality"),
            address.get("region"),
            address.get("postal_code"),
            address.get("country"),
        ]
        address_str = ", ".join([part for part in address_parts if part])
        row[FIELD_REGISTERED_ADDRESS] = address_str
    else:
        print(
            f"[JOB] Companies House company details failed for '{company_number}', "
            f"status={company_resp.status_code}",
        )

    # Step 3: officers (directors and secretaries)
    try:
        officers_resp = session.get(
            f"https://api.company-information.service.gov.uk/company/{company_number}/officers",
            auth=auth,
            timeout=10,
        )
    except requests.RequestException as e:
        print(f"[JOB] Companies House officers error for '{company_number}': {e}")
        return

    if officers_resp.status_code != 200:
        print(
            f"[JOB] Companies House officers failed for '{company_number}', "
            f"status={officers_resp.status_code}",
        )
        return

    officers_data = officers_resp.json()
    officers = officers_data.get("items") or []

    active_directors: List[str] = []
    active_secretaries: List[str] = []

    for officer in officers:
        name = officer.get("name")
        if not name:
            continue
        role = (officer.get("officer_role") or "").lower()
        resigned_on = officer.get("resigned_on")
        if resigned_on:
            continue  # not active

        if "director" in role:
            active_directors.append(name)
        if "secretary" in role:
            active_secretaries.append(name)

    if active_directors:
        row[FIELD_ACTIVE_DIRECTORS] = ", ".join(active_directors)
    else:
        row[FIELD_ACTIVE_DIRECTORS] = "None"

    if active_secretaries:
        row[FIELD_ACTIVE_SECRETARIES] = ", ".join(active_secretaries)
    else:
        row[FIELD_ACTIVE_SECRETARIES] = "None"


def apply_cqc_filter_and_companies_house_enrichment(
    rows: List[Dict[str, str]],
) -> List[Dict[str, str]]:
    """
    Apply the CQC provider site-count filter (>7 sites excluded) and
    enrich remaining rows with Companies House data.

    - If CQC provider fetch fails: row is skipped.
    - If Companies House lookup fails: row is kept with CH fields marked as not found.
    """
    cqc_subscription_key = os.environ.get("CQC_SUBSCRIPTION_KEY")
    if not cqc_subscription_key:
        print("[JOB] CQC_SUBSCRIPTION_KEY is not set; cannot apply provider site filter")
        return []

    ch_api_key = os.environ.get("COMPANIES_HOUSE_API_KEY")

    cqc_session = requests.Session()
    ch_session = requests.Session()
    provider_site_cache: Dict[str, int] = {}

    kept_rows: List[Dict[str, str]] = []

    for row in rows:
        provider_id = get_provider_id_from_row(row)
        if not provider_id:
            print("[JOB] No provider ID found for row; skipping due to missing provider identifier")
            continue

        ok, num_sites = get_provider_site_count(
            provider_id,
            cqc_session,
            cqc_subscription_key,
            provider_site_cache,
        )
        if not ok:
            print(f"[JOB] Skipping provider {provider_id} due to CQC provider fetch failure")
            continue

        if num_sites > 7:
            print(f"[JOB] Skipped provider {provider_id} due to {num_sites} sites (>7)")
            continue

        # Record number of sites in the row for Airtable
        row[FIELD_NUMBER_OF_SITES] = num_sites

        # Enrich with Companies House (best-effort)
        enrich_row_with_companies_house(row, ch_session, ch_api_key)

        kept_rows.append(row)

        # Light rate limiting for external APIs
        time.sleep(0.2)

    print(
        f"[JOB] After applying CQC site filter and Companies House enrichment, "
        f"{len(kept_rows)} of {len(rows)} rows remain",
    )
    return kept_rows


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

    # Before hitting Airtable, apply CQC provider site filter and Companies House enrichment.
    filtered_and_enriched_rows = apply_cqc_filter_and_companies_house_enrichment(all_rows)
    if not filtered_and_enriched_rows:
        print("[JOB] No rows left after CQC site filter / enrichment; nothing to sync to Airtable")
        return True

    # Normalize legacy CQC field names to current Airtable schema and
    # drop the old "(for office use only)" variants to avoid UNKNOWN_FIELD_NAME.
    for row in filtered_and_enriched_rows:
        legacy_mappings = [
            ("CQC Provider ID (for office use only)", "CQC Provider ID"),
            ("CQC Location ID (for office use only)", "CQC Location ID"),
        ]
        for old_key, new_key in legacy_mappings:
            if old_key in row:
                if new_key not in row or not row[new_key]:
                    row[new_key] = row[old_key]
                row.pop(old_key, None)

    print(f"[JOB] {len(filtered_and_enriched_rows)} rows remain after CQC site filter; fetching Airtable IDs")

    try:
        existing_ids = fetch_existing_airtable_ids(token)
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Aborting: failed to fetch existing Airtable IDs: {e}")
        return False
    # existing_ids: mapping from unique ID value -> Airtable record ID

    existing_count = 0
    duplicate_in_batch_count = 0
    create_rows: List[Dict[str, str]] = []
    update_records: List[Dict[str, Dict[str, str]]] = []
    seen_ids: set[str] = set()

    for row in filtered_and_enriched_rows:
        # Support both current and legacy unique ID field names
        unique_value = (row.get(UNIQUE_ID_FIELD) or row.get(UNIQUE_ID_FIELD_FALLBACK) or "").strip()
        if not unique_value:
            # Skip rows without an ID, they cannot be deduplicated.
            duplicate_in_batch_count += 1
            continue

        if unique_value in seen_ids:
            # Duplicate within this run; skip
            duplicate_in_batch_count += 1
            continue

        seen_ids.add(unique_value)

        if unique_value in existing_ids:
            existing_count += 1
            update_records.append({"id": existing_ids[unique_value], "fields": row})
        else:
            create_rows.append(row)

    print(f"[JOB] Total rows parsed: {total_rows}")
    print(f"[JOB] Existing records to update in Airtable: {existing_count}")
    print(f"[JOB] Skipped duplicates or rows without ID: {duplicate_in_batch_count}")
    print(f"[JOB] New rows to add to Airtable: {len(create_rows)}")

    try:
        created_count = upload_new_records_to_airtable(token, create_rows)
        updated_count = update_records_in_airtable(token, update_records)
    except Exception as e:  # noqa: BLE001
        print(f"[JOB] Error while syncing rows to Airtable: {e}")
        return False

    print(f"[JOB] New rows successfully added to Airtable: {created_count}")
    print(f"[JOB] Existing rows successfully updated in Airtable: {updated_count}")
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

