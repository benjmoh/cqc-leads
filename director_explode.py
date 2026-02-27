import os
import re
from typing import Any, Dict, List, Set, Tuple

import requests


# Airtable configuration
AIRTABLE_BASE_ID = "apphWtLxQpxaYaJhX"
AIRTABLE_API_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"

TABLE_LEADS = "Leads"
TABLE_DIRECTOR_ENRICHMENT = "Director Enrichment"

PROVIDER_ID_FIELD = "CQC Provider ID"
PROVIDER_NAME_FIELD = "Provider Name"
DIRECTOR_NAME_FIELD = "Director Name"
DIRECTOR_KEY_FIELD = "Director Key"
ENRICHMENT_STATUS_FIELD = "Enrichment Status"

# IMPORTANT:
# Set this to the exact field name in the "Leads" table that holds
# the raw director names string. For example, you might create a field
# called "Directors (raw)" and then set:
#   DIRECTORS_FIELD = "Directors (raw)"
#
# Currently this uses the existing "Active Directors" field populated
# by the Companies House enrichment step.
DIRECTORS_FIELD = "Active Directors"


def _get_airtable_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def list_records(
    token: str,
    table_name: str,
    fields: List[str] | None = None,
    page_size: int = 100,
) -> List[Dict[str, Any]]:
    """
    List all records from an Airtable table, handling pagination.

    Returns a list of Airtable record objects:
      {"id": "...", "fields": {...}}
    """
    from urllib.parse import quote

    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}"
    headers = _get_airtable_headers(token)

    params: Dict[str, Any] = {"pageSize": page_size}
    if fields:
        # Airtable expects repeated fields[] parameters
        params["fields[]"] = fields

    all_records: List[Dict[str, Any]] = []
    offset: str | None = None

    while True:
        if offset:
            params["offset"] = offset
        else:
            params.pop("offset", None)

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to list records from '{table_name}': "
                f"status={resp.status_code}, body={resp.text}",
            )

        data = resp.json()
        records = data.get("records", [])
        all_records.extend(records)

        offset = data.get("offset")
        if not offset:
            break

    return all_records


def create_records(
    token: str,
    table_name: str,
    records_batch: List[Dict[str, Any]],
) -> int:
    """
    Create a batch of records in Airtable.

    `records_batch` must be a list of field dictionaries (not including "id").
    This function sends them in a single POST request and returns the number
    of created records reported by Airtable.
    """
    if not records_batch:
        return 0

    from urllib.parse import quote

    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}"
    headers = _get_airtable_headers(token)

    payload = {"records": [{"fields": fields} for fields in records_batch]}
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create records in '{table_name}': "
            f"status={resp.status_code}, body={resp.text}",
        )

    data = resp.json()
    return len(data.get("records", []))


def normalise_name(name: str) -> str:
    """
    Normalise a director name for use in the Director Key.

    Rules:
    - lowercase
    - strip leading/trailing whitespace
    - remove punctuation characters (.,'"-()[]{} etc)
    - replace multiple spaces with a single space
    """
    # Lowercase and trim
    s = (name or "").strip().lower()
    if not s:
        return ""

    # Remove common punctuation
    s = s.translate(
        str.maketrans(
            "",
            "",
            ".,'\"-()[]{}",
        ),
    )

    # Collapse multiple whitespace characters into a single space
    s = re.sub(r"\s+", " ", s)
    return s


def parse_director_names(raw_str: str) -> List[str]:
    """
    Parse a raw directors string into a list of unique director names.

    Splits on commas, semicolons, newlines, and " and " (simple heuristic),
    trims whitespace, and de-duplicates names case-insensitively.
    """
    if not raw_str:
        return []

    text = str(raw_str)

    # Normalise line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # Treat " and " as a separator between names (simple heuristic)
    text = text.replace(" and ", ",")

    # Split on commas, semicolons, or newlines
    parts = re.split(r"[,\n;]+", text)

    names: List[str] = []
    seen_lower: Set[str] = set()

    for part in parts:
        name = part.strip()
        if not name:
            continue
        key = name.lower()
        if key in seen_lower:
            continue
        seen_lower.add(key)
        names.append(name)

    return names


def _fetch_existing_director_keys(token: str) -> Set[str]:
    """
    Fetch all existing Director Keys from the Director Enrichment table.
    """
    print("[DIR] Fetching existing Director Enrichment records to build de-duplication set")
    records = list_records(token, TABLE_DIRECTOR_ENRICHMENT, fields=[DIRECTOR_KEY_FIELD])

    existing_keys: Set[str] = set()
    for rec in records:
        fields = rec.get("fields", {})
        key = (fields.get(DIRECTOR_KEY_FIELD) or "").strip()
        if key:
            existing_keys.add(key)

    print(f"[DIR] Found {len(existing_keys)} existing Director Keys")
    return existing_keys


def _build_director_key(provider_id: str, director_name: str) -> str:
    """
    Build the deterministic Director Key for a provider/director pair.
    """
    normalised = normalise_name(director_name)
    if not normalised:
        return ""
    return f"{provider_id}::{normalised}"


def run_director_explode(token: str) -> Tuple[int, int, int, int, int]:
    """
    Execute the Phase 2 preparation:
    - Read director names from Leads
    - Explode into 1 row per director per provider in Director Enrichment

    Returns a tuple of counters:
      (leads_processed, leads_skipped_no_provider_id,
       total_director_names_parsed, created_count, skipped_existing_count)
    """
    print("[DIR] Starting director explode job (Phase 2 prep)")

    existing_keys = _fetch_existing_director_keys(token)

    print("[DIR] Fetching Leads records for director extraction")
    leads_records = list_records(
        token,
        TABLE_LEADS,
        fields=[PROVIDER_ID_FIELD, PROVIDER_NAME_FIELD, DIRECTORS_FIELD],
    )

    leads_processed = 0
    leads_skipped_no_provider_id = 0
    total_director_names_parsed = 0
    created_count = 0
    skipped_existing_count = 0

    # Track keys we intend to create in this run to avoid duplicates within the batch
    new_keys_this_run: Set[str] = set()
    records_to_create: List[Dict[str, Any]] = []

    for rec in leads_records:
        leads_processed += 1
        fields = rec.get("fields", {})

        provider_id = (fields.get(PROVIDER_ID_FIELD) or "").strip()
        if not provider_id:
            leads_skipped_no_provider_id += 1
            continue

        provider_name = (fields.get(PROVIDER_NAME_FIELD) or "").strip()
        directors_raw = (fields.get(DIRECTORS_FIELD) or "").strip()
        if not directors_raw:
            continue

        director_names = parse_director_names(directors_raw)
        if not director_names:
            continue

        total_director_names_parsed += len(director_names)

        for director_name in director_names:
            director_key = _build_director_key(provider_id, director_name)
            if not director_key:
                continue

            if director_key in existing_keys or director_key in new_keys_this_run:
                skipped_existing_count += 1
                continue

            new_keys_this_run.add(director_key)

            record_fields = {
                PROVIDER_NAME_FIELD: provider_name,
                PROVIDER_ID_FIELD: provider_id,
                DIRECTOR_NAME_FIELD: director_name,
                DIRECTOR_KEY_FIELD: director_key,
                ENRICHMENT_STATUS_FIELD: "pending",
            }
            records_to_create.append(record_fields)

    print(f"[DIR] Leads processed: {leads_processed}")
    print(f"[DIR] Leads skipped due to missing provider id: {leads_skipped_no_provider_id}")
    print(f"[DIR] Total director names parsed: {total_director_names_parsed}")
    print(f"[DIR] New director candidates after de-duplication: {len(records_to_create)}")
    print(f"[DIR] Director candidates skipped because key already existed: {skipped_existing_count}")

    # Create new Director Enrichment records in batches of 10
    batch_size = 10
    for i in range(0, len(records_to_create), batch_size):
        batch = records_to_create[i : i + batch_size]
        created_in_batch = create_records(token, TABLE_DIRECTOR_ENRICHMENT, batch)
        created_count += created_in_batch
        print(f"[DIR] Created batch of {created_in_batch} director records in Director Enrichment")

    print(f"[DIR] Finished director explode job, total created={created_count}")

    return (
        leads_processed,
        leads_skipped_no_provider_id,
        total_director_names_parsed,
        created_count,
        skipped_existing_count,
    )


def main() -> int:
    """
    Entrypoint for running the director explode job as a script.
    """
    token = os.environ.get("AIRTABLE_TOKEN")
    if not token:
        print("[DIR] AIRTABLE_TOKEN is not set; cannot sync to Airtable")
        return 1

    try:
        (
            leads_processed,
            leads_skipped_no_provider_id,
            total_director_names_parsed,
            created_count,
            skipped_existing_count,
        ) = run_director_explode(token)
    except Exception as exc:  # noqa: BLE001
        print(f"[DIR] Error while running director explode job: {exc}")
        return 1

    print("[DIR] Director explode job summary:")
    print(f"  Leads processed: {leads_processed}")
    print(f"  Leads skipped (missing provider id): {leads_skipped_no_provider_id}")
    print(f"  Total director names parsed: {total_director_names_parsed}")
    print(f"  Director records created: {created_count}")
    print(f"  Director records skipped due to existing Director Key: {skipped_existing_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

