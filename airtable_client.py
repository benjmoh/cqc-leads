import os
from typing import Any, Dict, List, Optional

import requests
from urllib.parse import quote


AIRTABLE_BASE_ID = "apphWtLxQpxaYaJhX"
AIRTABLE_API_BASE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}"


def _get_token() -> str:
    token = os.environ.get("AIRTABLE_TOKEN")
    if not token:
        raise RuntimeError("AIRTABLE_TOKEN is not set")
    return token


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {_get_token()}",
        "Content-Type": "application/json",
    }


def list_records(
    table_name: str,
    fields: Optional[List[str]] = None,
    page_size: int = 100,
    formula: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    List all records from an Airtable table, handling pagination.

    Returns a list of Airtable record objects:
      {"id": "...", "fields": {...}}
    """
    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}"
    params: Dict[str, Any] = {"pageSize": page_size}
    if fields:
        params["fields[]"] = fields
    if formula:
        params["filterByFormula"] = formula

    records: List[Dict[str, Any]] = []
    offset: Optional[str] = None

    while True:
        if offset:
            params["offset"] = offset
        else:
            params.pop("offset", None)

        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to list records from '{table_name}': "
                f"status={resp.status_code}, body={resp.text}",
            )

        data = resp.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break

    return records


def create_records(table_name: str, field_dicts: List[Dict[str, Any]]) -> int:
    """
    Create new records in a table.

    field_dicts: list of {"Field Name": value, ...}
    """
    if not field_dicts:
        return 0

    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}"
    payload = {"records": [{"fields": f} for f in field_dicts]}
    resp = requests.post(url, headers=_headers(), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to create records in '{table_name}': "
            f"status={resp.status_code}, body={resp.text}",
        )
    data = resp.json()
    return len(data.get("records", []))


def update_records(table_name: str, records: List[Dict[str, Any]]) -> int:
    """
    Update records in a table.

    records: list of {"id": "<recId>", "fields": {...}}
    """
    if not records:
        return 0

    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}"
    payload = {"records": records}
    resp = requests.patch(url, headers=_headers(), json=payload, timeout=30)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"Failed to update records in '{table_name}': "
            f"status={resp.status_code}, body={resp.text}",
        )
    data = resp.json()
    return len(data.get("records", []))


def get_record(table_name: str, record_id: str) -> Dict[str, Any]:
    """Fetch a single record by ID."""
    url = f"{AIRTABLE_API_BASE_URL}/{quote(table_name, safe='')}/{record_id}"
    resp = requests.get(url, headers=_headers(), timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Failed to get record {record_id} from '{table_name}': "
            f"status={resp.status_code}, body={resp.text}",
        )
    return resp.json()

