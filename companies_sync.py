from typing import Any, Dict, List

from airtable_client import create_records, list_records, update_records


TABLE_LEADS = "Leads"
TABLE_COMPANIES = "Companies"

FIELD_PROVIDER_ID = "CQC Provider ID"
FIELD_PROVIDER_NAME_LEADS = "Provider name"
FIELD_PROVIDER_NAME_COMPANIES = "Provider name"


def sync_companies_from_leads() -> None:
    """
    Upsert one Companies record per provider based on Leads.

    - Keyed by CQC Provider ID
    - Keeps Provider name in sync
    """
    print("[CO] Syncing Companies from Leads")

    leads = list_records(
        TABLE_LEADS,
        fields=[FIELD_PROVIDER_ID, FIELD_PROVIDER_NAME_LEADS],
    )

    providers_from_leads: Dict[str, str] = {}
    for rec in leads:
        fields = rec.get("fields", {})
        pid = (fields.get(FIELD_PROVIDER_ID) or "").strip()
        if not pid:
            continue
        name = (fields.get(FIELD_PROVIDER_NAME_LEADS) or "").strip()
        if not name:
            continue
        providers_from_leads[pid] = name

    if not providers_from_leads:
        print("[CO] No providers found in Leads")
        return

    companies = list_records(
        TABLE_COMPANIES,
        fields=[FIELD_PROVIDER_ID, FIELD_PROVIDER_NAME_COMPANIES],
    )

    existing_by_pid: Dict[str, Dict[str, Any]] = {}
    for rec in companies:
        fields = rec.get("fields", {})
        pid = (fields.get(FIELD_PROVIDER_ID) or "").strip()
        if not pid:
            continue
        existing_by_pid[pid] = rec

    to_create: List[Dict[str, Any]] = []
    to_update: List[Dict[str, Any]] = []

    for pid, name in providers_from_leads.items():
        existing = existing_by_pid.get(pid)
        if existing:
            fields = existing.get("fields", {})
            current_name = (fields.get(FIELD_PROVIDER_NAME_COMPANIES) or "").strip()
            if current_name != name:
                to_update.append(
                    {
                        "id": existing["id"],
                        "fields": {
                            FIELD_PROVIDER_ID: pid,
                            FIELD_PROVIDER_NAME_COMPANIES: name,
                        },
                    },
                )
        else:
            to_create.append(
                {
                    FIELD_PROVIDER_ID: pid,
                    FIELD_PROVIDER_NAME_COMPANIES: name,
                },
            )

    if to_create:
        print(f"[CO] Creating {len(to_create)} new Companies records")
        for i in range(0, len(to_create), 10):
            batch = to_create[i : i + 10]
            created = create_records(TABLE_COMPANIES, batch)
            print(f"[CO] Created batch of {created} Companies records")

    if to_update:
        print(f"[CO] Updating {len(to_update)} Companies records")
        for i in range(0, len(to_update), 10):
            batch = to_update[i : i + 10]
            updated = update_records(TABLE_COMPANIES, batch)
            print(f"[CO] Updated batch of {updated} Companies records")

    print("[CO] Companies sync complete")

