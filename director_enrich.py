import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from airtable_client import get_record, list_records, update_records


TABLE_COMPANIES = "Companies"
TABLE_DIRECTOR_ENRICHMENT = "Director Enrichment"

FIELD_PROVIDER_ID = "CQC Provider ID"
FIELD_PROVIDER_NAME = "Provider name"
FIELD_DIRECTOR_NAME = "Director Name"
FIELD_EMAIL = "Email"
FIELD_LINKEDIN_PERSON = "LinkedIn"
FIELD_SCORE = "Score"
FIELD_ENRICH_STATUS = "Enrichment Status"
FIELD_LAST_ENRICHED_AT = "Last Enriched At"

# Companies fields
FIELD_COMPANY_DOMAIN = "Company Domain"
FIELD_COMPANY_WEBSITE = "Company Website"
FIELD_COMPANY_LINKEDIN = "Company LinkedIn"
FIELD_COMPANY_STATUS = "Company Enrichment Status"

HUNTER_API_KEY_ENV = "HUNTER_API_KEY"
OPENAI_API_KEY_ENV = "OPENAI_API_KEY"


def _find_company_by_provider_id(provider_id: str) -> Optional[Dict[str, Any]]:
    """
    Look up a Companies record by CQC Provider ID.
    """
    records = list_records(
        TABLE_COMPANIES,
        fields=[
            FIELD_PROVIDER_ID,
            FIELD_PROVIDER_NAME,
            FIELD_COMPANY_DOMAIN,
            FIELD_COMPANY_WEBSITE,
            FIELD_COMPANY_LINKEDIN,
            FIELD_COMPANY_STATUS,
        ],
        formula=f"{{{FIELD_PROVIDER_ID}}} = '{provider_id}'",
    )
    return records[0] if records else None


def _hunter_email_for_director(name: str, domain: str) -> Dict[str, Any]:
    """
    Use Hunter's Email Finder API to get a candidate email for a director.
    """
    api_key = os.environ.get(HUNTER_API_KEY_ENV)
    if not api_key:
        print("[DIR] HUNTER_API_KEY not set; skipping Hunter")
        return {}

    first, *rest = name.split()
    last = rest[-1] if rest else ""

    params: Dict[str, Any] = {
        "api_key": api_key,
        "domain": domain,
    }
    if first:
        params["first_name"] = first
    if last:
        params["last_name"] = last

    try:
        resp = requests.get(
            "https://api.hunter.io/v2/email-finder",
            params=params,
            timeout=30,
        )
    except requests.RequestException as exc:  # noqa: BLE001
        print(f"[DIR] Hunter network error for '{name}' @{domain}: {exc}")
        return {}

    if resp.status_code != 200:
        print(f"[DIR] Hunter error for '{name}' @{domain}: {resp.status_code} {resp.text}")
        return {}

    data = resp.json().get("data") or {}
    return data


def _openai_score_match(
    director_name: str,
    provider_name: str,
    company_domain: str,
    email: Optional[str],
) -> int:
    """
    Placeholder for OpenAI-based scoring.

    For now, use a simple heuristic to avoid incurring model cost until
    you are ready to wire in a real OpenAI call.
    """
    # Very simple heuristic: if we have an email, treat as reasonably confident.
    if email:
        return 80
    return 40


def _update_status_only(record_id: str, status: str) -> None:
    update_records(
        TABLE_DIRECTOR_ENRICHMENT,
        [
            {
                "id": record_id,
                "fields": {
                    FIELD_ENRICH_STATUS: status,
                    FIELD_LAST_ENRICHED_AT: datetime.now(timezone.utc).isoformat(),
                },
            },
        ],
    )


def enrich_director_record(director_record_id: str) -> Dict[str, Any]:
    """
    Enrich a single Director Enrichment record.

    Returns a dict with summary:
      {"ok": bool, "status": "enriched"/"failed", "score": int | None}
    """
    rec = get_record(TABLE_DIRECTOR_ENRICHMENT, director_record_id)
    fields = rec.get("fields", {})

    status = (fields.get(FIELD_ENRICH_STATUS) or "").lower()
    if status == "enriched":
        print(f"[DIR] Record {director_record_id} already enriched; skipping")
        return {"ok": True, "status": "enriched", "score": fields.get(FIELD_SCORE)}

    provider_id = (fields.get(FIELD_PROVIDER_ID) or "").strip()
    provider_name = (fields.get(FIELD_PROVIDER_NAME) or "").strip()
    director_name = (fields.get(FIELD_DIRECTOR_NAME) or "").strip()
    if not provider_id or not director_name:
        print(f"[DIR] Missing provider_id or director_name for {director_record_id}")
        _update_status_only(director_record_id, "failed")
        return {"ok": False, "status": "failed", "score": None}

    company = _find_company_by_provider_id(provider_id)
    if not company:
        print(f"[DIR] No company record found for provider_id={provider_id}")
        _update_status_only(director_record_id, "failed")
        return {"ok": False, "status": "failed", "score": None}

    company_fields = company.get("fields", {})
    domain = (company_fields.get(FIELD_COMPANY_DOMAIN) or "").strip()
    if not domain:
        print(f"[DIR] Company for provider_id={provider_id} has no domain; cannot enrich")
        _update_status_only(director_record_id, "failed")
        return {"ok": False, "status": "failed", "score": None}

    # Hunter: get email
    hunter_data = _hunter_email_for_director(director_name, domain)
    email = hunter_data.get("email")
    hunter_score = hunter_data.get("score") or 0

    # OpenAI: combine confidence (placeholder)
    final_score = _openai_score_match(
        director_name=director_name,
        provider_name=provider_name,
        company_domain=domain,
        email=email,
    )
    if hunter_score:
        try:
            final_score = int((final_score + int(hunter_score)) / 2)
        except (TypeError, ValueError):
            pass

    if email and final_score >= 60:
        new_status = "enriched"
    else:
        new_status = "failed"

    new_fields: Dict[str, Any] = {
        FIELD_ENRICH_STATUS: new_status,
        FIELD_LAST_ENRICHED_AT: datetime.now(timezone.utc).isoformat(),
        FIELD_SCORE: final_score,
    }
    if email:
        new_fields[FIELD_EMAIL] = email

    update_records(
        TABLE_DIRECTOR_ENRICHMENT,
        [
            {
                "id": director_record_id,
                "fields": new_fields,
            },
        ],
    )

    print(
        f"[DIR] Enriched director {director_record_id}: "
        f"status={new_status}, score={final_score}, email={email}",
    )
    return {"ok": True, "status": new_status, "score": final_score}


def main(record_id: str) -> int:
    try:
        result = enrich_director_record(record_id)
    except Exception as exc:  # noqa: BLE001
        print(f"[DIR] Error during director enrichment: {exc}")
        return 1

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit("Use enrich_director_record from FastAPI endpoint instead.")

