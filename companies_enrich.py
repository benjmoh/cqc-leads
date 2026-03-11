import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from airtable_client import list_records, update_records


TABLE_COMPANIES = "Companies"

FIELD_PROVIDER_ID = "CQC Provider ID"
FIELD_PROVIDER_NAME = "Provider name"
FIELD_COMPANY_DOMAIN = "Company Domain"
FIELD_COMPANY_WEBSITE = "Company Website"
FIELD_COMPANY_LINKEDIN = "Company LinkedIn"
FIELD_COMPANY_STATUS = "Company Enrichment Status"
FIELD_COMPANY_ENRICHED_AT = "Company Enriched At"

SERPAPI_KEY_ENV = "SERPAPI_API_KEY"


def _serpapi_search_company(name: str) -> Dict[str, str]:
    """
    Simple SerpAPI wrapper: given provider name, return
    best-guess website/domain/LinkedIn.
    """
    api_key = os.environ.get(SERPAPI_KEY_ENV)
    if not api_key:
        print("[CO] SERPAPI_API_KEY not set; skipping SerpAPI")
        return {}

    params = {
        "engine": "google",
        "q": f"{name} care home",
        "api_key": api_key,
        "num": 10,
    }
    try:
        resp = requests.get("https://serpapi.com/search", params=params, timeout=30)
    except requests.RequestException as exc:  # noqa: BLE001
        print(f"[CO] SerpAPI network error for '{name}': {exc}")
        return {}

    if resp.status_code != 200:
        print(f"[CO] SerpAPI error for '{name}': {resp.status_code} {resp.text}")
        return {}

    data = resp.json()
    results = data.get("organic_results") or []

    website = ""
    domain = ""
    company_linkedin = ""

    for r in results:
        link = r.get("link") or ""
        if not website and link.startswith("http"):
            website = link

        if "linkedin.com/company" in link and not company_linkedin:
            company_linkedin = link

        if website and company_linkedin:
            break

    if website:
        try:
            from urllib.parse import urlparse

            parsed = urlparse(website)
            domain = parsed.netloc
        except Exception:  # noqa: BLE001
            pass

    return {
        "website": website,
        "domain": domain,
        "linkedin": company_linkedin,
    }


def enrich_companies(limit: int = 50) -> None:
    """
    Enrich up to `limit` Companies without a Company Domain or with
    non-enriched status.
    """
    print("[CO] Starting Companies enrichment")

    # Airtable formula: OR({Company Domain} = "", {Company Enrichment Status} != "enriched")
    formula = 'OR({Company Domain} = "", {Company Enrichment Status} != "enriched")'
    companies = list_records(
        TABLE_COMPANIES,
        fields=[
            FIELD_PROVIDER_ID,
            FIELD_PROVIDER_NAME,
            FIELD_COMPANY_DOMAIN,
            FIELD_COMPANY_WEBSITE,
            FIELD_COMPANY_LINKEDIN,
            FIELD_COMPANY_STATUS,
        ],
        formula=formula,
    )

    if not companies:
        print("[CO] No Companies to enrich")
        return

    to_process = companies[:limit]
    updates: List[Dict[str, Any]] = []

    for rec in to_process:
        rec_id = rec["id"]
        fields = rec.get("fields", {})
        name = (fields.get(FIELD_PROVIDER_NAME) or "").strip()
        if not name:
            continue

        print(f"[CO] Enriching company for provider '{name}' ({rec_id})")

        serp = _serpapi_search_company(name)
        if not serp:
            status = "failed"
        else:
            status = "enriched"

        new_fields: Dict[str, Any] = {
            FIELD_COMPANY_STATUS: status,
            FIELD_COMPANY_ENRICHED_AT: datetime.now(timezone.utc).isoformat(),
        }
        if serp.get("domain"):
            new_fields[FIELD_COMPANY_DOMAIN] = serp["domain"]
        if serp.get("website"):
            new_fields[FIELD_COMPANY_WEBSITE] = serp["website"]
        if serp.get("linkedin"):
            new_fields[FIELD_COMPANY_LINKEDIN] = serp["linkedin"]

        updates.append({"id": rec_id, "fields": new_fields})

    if updates:
        for i in range(0, len(updates), 10):
            batch = updates[i : i + 10]
            updated = update_records(TABLE_COMPANIES, batch)
            print(f"[CO] Updated batch of {updated} Companies records")

    print("[CO] Companies enrichment complete")


def main() -> int:
    try:
        enrich_companies()
    except Exception as exc:  # noqa: BLE001
        print(f"[CO] Error during Companies enrichment: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

