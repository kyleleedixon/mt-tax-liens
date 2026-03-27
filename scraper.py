#!/usr/bin/env python3
"""
Montana Delinquent Tax Lien Scraper
Gallatin County — itax.gallatin.mt.gov

Workflow:
  1. Download the delinquent parcel PDF from Gallatin County
  2. Extract all ParcelNo values using pdfplumber
  3. Scrape each parcel's detail page from iTax
  4. Calculate tax/value ratio
  5. Save results to docs/data.json (served by GitHub Pages)
"""

import json
import time
import re
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pdfplumber

# ── Configuration ────────────────────────────────────────────────────────────

PDF_URL = (
    "https://www.gallatinmt.gov/sites/g/files/vyhlif606/f/"
    "uploads/2020_-_2024_dlq_as_of_1-28-26.pdf"
)
ITAX_BASE = "https://itax.gallatin.mt.gov/detail.aspx"
CADASTRAL_BASE = "https://svc.mt.gov/msl/cadastral/"
OUTPUT_PATH = Path("docs/data.json")
PDF_CACHE   = Path("docs/delinquent.pdf")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; MT-TaxLien-Bot/1.0; "
        "+https://github.com/your-username/mt-tax-liens)"
    )
}
REQUEST_DELAY = 0.5   # seconds between iTax requests — be polite

# ── PDF Download ─────────────────────────────────────────────────────────────

def download_pdf(url: str, dest: Path) -> bool:
    """Download the delinquent parcel PDF. Returns True on success."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"[PDF] Downloading: {url}")
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        print(f"[PDF] Saved to {dest} ({len(r.content):,} bytes)")
        return True
    except Exception as e:
        print(f"[PDF] Download failed: {e}")
        return False


def extract_parcel_ids_from_pdf(pdf_path: Path) -> list[str]:
    """
    Extract ParcelNo values from the Gallatin County delinquent PDF.
    The PDF has rows like:  RDC31418  OWNER NAME  ...
    Parcel IDs match the pattern RDC###### (or similar alphanumeric codes).
    """
    ids = []
    seen = set()
    # Pattern covers common Gallatin parcel formats: RDC##### or RG##### etc.
    pattern = re.compile(r'\b(R[A-Z]{1,3}\d{4,6})\b')

    print(f"[PDF] Extracting parcel IDs from {pdf_path}")
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ""
            matches = pattern.findall(text)
            for m in matches:
                if m not in seen:
                    seen.add(m)
                    ids.append(m)
            print(f"[PDF] Page {page_num}: found {len(matches)} IDs (running total: {len(ids)})")

    print(f"[PDF] Total unique parcel IDs extracted: {len(ids)}")
    return ids


# ── iTax Scraper ─────────────────────────────────────────────────────────────

def scrape_parcel(parcel_id: str, session: requests.Session) -> dict:
    """Scrape a single parcel detail page from iTax."""
    url = f"{ITAX_BASE}?taxid={parcel_id}"
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return parse_itax_html(r.text, parcel_id, url)
    except requests.RequestException as e:
        print(f"  [SKIP] {parcel_id}: {e}")
        return {
            "parcelId": parcel_id,
            "error": str(e),
            "status": "Error",
            "scrapedAt": utc_now()
        }


def parse_itax_html(html: str, parcel_id: str, source_url: str) -> dict:
    """Parse an iTax detail page into a structured dict."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")

    def find(pattern, default=None):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1).strip() if m else default

    def find_dollar(pattern):
        raw = find(pattern)
        if raw is None:
            return None
        try:
            return float(re.sub(r"[^0-9.]", "", raw))
        except ValueError:
            return None

    # Owner — appears after "Owner(s):"
    owner = find(r"Owner\(s\):\s*(.+?)(?:\n|Mailing)")

    # Mailing address — multi-line, grab up to Levy
    mailing_match = re.search(
        r"Mailing Address:\s*(.+?)\s*Levy District", text, re.DOTALL | re.IGNORECASE
    )
    mailing = " ".join(mailing_match.group(1).split()) if mailing_match else None

    # Property address
    prop_addr = find(r"Property address:\s*(.+?)(?:\n|Subdivision|TRS)")

    # Values
    market_value  = find_dollar(r"Market Value\s+\$?([\d,]+)")
    taxable_value = find_dollar(r"Taxable:\s+\$?([\d,]+)")

    # Taxes — careful: "Total:" appears for payments too, grab the taxes total
    taxes_total  = find_dollar(r"(?:2025 Taxes|Total):\s+\$?([\d,]+\.?\d*)")
    first_half   = find_dollar(r"First Half:\s+\$?([\d,]+\.?\d*)")
    second_half  = find_dollar(r"Second Half:\s+\$?([\d,]+\.?\d*)")

    # Geocode
    geocode = find(r"Geo Code:\s*([\d\-]+)")

    # Legal
    trs          = find(r"TRS:\s*(.+?)(?:\n|Legal)")
    legal        = find(r"Legal:\s*(.+?)(?:\n|Acres)")
    acres_str    = find(r"Acres?:\s*([\d.]+)")
    acres        = float(acres_str) if acres_str else None
    subdivision  = find(r"Subdivision:\s*\(\d+\)\s*(.+?)(?:\n|Lot)")
    lot          = find(r"Lot:\s*(\S+)")
    levy_district = find(r"Levy District:\s*(.+?)(?:\n|$)")

    # Status
    status = find(r"Status:\s*(\w+)") or "Delinquent"

    # Cadastral map URL
    cadastral_url = (
        f"{CADASTRAL_BASE}?page=Map&geocode={geocode}&taxYear=2026"
        if geocode else None
    )

    # Tax / value ratio
    ratio = None
    if market_value and market_value > 0 and taxes_total:
        ratio = round((taxes_total / market_value) * 100, 6)

    return {
        "parcelId":        parcel_id,
        "owner":           owner,
        "mailingAddress":  mailing,
        "propertyAddress": prop_addr,
        "status":          status,
        "marketValue":     market_value,
        "taxableValue":    taxable_value,
        "totalTaxes":      taxes_total,
        "firstHalf":       first_half,
        "secondHalf":      second_half,
        "ratio":           ratio,
        "geocode":         geocode,
        "trs":             trs,
        "legal":           legal,
        "acres":           acres,
        "subdivision":     subdivision,
        "lot":             lot,
        "levyDistrict":    levy_district,
        "taxYear":         2025,
        "itaxUrl":         source_url,
        "cadastralUrl":    cadastral_url,
        "scrapedAt":       utc_now(),
    }


# ── Helpers ───────────────────────────────────────────────────────────────────

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_existing(path: Path) -> dict:
    """Load previously scraped data so we can preserve successful records."""
    if path.exists():
        try:
            return {r["parcelId"]: r for r in json.loads(path.read_text())["parcels"]}
        except Exception:
            pass
    return {}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    print(f"[START] {utc_now()}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # 1. Download PDF (skip if cached and less than 12 hours old)
    pdf_fresh = False
    if PDF_CACHE.exists():
        age_hours = (time.time() - PDF_CACHE.stat().st_mtime) / 3600
        if age_hours < 12:
            print(f"[PDF] Using cached PDF (age: {age_hours:.1f}h)")
            pdf_fresh = True

    if not pdf_fresh:
        pdf_fresh = download_pdf(PDF_URL, PDF_CACHE)

    # 2. Extract parcel IDs
    if pdf_fresh and PDF_CACHE.exists():
        parcel_ids = extract_parcel_ids_from_pdf(PDF_CACHE)
    else:
        # Fallback: load IDs from existing data.json if PDF unavailable
        existing = load_existing(OUTPUT_PATH)
        parcel_ids = list(existing.keys())
        print(f"[FALLBACK] Using {len(parcel_ids)} IDs from previous data.json")

    if not parcel_ids:
        print("[ERROR] No parcel IDs found. Exiting.")
        return

    # 3. Scrape iTax — load existing so we don't lose data on errors
    existing_cache = load_existing(OUTPUT_PATH)
    results = []
    session = requests.Session()
    errors = 0

    for i, pid in enumerate(parcel_ids, 1):
        print(f"[{i:4d}/{len(parcel_ids)}] {pid}", end=" ... ", flush=True)
        record = scrape_parcel(pid, session)
        if record.get("error"):
            errors += 1
            # Fall back to cached version if available
            if pid in existing_cache:
                print(f"ERROR (using cached)")
                results.append(existing_cache[pid])
            else:
                print(f"ERROR (no cache)")
                results.append(record)
        else:
            print(f"OK  MV=${record.get('marketValue') or 0:,.0f}  "
                  f"Tax=${record.get('totalTaxes') or 0:,.2f}  "
                  f"Ratio={record.get('ratio') or 0:.4f}%")
            results.append(record)
        time.sleep(REQUEST_DELAY)

    # 4. Sort by ratio descending (best investment opportunities first)
    results.sort(key=lambda r: r.get("ratio") or 0, reverse=True)

    # 5. Build summary stats
    valid = [r for r in results if not r.get("error")]
    total_taxes = sum(r.get("totalTaxes") or 0 for r in valid)
    total_mv    = sum(r.get("marketValue") or 0 for r in valid)
    ratios      = [r["ratio"] for r in valid if r.get("ratio")]
    avg_ratio   = sum(ratios) / len(ratios) if ratios else 0
    high_ratio  = sum(1 for r in ratios if r >= 1.0)

    output = {
        "generatedAt": utc_now(),
        "county": "Gallatin",
        "state": "Montana",
        "sourceUrl": PDF_URL,
        "totalParcels": len(results),
        "successfulScrapes": len(valid),
        "errors": errors,
        "summary": {
            "totalTaxesOwed": round(total_taxes, 2),
            "totalMarketValue": round(total_mv, 2),
            "avgTaxValueRatio": round(avg_ratio, 6),
            "highRatioParcels": high_ratio,
        },
        "parcels": results,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2))
    elapsed = time.time() - start
    print(f"\n[DONE] {len(results)} parcels saved to {OUTPUT_PATH}")
    print(f"[DONE] {errors} errors | {elapsed:.1f}s elapsed")


if __name__ == "__main__":
    main()
