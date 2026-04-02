#!/usr/bin/env python3
"""
Montana Delinquent Tax Lien Scraper
Supports: Gallatin County (PDF), Flathead County (fixed-width text)

Workflow per county:
  1. Download the delinquent parcel list
  2. Extract all parcel IDs
  3. Scrape each parcel's iTax detail page
  4. Calculate tax/value ratio
  5. Merge all counties and save to docs/data.json
"""

import json
import time
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pdfplumber

# ── Headers ───────────────────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

REQUEST_DELAY  = 0.5   # seconds between iTax requests — be polite
CADASTRAL_BASE = "https://svc.mt.gov/msl/cadastral/"
OUTPUT_PATH    = Path("docs/data.json")

# ── County Definitions ────────────────────────────────────────────────────────

COUNTIES = [
    {
        "name":       "Gallatin",
        "type":       "pdf",
        "source_url": (
            "https://www.gallatinmt.gov/sites/g/files/vyhlif606/f/"
            "uploads/2020_-_2024_dlq_as_of_1-28-26.pdf"
        ),
        "cache_path": Path("docs/gallatin_delinquent.pdf"),
        "itax_base":  "https://itax.gallatin.mt.gov/detail.aspx",
        "itax_param": "taxid",
    },
    {
        "name":       "Flathead",
        "type":       "fixed_width",
        "source_url": (
            "https://flatheadcounty.gov/smbstorage/ocDownloads/"
            "ocSharedTRDocumentsAdministration/TaxWise/Unpaids/All_current"
        ),
        "cache_path": Path("docs/flathead_delinquent.txt"),
        "itax_base":  "https://taxes.flatheadcounty.gov/detail.aspx",
        "itax_param": "taxid",
    },
]

# ── Source Download ───────────────────────────────────────────────────────────

def download_source(url: str, dest: Path) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        print(f"  [DL] {url}")
        r = requests.get(url, headers=HEADERS, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        print(f"  [DL] Saved {len(r.content):,} bytes to {dest}")
        return True
    except Exception as e:
        print(f"  [DL] Failed: {e}")
        return False


def source_is_fresh(path: Path, max_age_hours: float = 12.0) -> bool:
    if not path.exists():
        return False
    age = (time.time() - path.stat().st_mtime) / 3600
    return age < max_age_hours

# ── Gallatin: PDF Parcel Extraction ──────────────────────────────────────────

def extract_ids_from_pdf(pdf_path: Path) -> list[str]:
    """
    Extract Gallatin County parcel IDs from the delinquent PDF.
    Pattern: R followed by 1-3 uppercase letters then 4-6 digits (e.g. RDC31418)
    """
    ids, seen = [], set()
    pattern = re.compile(r'\b(R[A-Z]{1,3}\d{4,6})\b')
    print(f"  [PDF] Parsing {pdf_path}")
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages, 1):
            matches = pattern.findall(page.extract_text() or "")
            for m in matches:
                if m not in seen:
                    seen.add(m)
                    ids.append(m)
            print(f"  [PDF] Page {i}: {len(ids)} unique IDs so far")
    print(f"  [PDF] Total: {len(ids)} parcel IDs")
    return ids

# ── Flathead: Fixed-Width Text Parcel Extraction ──────────────────────────────

def extract_ids_from_fixed_width(txt_path: Path) -> list[str]:
    """
    Parse Flathead County's fixed-width unpaid tax roll and return only
    parcels that are GENUINELY delinquent — i.e. the 1st installment has
    an outstanding balance. Parcels where only the 2nd installment is
    unpaid are skipped: that half likely isn't due yet and the parcel is
    current, not delinquent.

    File format (all amounts: 11 chars, 2 implied decimal places):
      Chars  1-16  : Assessor number (parcel ID), zero-padded
      Chars 17-20  : Tax year
      Chars 21-31  : Amount billed, 1st installment
      Chars 32-42  : Amount billed, 2nd installment
      Chars 43-53  : Amount paid,   1st installment
      Chars 54-64  : Amount paid,   2nd installment
      Char  65     : Assignment flag (blank / 1 / 2 / 3)
      Char  66     : Bankruptcy flag (blank / Y)
    """
    ids, seen = [], set()
    content = txt_path.read_bytes().decode("latin-1", errors="replace")
    lines = content.splitlines()
    skipped_current = 0
    skipped_bad     = 0

    print(f"  [TXT] Parsing {txt_path} — {len(lines):,} lines")

    for line in lines:
        if len(line) < 64:
            skipped_bad += 1
            continue
        raw_id = line[0:16].strip()
        if not raw_id or raw_id in seen:
            continue
        try:
            billed1 = int(line[20:31])
            paid1   = int(line[42:53])
        except ValueError:
            skipped_bad += 1
            continue

        # Only scrape parcels where the 1st installment has an unpaid balance.
        # Parcels where only the 2nd half is owed are not yet delinquent.
        if billed1 - paid1 <= 0:
            skipped_current += 1
            continue

        seen.add(raw_id)
        ids.append(raw_id)

    print(f"  [TXT] {skipped_current:,} skipped (current — only 2nd half owed)")
    print(f"  [TXT] {skipped_bad:,} skipped (malformed lines)")
    print(f"  [TXT] {len(ids):,} genuinely delinquent parcels to scrape")
    return ids


# ── iTax Scraper ──────────────────────────────────────────────────────────────

def scrape_parcel(parcel_id: str, county: dict, session: requests.Session) -> dict:
    url = f"{county['itax_base']}?{county['itax_param']}={parcel_id}"
    try:
        r = session.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return parse_itax_html(r.text, parcel_id, url, county["name"])
    except requests.RequestException as e:
        print(f"ERROR: {e}")
        return {
            "parcelId":  parcel_id,
            "county":    county["name"],
            "error":     str(e),
            "status":    "Error",
            "scrapedAt": utc_now(),
        }


def parse_itax_html(html: str, parcel_id: str, source_url: str, county_name: str) -> dict:
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

    # Owner
    owner = find(r"(?:Owner\(s\)|Owner):\s*(.+?)(?:\n|Mailing)")

    # Mailing address — stop at Levy District OR Legal Description
    mailing_match = re.search(
        r"Mailing Address:\s*(.+?)\s*(?:Levy District|Legal Description)",
        text, re.DOTALL | re.IGNORECASE
    )
    mailing = " ".join(mailing_match.group(1).split()) if mailing_match else None

    # Property address
    prop_addr = find(r"Property address:\s*(.+?)(?:\n|Subdivision|TRS|$)")

    # Market / taxable values
    market_value  = find_dollar(r"Market Value\s+\$?([\d,]+)")
    taxable_value = find_dollar(r"Taxable:\s+\$?([\d,]+)")

    # Total taxes — find the total within the "2025 Taxes" block specifically
    taxes_section = re.search(
        r"2025 Taxes.*?Total:\s+\$?([\d,]+\.?\d*)", text, re.DOTALL | re.IGNORECASE
    )
    taxes_total = float(re.sub(r"[^0-9.]", "", taxes_section.group(1))) if taxes_section else None

    # First/second half — take first two matches (taxes block comes before payments block)
    half_matches = re.findall(r"(?:First|Second) Half:\s+\$?([\d,]+\.?\d*)", text, re.IGNORECASE)
    first_half  = float(re.sub(r"[^0-9.]", "", half_matches[0])) if len(half_matches) > 0 else None
    second_half = float(re.sub(r"[^0-9.]", "", half_matches[1])) if len(half_matches) > 1 else None

    # Geocode
    geocode = find(r"Geo Code:\s*([\d\-]+)")

    # Legal details
    trs         = find(r"TRS:\s*(.+?)(?:\n|Legal|$)")
    legal       = find(r"Legal(?:\s+Description)?:\s*(.+?)(?:\n|Acres|Short|COS|$)")
    acres_str   = find(r"Acres?:\s*([\d.]+)")
    acres       = float(acres_str) if acres_str else None
    subdivision = find(r"Subdivision:\s*(?:\(\d+\)\s*)?(.+?)(?:\n|Lot|$)")
    lot         = find(r"Lot:\s*(\S+)")
    levy_district = find(r"Levy District:\s*(.+?)(?:\n|$)")

    # Status — read actual value from the page
    status = find(r"Status:\s*(\w+)") or "Unknown"

    # Cadastral URL
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
        "county":          county_name,
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
    """Load prior data keyed by 'County:parcelId' to avoid collisions across counties."""
    if path.exists():
        try:
            data = json.loads(path.read_text())
            return {f"{r.get('county','?')}:{r['parcelId']}": r for r in data.get("parcels", [])}
        except Exception:
            pass
    return {}


def scrape_county(county: dict, existing_cache: dict, session: requests.Session) -> list[dict]:
    name = county["name"]
    print(f"\n{'='*60}")
    print(f"[{name.upper()}] Starting scrape")
    print(f"{'='*60}")

    # 1. Get source file
    cache = county["cache_path"]
    if source_is_fresh(cache):
        print(f"  [CACHE] Using cached file (< 12h old)")
        fresh = True
    else:
        fresh = download_source(county["source_url"], cache)

    # 2. Extract parcel IDs
    if fresh and cache.exists():
        if county["type"] == "pdf":
            parcel_ids = extract_ids_from_pdf(cache)
        else:
            parcel_ids = extract_ids_from_fixed_width(cache)
    else:
        parcel_ids = [
            k.split(":", 1)[1]
            for k in existing_cache
            if k.startswith(f"{name}:")
        ]
        print(f"  [FALLBACK] Using {len(parcel_ids)} IDs from previous data.json")

    if not parcel_ids:
        print(f"  [WARN] No parcel IDs found for {name}")
        return []

    # 3. Scrape iTax
    results = []
    errors = 0
    for i, pid in enumerate(parcel_ids, 1):
        print(f"  [{i:4d}/{len(parcel_ids)}] {pid} ... ", end="", flush=True)
        record = scrape_parcel(pid, county, session)
        cache_key = f"{name}:{pid}"
        if record.get("error"):
            errors += 1
            if cache_key in existing_cache:
                print("ERROR (using cached)")
                results.append(existing_cache[cache_key])
            else:
                print("ERROR (no cache)")
                results.append(record)
        else:
            mv  = record.get("marketValue") or 0
            tax = record.get("totalTaxes")  or 0
            rat = record.get("ratio")       or 0
            print(f"OK  MV=${mv:,.0f}  Tax=${tax:,.2f}  Ratio={rat:.4f}%")
            results.append(record)
        time.sleep(REQUEST_DELAY)

    print(f"\n  [{name.upper()}] Done — {len(results)} parcels, {errors} errors")
    return results

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start = time.time()
    print(f"[START] {utc_now()}")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    existing_cache = load_existing(OUTPUT_PATH)
    session = requests.Session()
    all_results = []

    for county in COUNTIES:
        records = scrape_county(county, existing_cache, session)
        all_results.extend(records)

    # Sort by ratio descending across all counties
    all_results.sort(key=lambda r: r.get("ratio") or 0, reverse=True)

    # Summary stats
    valid       = [r for r in all_results if not r.get("error")]
    total_taxes = sum(r.get("totalTaxes")  or 0 for r in valid)
    total_mv    = sum(r.get("marketValue") or 0 for r in valid)
    ratios      = [r["ratio"] for r in valid if r.get("ratio")]
    avg_ratio   = sum(ratios) / len(ratios) if ratios else 0
    high_ratio  = sum(1 for r in ratios if r >= 1.0)

    # Per-county breakdown
    county_summary = {}
    for county in COUNTIES:
        name = county["name"]
        crecs = [r for r in valid if r.get("county") == name]
        county_summary[name] = {
            "parcels":          len(crecs),
            "totalTaxesOwed":   round(sum(r.get("totalTaxes")  or 0 for r in crecs), 2),
            "totalMarketValue": round(sum(r.get("marketValue") or 0 for r in crecs), 2),
        }

    output = {
        "generatedAt":       utc_now(),
        "counties":          [c["name"] for c in COUNTIES],
        "state":             "Montana",
        "totalParcels":      len(all_results),
        "successfulScrapes": len(valid),
        "errors":            len(all_results) - len(valid),
        "summary": {
            "totalTaxesOwed":   round(total_taxes, 2),
            "totalMarketValue": round(total_mv, 2),
            "avgTaxValueRatio": round(avg_ratio, 6),
            "highRatioParcels": high_ratio,
        },
        "countySummary": county_summary,
        "parcels": all_results,
    }

    OUTPUT_PATH.write_text(json.dumps(output, indent=2))
    elapsed = time.time() - start
    print(f"\n[DONE] {len(all_results)} total parcels across {len(COUNTIES)} counties")
    print(f"[DONE] {len(all_results) - len(valid)} errors | {elapsed:.1f}s elapsed")
    print(f"[DONE] Output: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
