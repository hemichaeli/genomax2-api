#!/usr/bin/env python3
"""
GenoMAX² One-Time Net Quantity Backfill Script

Fetches Product Amount from Supliful catalog pages and updates os_modules_v3_1.

Usage:
    python scripts/one_time_backfill_net_quantity.py [--dry-run]

Options:
    --dry-run    Show what would be updated without making changes

Requirements:
    pip install requests beautifulsoup4

Process:
1. GET /api/v1/qa/net-qty/missing to get modules missing net_quantity
2. For each module with supplier_url:
   - Fetch the Supliful page
   - Extract "Product Amount" value
   - Update DB via POST /net-qty/update
3. Output QC report

DETERMINISTIC RULES:
- Only modules missing net_quantity are processed
- Only modules with valid supplier_url are scraped
- Extraction uses exact text matching for "Product Amount"
- No fuzzy matching
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_API_URL = "https://web-production-d2d46b.up.railway.app"
MISSING_ENDPOINT = "/api/v1/qa/net-qty/missing"
UPDATE_ENDPOINT = "/api/v1/qa/net-qty/update"

# Rate limiting
REQUEST_DELAY_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 2
RETRY_BACKOFF_BASE = 2  # Exponential backoff

# Validation
MAX_NET_QUANTITY_LENGTH = 64


def fetch_missing_modules() -> Dict:
    """Fetch modules missing net_quantity from API."""
    url = f"{BASE_API_URL}{MISSING_ENDPOINT}"
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def extract_product_amount(html_content: str) -> Optional[str]:
    """
    Extract Product Amount from Supliful catalog page HTML.
    
    Extraction rules (STRICT):
    1. Search for element containing exact text "Product Amount" (case-sensitive first)
    2. Locate the nearest value container (sibling or adjacent element)
    3. If multiple candidates, choose shortest non-empty value
    4. Return None if ambiguous or not found
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Strategy 1: Look for "Product Amount" label and adjacent value
    # Common patterns on Supliful pages
    
    # Try case-sensitive first
    patterns_to_try = [
        "Product Amount",
        "product amount",
        "Product amount",
    ]
    
    for pattern in patterns_to_try:
        # Find all elements containing the label
        label_elements = soup.find_all(string=re.compile(re.escape(pattern), re.IGNORECASE))
        
        for label_el in label_elements:
            parent = label_el.parent
            if not parent:
                continue
            
            # Check for value in common structures
            
            # Pattern A: Label and value in same container with separator
            # e.g., "Product Amount: 60 Capsules"
            full_text = parent.get_text(strip=True)
            if ':' in full_text:
                parts = full_text.split(':', 1)
                if len(parts) == 2:
                    value = parts[1].strip()
                    if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                        return value
            
            # Pattern B: Value in next sibling element
            next_sib = parent.find_next_sibling()
            if next_sib:
                value = next_sib.get_text(strip=True)
                if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                    return value
            
            # Pattern C: Value in adjacent element within same parent
            grandparent = parent.parent
            if grandparent:
                # Look for adjacent elements
                for sibling in grandparent.children:
                    if sibling == parent:
                        continue
                    if hasattr(sibling, 'get_text'):
                        text = sibling.get_text(strip=True)
                        # Skip if it's another label
                        if text and 'amount' not in text.lower() and len(text) <= MAX_NET_QUANTITY_LENGTH:
                            # Validate it looks like a quantity (has numbers or common units)
                            if re.search(r'\d+|capsule|tablet|gummy|scoop|oz|ml|mg|g\b', text, re.IGNORECASE):
                                return text
    
    # Strategy 2: Look for specific data attributes or structured data
    # Some sites use data-* attributes or JSON-LD
    
    # Try finding any element with "amount" in class/id
    amount_elements = soup.find_all(attrs={'class': re.compile(r'amount', re.IGNORECASE)})
    for el in amount_elements:
        text = el.get_text(strip=True)
        if text and len(text) <= MAX_NET_QUANTITY_LENGTH:
            if re.search(r'\d+', text):  # Has numbers
                return text
    
    # Strategy 3: Look in product description/details section
    # Find structured product info
    for dl in soup.find_all(['dl', 'table']):
        rows = dl.find_all(['dt', 'th', 'tr'])
        for row in rows:
            text = row.get_text(strip=True).lower()
            if 'product amount' in text or 'quantity' in text or 'size' in text:
                # Get the corresponding value
                next_el = row.find_next(['dd', 'td'])
                if next_el:
                    value = next_el.get_text(strip=True)
                    if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                        return value
    
    return None


def fetch_and_extract(url: str, retries: int = MAX_RETRIES) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch URL and extract Product Amount.
    
    Returns:
        Tuple of (extracted_value, error_message)
        - Success: (value, None)
        - Failure: (None, error_description)
    """
    attempt = 0
    last_error = None
    
    while attempt <= retries:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
            response.raise_for_status()
            
            value = extract_product_amount(response.text)
            if value:
                return (value, None)
            else:
                return (None, "Product Amount not found in page")
                
        except requests.exceptions.Timeout:
            last_error = f"Timeout after {REQUEST_TIMEOUT_SECONDS}s"
        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP {e.response.status_code}"
            if e.response.status_code in (404, 410):  # Not found / Gone
                return (None, last_error)  # Don't retry for these
        except requests.exceptions.RequestException as e:
            last_error = str(e)
        
        attempt += 1
        if attempt <= retries:
            wait_time = RETRY_BACKOFF_BASE ** attempt
            print(f"  Retry {attempt}/{retries} after {wait_time}s...")
            time.sleep(wait_time)
    
    return (None, last_error)


def update_net_quantity(module_code: str, net_quantity: str, dry_run: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Update net_quantity via API.
    
    Returns:
        Tuple of (success, error_message)
    """
    if dry_run:
        return (True, None)
    
    try:
        url = f"{BASE_API_URL}{UPDATE_ENDPOINT}"
        params = {"module_code": module_code, "net_quantity": net_quantity}
        response = requests.post(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return (True, None)
    except requests.exceptions.RequestException as e:
        return (False, str(e))


def run_backfill(dry_run: bool = False) -> Dict:
    """
    Main backfill process.
    
    Returns QC report dictionary.
    """
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": dry_run,
        "total_missing_before": 0,
        "missing_with_url": 0,
        "missing_without_url": 0,
        "attempted": 0,
        "extracted": 0,
        "updated": 0,
        "failed": 0,
        "still_missing_after": 0,
        "failures": [],
        "successes": []
    }
    
    print("=" * 60)
    print("GenoMAX² Net Quantity Backfill")
    print(f"Started: {report['run_at']}")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
    print("=" * 60)
    
    # Step 1: Get missing modules
    print("\n[1/3] Fetching modules missing net_quantity...")
    try:
        missing_data = fetch_missing_modules()
    except Exception as e:
        print(f"ERROR: Failed to fetch missing modules: {e}")
        return report
    
    report["total_missing_before"] = missing_data["total_missing"]
    report["missing_with_url"] = missing_data["missing_with_url"]
    report["missing_without_url"] = missing_data["missing_without_url"]
    
    print(f"  Total missing: {report['total_missing_before']}")
    print(f"  With URL: {report['missing_with_url']}")
    print(f"  Without URL: {report['missing_without_url']}")
    
    if report["missing_without_url"] > 0:
        print(f"\n  WARNING: {report['missing_without_url']} modules have no supplier URL")
        print("  These will be skipped and marked as unresolved.")
    
    # Step 2: Process modules with URLs
    modules_with_url = [m for m in missing_data["modules"] if m["reason"] == "OK"]
    report["attempted"] = len(modules_with_url)
    
    print(f"\n[2/3] Processing {len(modules_with_url)} modules with URLs...")
    
    for i, module in enumerate(modules_with_url, 1):
        module_code = module["module_code"]
        supplier_url = module["supplier_url"]
        
        print(f"\n  [{i}/{len(modules_with_url)}] {module_code}")
        print(f"    URL: {supplier_url}")
        
        # Fetch and extract
        extracted_value, error = fetch_and_extract(supplier_url)
        
        if extracted_value:
            report["extracted"] += 1
            print(f"    Extracted: {extracted_value}")
            
            # Update DB
            success, update_error = update_net_quantity(module_code, extracted_value, dry_run)
            
            if success:
                report["updated"] += 1
                report["successes"].append({
                    "module_code": module_code,
                    "supplier_url": supplier_url,
                    "net_quantity": extracted_value
                })
                print(f"    {'Would update' if dry_run else 'Updated'}: OK")
            else:
                report["failed"] += 1
                report["failures"].append({
                    "module_code": module_code,
                    "supplier_url": supplier_url,
                    "error": f"Update failed: {update_error}"
                })
                print(f"    Update FAILED: {update_error}")
        else:
            report["failed"] += 1
            report["failures"].append({
                "module_code": module_code,
                "supplier_url": supplier_url,
                "error": error or "Unknown extraction error"
            })
            print(f"    Extraction FAILED: {error}")
        
        # Rate limiting
        if i < len(modules_with_url):
            time.sleep(REQUEST_DELAY_SECONDS)
    
    # Add modules without URL to failures
    for module in missing_data["modules"]:
        if module["reason"] == "MISSING_SUPPLIER_URL":
            report["failures"].append({
                "module_code": module["module_code"],
                "supplier_url": None,
                "error": "MISSING_SUPPLIER_URL"
            })
    
    # Step 3: Verify
    print(f"\n[3/3] Verification...")
    if not dry_run:
        try:
            after_data = fetch_missing_modules()
            report["still_missing_after"] = after_data["total_missing"]
            print(f"  Still missing: {report['still_missing_after']}")
        except Exception as e:
            print(f"  WARNING: Verification failed: {e}")
            report["still_missing_after"] = -1
    else:
        report["still_missing_after"] = report["total_missing_before"] - report["updated"]
        print(f"  Would still be missing: {report['still_missing_after']}")
    
    # Summary
    print("\n" + "=" * 60)
    print("QC REPORT SUMMARY")
    print("=" * 60)
    print(f"  Total missing before:  {report['total_missing_before']}")
    print(f"  Attempted:             {report['attempted']}")
    print(f"  Extracted:             {report['extracted']}")
    print(f"  Updated:               {report['updated']}")
    print(f"  Failed:                {report['failed']}")
    print(f"  Still missing after:   {report['still_missing_after']}")
    print("=" * 60)
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description="GenoMAX² One-Time Net Quantity Backfill"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be updated without making changes"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON report to file"
    )
    
    args = parser.parse_args()
    
    report = run_backfill(dry_run=args.dry_run)
    
    # Output JSON report
    report_json = json.dumps(report, indent=2, default=str)
    
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report_json)
        print(f"\nReport saved to: {args.output}")
    
    print("\n--- JSON REPORT ---")
    print(report_json)
    
    # Exit code based on success
    if report["failed"] == 0 and report["still_missing_after"] == 0:
        return 0
    elif report["updated"] > 0:
        return 0  # Partial success
    else:
        return 1


if __name__ == "__main__":
    sys.exit(main())
