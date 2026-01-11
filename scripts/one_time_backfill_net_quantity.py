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
- Extraction uses pattern matching for amount labels
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
MAX_NET_QUANTITY_LENGTH = 100


def fetch_missing_modules() -> Dict:
    """Fetch modules missing net_quantity from API."""
    url = f"{BASE_API_URL}{MISSING_ENDPOINT}"
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def extract_product_amount(html_content: str) -> Optional[str]:
    """
    Extract Product Amount from Supliful catalog page HTML.
    
    Handles variations:
    - <strong>Product Amount</strong>:VALUE
    - <strong>Product amount (oz/lbs/g)</strong>:VALUE  
    - <strong>Amount</strong>:VALUE
    - Case insensitive matching
    
    Returns:
        Extracted value or None if not found
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all <strong> tags that might be amount labels
    for strong in soup.find_all('strong'):
        label = strong.get_text(strip=True).lower()
        
        # Match variations of amount labels
        # "amount", "product amount", "product amount (oz/lbs/g)"
        if 'amount' in label:
            parent = strong.parent
            if parent:
                full_text = parent.get_text(strip=True)
                
                # Handle "Label:Value" pattern  
                if ':' in full_text:
                    # Split only on first colon
                    parts = full_text.split(':', 1)
                    if len(parts) == 2:
                        value = parts[1].strip()
                        # Skip if it's just the format hint or empty
                        if value and value != '(oz/lbs/g)' and not value.startswith('(oz'):
                            # Validate length
                            if len(value) <= MAX_NET_QUANTITY_LENGTH:
                                return value
    
    # Fallback: Try regex on raw HTML for common patterns
    patterns = [
        r'<strong>(?:Product\s+)?Amount(?:\s*\([^)]+\))?</strong>\s*:\s*([^<]+)',
        r'(?:Product\s+)?Amount[:\s]+(\d+\s*(?:caps?|tablets?|softgels?|strips?|oz|ml|g|ct|count)[^\n<]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, html_content, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value and len(value) > 1 and len(value) <= MAX_NET_QUANTITY_LENGTH:
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
            if e.response.status_code in (404, 410, 503):  # Not found / Gone / Service unavailable
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
        "failed_404": 0,
        "failed_503": 0,
        "failed_extract": 0,
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
            # Categorize failure type
            if error and "404" in error:
                report["failed_404"] += 1
            elif error and "503" in error:
                report["failed_503"] += 1
            else:
                report["failed_extract"] += 1
            
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
    print(f"  Failed total:          {report['failed']}")
    print(f"    - 404 (removed):     {report['failed_404']}")
    print(f"    - 503 (unavailable): {report['failed_503']}")
    print(f"    - Extract failed:    {report['failed_extract']}")
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
