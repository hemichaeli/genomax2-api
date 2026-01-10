#!/usr/bin/env python3
"""
GenoMAX² One-Time Net Quantity Backfill Script v1.0

Fetches missing net_quantity values from Supliful product pages
and updates the database.

Usage:
    python one_time_backfill_net_quantity.py [--dry-run] [--limit N]

Arguments:
    --dry-run   Show what would be updated without making changes
    --limit N   Process only first N modules (for testing)

Process:
1. Calls GET /api/v1/qa/net-qty/missing to get modules needing data
2. For each module with supplier_url:
   - Fetches the product page
   - Extracts "Product Amount" field
   - Updates database with extracted value
3. Outputs QC report as JSON

Requirements:
    pip install requests beautifulsoup4 psycopg2-binary

Author: GenoMAX² Engineering
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
import requests
from bs4 import BeautifulSoup
import re

# Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "https://web-production-97b74.up.railway.app")
DATABASE_URL = os.getenv("DATABASE_URL")

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests
REQUEST_TIMEOUT = 20  # seconds
MAX_RETRIES = 2
RETRY_BACKOFF = 2  # exponential backoff multiplier

# Validation
MAX_NET_QUANTITY_LENGTH = 64


def get_db_connection():
    """Get database connection."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable not set")
    
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def fetch_missing_modules() -> Dict[str, Any]:
    """Fetch modules missing net_quantity from API."""
    url = f"{API_BASE_URL}/api/v1/qa/net-qty/missing"
    
    print(f"Fetching missing modules from: {url}")
    
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    return response.json()


def extract_product_amount(html_content: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract Product Amount from Supliful product page HTML.
    
    Returns:
        Tuple of (extracted_value, error_message)
        - On success: (value, None)
        - On failure: (None, error_description)
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Strategy 1: Look for "Product Amount" label in specification sections
    # Supliful uses various structures - try multiple approaches
    
    # Pattern 1: Look for exact text "Product Amount" (case-sensitive first)
    amount_labels = soup.find_all(string=re.compile(r'^Product\s*Amount\s*:?\s*$', re.IGNORECASE))
    
    for label in amount_labels:
        parent = label.parent
        if parent:
            # Check siblings
            next_sibling = parent.find_next_sibling()
            if next_sibling:
                value = next_sibling.get_text(strip=True)
                if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                    return (value, None)
            
            # Check parent's siblings
            parent_sibling = parent.parent.find_next_sibling() if parent.parent else None
            if parent_sibling:
                value = parent_sibling.get_text(strip=True)
                if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                    return (value, None)
    
    # Pattern 2: Look for dt/dd pairs (common specification format)
    for dt in soup.find_all('dt'):
        dt_text = dt.get_text(strip=True).lower()
        if 'product amount' in dt_text:
            dd = dt.find_next_sibling('dd')
            if dd:
                value = dd.get_text(strip=True)
                if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                    return (value, None)
    
    # Pattern 3: Look for table rows with "Product Amount"
    for row in soup.find_all('tr'):
        cells = row.find_all(['td', 'th'])
        for i, cell in enumerate(cells):
            cell_text = cell.get_text(strip=True).lower()
            if 'product amount' in cell_text:
                # Get next cell as value
                if i + 1 < len(cells):
                    value = cells[i + 1].get_text(strip=True)
                    if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                        return (value, None)
    
    # Pattern 4: Look for div with class containing "amount" or "specification"
    spec_divs = soup.find_all('div', class_=re.compile(r'spec|amount|detail', re.IGNORECASE))
    for div in spec_divs:
        text = div.get_text()
        match = re.search(r'Product\s*Amount\s*:?\s*([^\n]+)', text, re.IGNORECASE)
        if match:
            value = match.group(1).strip()
            if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
                return (value, None)
    
    # Pattern 5: Generic regex search in full text
    full_text = soup.get_text()
    match = re.search(r'Product\s*Amount\s*:?\s*([^\n]+)', full_text, re.IGNORECASE)
    if match:
        value = match.group(1).strip()
        # Clean up - remove any trailing labels
        value = re.split(r'\s{2,}|\t', value)[0].strip()
        if value and len(value) <= MAX_NET_QUANTITY_LENGTH:
            return (value, None)
    
    return (None, "PRODUCT_AMOUNT_NOT_FOUND")


def fetch_and_extract(supplier_url: str, retries: int = MAX_RETRIES) -> Tuple[Optional[str], Optional[str]]:
    """
    Fetch supplier URL and extract Product Amount.
    
    Returns:
        Tuple of (extracted_value, error_message)
    """
    for attempt in range(retries + 1):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            response = requests.get(supplier_url, headers=headers, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 404:
                return (None, "HTTP_404_NOT_FOUND")
            
            response.raise_for_status()
            
            return extract_product_amount(response.text)
            
        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** attempt)
                continue
            return (None, "TIMEOUT")
            
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                time.sleep(RETRY_BACKOFF ** attempt)
                continue
            return (None, f"HTTP_ERROR: {str(e)[:100]}")
    
    return (None, "MAX_RETRIES_EXCEEDED")


def update_net_quantity(conn, module_code: str, net_quantity: str) -> bool:
    """Update net_quantity in database."""
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE os_modules_v3_1
            SET net_quantity = %s, updated_at = NOW()
            WHERE module_code = %s
        """, (net_quantity, module_code))
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        print(f"  DB Error: {e}")
        conn.rollback()
        return False


def main():
    parser = argparse.ArgumentParser(description='Backfill missing net_quantity values')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be updated without making changes')
    parser.add_argument('--limit', type=int, help='Process only first N modules')
    args = parser.parse_args()
    
    print("=" * 60)
    print("GenoMAX² Net Quantity Backfill Script v1.0")
    print("=" * 60)
    print(f"API Base URL: {API_BASE_URL}")
    print(f"Dry Run: {args.dry_run}")
    print(f"Limit: {args.limit or 'None'}")
    print()
    
    # Step 1: Fetch missing modules
    try:
        missing_data = fetch_missing_modules()
    except Exception as e:
        print(f"ERROR: Failed to fetch missing modules: {e}")
        sys.exit(1)
    
    total_missing_before = missing_data["total_missing"]
    missing_with_url = missing_data["missing_with_url"]
    missing_without_url = missing_data["missing_without_url"]
    
    print(f"Total missing net_quantity: {total_missing_before}")
    print(f"  - With supplier URL: {missing_with_url}")
    print(f"  - Without supplier URL: {missing_without_url}")
    print()
    
    if missing_without_url > 0:
        print(f"WARNING: {missing_without_url} modules have no supplier URL and cannot be backfilled")
        print()
    
    if missing_with_url == 0:
        print("No modules to process. Exiting.")
        sys.exit(0)
    
    # Filter to only modules with URLs
    modules_to_process = [m for m in missing_data["modules"] if m["supplier_url"]]
    
    # Apply limit if specified
    if args.limit:
        modules_to_process = modules_to_process[:args.limit]
    
    print(f"Processing {len(modules_to_process)} modules...")
    print("-" * 60)
    
    # Connect to database (unless dry run)
    conn = None
    if not args.dry_run:
        try:
            conn = get_db_connection()
            print("Database connection established")
        except Exception as e:
            print(f"ERROR: Failed to connect to database: {e}")
            sys.exit(1)
    
    # Process each module
    results = {
        "updated": 0,
        "failed": 0,
        "failures": []
    }
    
    for i, module in enumerate(modules_to_process, 1):
        module_code = module["module_code"]
        supplier_url = module["supplier_url"]
        
        print(f"[{i}/{len(modules_to_process)}] {module_code}")
        print(f"  URL: {supplier_url}")
        
        # Fetch and extract
        value, error = fetch_and_extract(supplier_url)
        
        if error:
            print(f"  FAILED: {error}")
            results["failed"] += 1
            results["failures"].append({
                "module_code": module_code,
                "supplier_url": supplier_url,
                "error": error
            })
        else:
            print(f"  Extracted: {value}")
            
            if args.dry_run:
                print(f"  [DRY RUN] Would update: {value}")
                results["updated"] += 1
            else:
                if update_net_quantity(conn, module_code, value):
                    print(f"  Updated successfully")
                    results["updated"] += 1
                else:
                    print(f"  FAILED: Database update error")
                    results["failed"] += 1
                    results["failures"].append({
                        "module_code": module_code,
                        "supplier_url": supplier_url,
                        "error": "DB_UPDATE_FAILED"
                    })
        
        # Rate limiting
        if i < len(modules_to_process):
            time.sleep(REQUEST_DELAY)
    
    # Close database connection
    if conn:
        conn.close()
    
    # Generate QC report
    print()
    print("=" * 60)
    print("QC REPORT")
    print("=" * 60)
    
    qc_report = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "dry_run": args.dry_run,
        "total_missing_before": total_missing_before,
        "attempted": len(modules_to_process),
        "updated": results["updated"],
        "failed": results["failed"],
        "still_missing_after": total_missing_before - results["updated"] if not args.dry_run else total_missing_before,
        "modules_without_url": missing_without_url,
        "failures": results["failures"]
    }
    
    print(json.dumps(qc_report, indent=2))
    
    # Verify results (if not dry run)
    if not args.dry_run and results["updated"] > 0:
        print()
        print("Verifying results...")
        try:
            verify_data = fetch_missing_modules()
            print(f"After backfill: {verify_data['total_missing']} still missing")
            qc_report["verified_missing_after"] = verify_data["total_missing"]
        except Exception as e:
            print(f"Verification failed: {e}")
    
    print()
    print("Backfill complete.")
    
    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
