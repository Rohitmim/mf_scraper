#!/usr/bin/env python3
"""
Sync full mutual fund list to database
Fetches ALL Direct Growth funds from MFAPI and stores metadata in mutual_funds table
"""

import argparse
from common import MFAPIClient, SupabaseDB


def main():
    parser = argparse.ArgumentParser(description="Sync full MF list to database")
    parser.add_argument("--insecure", action="store_true", help="Disable SSL verification")
    args = parser.parse_args()

    if args.insecure:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    print("Fetching full mutual fund list from MFAPI...")

    mfapi = MFAPIClient(verify_ssl=not args.insecure)
    schemes = mfapi.get_fund_list(filter_direct_growth=True)

    print(f"Found {len(schemes)} Direct Growth funds")

    # Prepare records for upsert
    fund_records = []
    for scheme in schemes:
        fund_records.append({
            "fund_name": scheme["schemeName"],
            "fund_house": "Unknown",  # Will be updated when NAV data is fetched
            "category": "Unknown",
            "scheme_code": scheme["schemeCode"]  # Store scheme code for later NAV fetching
        })

    print(f"Saving {len(fund_records)} funds to database...")

    db = SupabaseDB()

    # Batch upsert in chunks of 500
    chunk_size = 500
    saved = 0
    for i in range(0, len(fund_records), chunk_size):
        chunk = fund_records[i:i + chunk_size]
        try:
            db.client.table("mutual_funds").upsert(
                chunk, on_conflict="fund_name"
            ).execute()
            saved += len(chunk)
            print(f"  Saved {saved}/{len(fund_records)} funds...")
        except Exception as e:
            print(f"  Error saving chunk: {e}")

    print(f"\nDone! Saved {saved} funds to database.")


if __name__ == "__main__":
    main()
