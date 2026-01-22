#!/usr/bin/env python3
"""
Extract and update fund focus/theme from fund names
"""

import re
from common import SupabaseDB

# Priority-ordered list of focus keywords to extract
# More specific terms first, then general terms
FOCUS_KEYWORDS = [
    # Sector specific
    ("Infrastructure", ["infrastructure", "infra"]),
    ("Banking", ["banking", "bank", "psu bank", "private bank"]),
    ("Financial", ["financial", "finance"]),
    ("Pharma", ["pharma", "healthcare", "health care"]),
    ("IT", ["technology", "tech", "digital", " it "]),
    ("FMCG", ["fmcg", "consumption", "consumer"]),
    ("Auto", ["auto", "automobile"]),
    ("Energy", ["energy", "power", "oil", "gas"]),
    ("Realty", ["realty", "real estate", "housing"]),
    ("Metal", ["metal", "steel"]),
    ("Manufacturing", ["manufacturing", "make in india"]),
    ("PSU", ["psu", "public sector"]),
    ("MNC", ["mnc", "multinational"]),

    # Commodities
    ("Gold", ["gold"]),
    ("Silver", ["silver"]),
    ("Commodities", ["commodit"]),

    # Market cap
    ("Large Cap", ["large cap", "largecap", "large-cap", "bluechip", "blue chip"]),
    ("Mid Cap", ["mid cap", "midcap", "mid-cap"]),
    ("Small Cap", ["small cap", "smallcap", "small-cap"]),
    ("Micro Cap", ["micro cap", "microcap"]),
    ("Large & Mid", ["large & mid", "large and mid"]),
    ("Mid & Small", ["mid & small", "flexi cap"]),
    ("Multi Cap", ["multi cap", "multicap", "multi-cap"]),
    ("Flexi Cap", ["flexi cap", "flexicap", "flexi-cap"]),

    # Strategy
    ("Contra", ["contra", "contrarian"]),
    ("Value", ["value"]),
    ("Growth", ["growth"]),
    ("Dividend", ["dividend"]),
    ("Focused", ["focused", "focus"]),
    ("Momentum", ["momentum"]),
    ("Quant", ["quant"]),
    ("ESG", ["esg", "sustainable", "sustainability"]),
    ("Special Situations", ["special situation", "opportunities"]),

    # Thematic
    ("Equity Savings", ["equity savings"]),
    ("Balanced", ["balanced", "hybrid", "aggressive hybrid"]),
    ("Arbitrage", ["arbitrage"]),
    ("Dynamic Asset", ["dynamic asset", "dynamic bond"]),
    ("Debt", ["debt", "bond", "income", "gilt", "liquid", "money market", "overnight"]),
    ("Tax Saver", ["tax", "elss"]),
    ("Retirement", ["retirement", "pension"]),
    ("Children", ["children", "child"]),

    # Index
    ("Nifty 50", ["nifty 50", "nifty50"]),
    ("Nifty Next 50", ["nifty next 50", "next 50"]),
    ("Nifty 100", ["nifty 100"]),
    ("Nifty 500", ["nifty 500"]),
    ("Sensex", ["sensex", "bse"]),
    ("Index", ["index"]),

    # International
    ("US", ["us ", "usa", "america", "nasdaq", "s&p"]),
    ("China", ["china", "greater china"]),
    ("Global", ["global", "international", "world", "emerging market"]),
    ("Asia", ["asia", "asean"]),
    ("Europe", ["europe"]),
]


def extract_focus(fund_name):
    """Extract the primary focus/theme from a fund name"""
    name_lower = fund_name.lower()

    # Try each keyword in priority order
    for focus, keywords in FOCUS_KEYWORDS:
        for keyword in keywords:
            if keyword in name_lower:
                return focus

    # Default fallback based on category-like patterns
    if "equity" in name_lower:
        return "Equity"

    return "Other"


def main():
    print("=" * 60)
    print("  Fund Focus Extractor")
    print("=" * 60)

    db = SupabaseDB()

    # First, run the migration to add the focus column
    print("\n1. Ensuring focus column exists...")
    try:
        # Try to select focus column - if it fails, we need to add it
        test = db.client.table("mutual_funds").select("focus").limit(1).execute()
        print("   Focus column already exists")
    except:
        print("   Please run migration 006_fund_focus.sql in Supabase first!")
        print("   SQL: ALTER TABLE mutual_funds ADD COLUMN IF NOT EXISTS focus TEXT;")
        return

    # Get all funds
    print("\n2. Fetching all funds...")
    funds = []
    offset = 0
    while True:
        batch = db.client.table("mutual_funds").select("id,fund_name,focus").range(offset, offset + 999).execute().data
        if not batch:
            break
        funds.extend(batch)
        offset += 1000
        if len(batch) < 1000:
            break

    print(f"   Found {len(funds)} funds")

    # Extract focus for funds without it
    print("\n3. Extracting focus for funds...")
    updates = []
    focus_counts = {}

    for fund in funds:
        if fund.get("focus"):  # Already has focus
            focus = fund["focus"]
        else:
            focus = extract_focus(fund["fund_name"])
            updates.append({"id": fund["id"], "focus": focus})

        focus_counts[focus] = focus_counts.get(focus, 0) + 1

    print(f"   Need to update {len(updates)} funds")

    # Show focus distribution
    print("\n4. Focus distribution:")
    for focus, count in sorted(focus_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"   {focus}: {count}")

    # Update database
    if updates:
        print(f"\n5. Updating database...")
        batch_size = 100
        updated = 0
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i+batch_size]
            for update in batch:
                db.client.table("mutual_funds").update({"focus": update["focus"]}).eq("id", update["id"]).execute()
                updated += 1
            print(f"   Updated {updated}/{len(updates)}...")

        print(f"   Done! Updated {updated} funds")
    else:
        print("\n5. All funds already have focus values")

    print("\n" + "=" * 60)
    print("  Complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
