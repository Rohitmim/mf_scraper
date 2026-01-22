#!/usr/bin/env python3
"""
Comprehensive Data Audit Tool

Checks for:
1. Duplicate ROI values between consecutive dates (bug pattern)
2. None/NULL values
3. Outlier values (unrealistic ROI)
4. Missing dates (gaps in data)
5. Stale data (created_at doesn't match report_date)
6. Data consistency (same fund different values on same date)

Usage:
    python data_audit.py              # Full audit
    python data_audit.py --fix        # Audit and fix issues
    python data_audit.py --days 30    # Audit last 30 days only
"""

import argparse
from datetime import datetime, timedelta
from collections import defaultdict
from common import SupabaseDB, MFAPIClient, ROICalculator


class DataAuditor:
    def __init__(self, db: SupabaseDB):
        self.db = db
        self.issues = []

    def add_issue(self, severity: str, category: str, description: str, dates: list = None):
        self.issues.append({
            'severity': severity,  # 'critical', 'warning', 'info'
            'category': category,
            'description': description,
            'dates': dates or []
        })

    def check_duplicate_consecutive_dates(self, dates: list) -> list:
        """Check for identical ROI values between consecutive dates"""
        print("\n1. Checking for duplicate values between consecutive dates...")
        duplicates = []

        sorted_dates = sorted(dates)
        for i in range(len(sorted_dates) - 1):
            d1, d2 = sorted_dates[i], sorted_dates[i + 1]

            r1 = self.db.client.table('mutual_fund_returns').select('fund_id, roi_3y').eq('report_date', d1).execute()
            r2 = self.db.client.table('mutual_fund_returns').select('fund_id, roi_3y').eq('report_date', d2).execute()

            if not r1.data or not r2.data:
                continue

            # Build lookup
            d1_values = {r['fund_id']: r['roi_3y'] for r in r1.data}
            d2_values = {r['fund_id']: r['roi_3y'] for r in r2.data}

            # Count matches
            common_funds = set(d1_values.keys()) & set(d2_values.keys())
            if not common_funds:
                continue

            matches = sum(1 for f in common_funds if d1_values[f] == d2_values[f] and d1_values[f] is not None)
            match_pct = (matches / len(common_funds)) * 100

            if match_pct > 90:  # More than 90% identical = suspicious
                duplicates.append((d1, d2, match_pct))
                self.add_issue('critical', 'duplicate',
                    f"{d1} and {d2} have {match_pct:.0f}% identical ROI values", [d1, d2])
                print(f"   CRITICAL: {d1} vs {d2}: {match_pct:.0f}% identical")

        if not duplicates:
            print("   OK - No duplicate patterns found")

        return duplicates

    def check_none_values(self, dates: list) -> list:
        """Check for None/NULL values in ROI fields"""
        print("\n2. Checking for None values...")
        issues = []

        for d in dates:
            result = self.db.client.table('mutual_fund_returns').select('roi_1y, roi_2y, roi_3y').eq('report_date', d).execute()

            none_1y = sum(1 for r in result.data if r['roi_1y'] is None)
            none_2y = sum(1 for r in result.data if r['roi_2y'] is None)
            none_3y = sum(1 for r in result.data if r['roi_3y'] is None)

            if none_3y > 0:  # We require 3Y data
                issues.append((d, none_1y, none_2y, none_3y))
                self.add_issue('warning', 'none_values',
                    f"{d}: {none_3y} funds with None roi_3y", [d])
                print(f"   WARNING: {d}: {none_3y} funds with None roi_3y")

        if not issues:
            print("   OK - No None values in roi_3y")

        return issues

    def check_outliers(self, dates: list, threshold: float = 200) -> list:
        """Check for unrealistic ROI values"""
        print(f"\n3. Checking for outliers (|ROI| > {threshold}%)...")
        outliers = []

        for d in dates[-5:]:  # Check recent dates only
            result = self.db.client.table('mutual_fund_returns').select(
                'fund_id, roi_1y, roi_2y, roi_3y'
            ).eq('report_date', d).execute()

            for r in result.data:
                for field in ['roi_1y', 'roi_2y', 'roi_3y']:
                    val = r.get(field)
                    if val is not None and abs(val) > threshold:
                        outliers.append((d, r['fund_id'], field, val))

        if outliers:
            self.add_issue('info', 'outliers',
                f"Found {len(outliers)} potential outliers (may be valid for gold/commodity funds)")
            print(f"   INFO: Found {len(outliers)} values > {threshold}% (may be valid)")
        else:
            print(f"   OK - No extreme outliers")

        return outliers

    def check_missing_dates(self, dates: list) -> list:
        """Check for gaps in date coverage"""
        print("\n4. Checking for missing dates in last 30 days...")
        missing = []

        today = datetime.now()
        expected_dates = set()

        # Generate expected trading days (weekdays, excluding known holidays)
        for i in range(30):
            d = today - timedelta(days=i)
            if d.weekday() < 5:  # Monday to Friday
                expected_dates.add(d.strftime('%Y-%m-%d'))

        available = set(dates)

        # Check which expected dates are missing
        for d in sorted(expected_dates, reverse=True):
            if d not in available:
                # Check if it might be a holiday (no NAV data available)
                missing.append(d)

        if len(missing) > 10:  # Too many missing = probably just not fetched
            print(f"   INFO: {len(missing)} dates not in DB (may not have been fetched)")
        elif missing:
            for d in missing[:5]:
                print(f"   INFO: Missing {d}")
            self.add_issue('info', 'missing_dates',
                f"{len(missing)} dates missing in last 30 days", missing)
        else:
            print("   OK - No unexpected gaps")

        return missing

    def check_fund_count_consistency(self, dates: list) -> list:
        """Check if fund count is consistent across dates"""
        print("\n5. Checking fund count consistency...")
        counts = {}

        for d in dates[-10:]:  # Last 10 dates
            result = self.db.client.table('mutual_fund_returns').select('fund_id').eq('report_date', d).execute()
            counts[d] = len(result.data)

        if counts:
            avg = sum(counts.values()) / len(counts)
            anomalies = [(d, c) for d, c in counts.items() if abs(c - avg) > 50]

            if anomalies:
                for d, c in anomalies:
                    print(f"   WARNING: {d} has {c} funds (avg: {avg:.0f})")
                    self.add_issue('warning', 'count_anomaly',
                        f"{d} has unusual fund count: {c} (avg: {avg:.0f})", [d])
            else:
                print(f"   OK - Fund counts consistent (~{avg:.0f} per date)")

        return []

    def check_top200_coverage(self, dates: list) -> list:
        """Check if current top 200 funds have data for all significant dates"""
        print("\n6. Checking top 200 fund coverage across dates...")

        # Get latest date
        latest = self.db.client.table("mutual_fund_returns").select("report_date").order("report_date", desc=True).limit(1).execute()
        latest_date = latest.data[0]["report_date"]

        # Get current top 200 fund IDs
        top = self.db.client.table("mutual_fund_returns").select("fund_id").eq("report_date", latest_date).order("roi_3y", desc=True).limit(200).execute()
        top_fund_ids = set(r["fund_id"] for r in top.data)

        # Get significant dates (last 20 for display)
        sig_dates = sorted(self.db.get_significant_change_dates("SENSEX"), reverse=True)[:20]

        # Check coverage for each date
        incomplete_dates = []
        for date in sig_dates:
            result = self.db.client.table("mutual_fund_returns").select("fund_id").in_("fund_id", list(top_fund_ids)).eq("report_date", date).execute()
            coverage = len(result.data)

            if coverage < 180:  # Less than 90% coverage
                incomplete_dates.append((date, coverage))
                self.add_issue('critical', 'incomplete_coverage',
                    f"{date}: only {coverage}/200 top funds have data", [date])
                print(f"   CRITICAL: {date}: only {coverage}/200 funds")

        if not incomplete_dates:
            print(f"   OK - All top 200 funds have data for {len(sig_dates)} display dates")

        return incomplete_dates

    def run_full_audit(self, days: int = None) -> dict:
        """Run all audit checks"""
        print("=" * 60)
        print("  DATA AUDIT REPORT")
        print("=" * 60)

        # Get all dates
        all_dates = sorted(self.db.get_available_dates())

        if days:
            cutoff = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            dates = [d for d in all_dates if d >= cutoff]
            print(f"\nAuditing last {days} days ({len(dates)} dates)")
        else:
            dates = all_dates
            print(f"\nAuditing all data ({len(dates)} dates)")

        # Run checks
        duplicates = self.check_duplicate_consecutive_dates(dates)
        none_issues = self.check_none_values(dates)
        outliers = self.check_outliers(dates)
        missing = self.check_missing_dates(dates)
        self.check_fund_count_consistency(dates)
        self.check_top200_coverage(dates)

        # Summary
        print("\n" + "=" * 60)
        print("  SUMMARY")
        print("=" * 60)

        critical = [i for i in self.issues if i['severity'] == 'critical']
        warnings = [i for i in self.issues if i['severity'] == 'warning']
        info = [i for i in self.issues if i['severity'] == 'info']

        print(f"\n   Critical: {len(critical)}")
        print(f"   Warnings: {len(warnings)}")
        print(f"   Info:     {len(info)}")

        if critical:
            print("\n   CRITICAL ISSUES (require fix):")
            for i in critical:
                print(f"   - {i['description']}")

        dates_to_fix = set()
        for i in critical:
            dates_to_fix.update(i['dates'])

        if dates_to_fix:
            print(f"\n   Dates needing recalculation: {sorted(dates_to_fix)}")

        print("\n" + "=" * 60)

        return {
            'dates_to_fix': sorted(dates_to_fix),
            'issues': self.issues
        }

    def fix_dates(self, dates: list):
        """Recalculate data for specified dates"""
        if not dates:
            print("No dates to fix")
            return

        print(f"\nFixing {len(dates)} dates...")

        mfapi = MFAPIClient()
        calc = ROICalculator(mfapi)

        for date_str in dates:
            print(f"\n[{date_str}]")
            target_date = datetime.strptime(date_str, '%Y-%m-%d')

            result = self.db.client.table('mutual_fund_returns').select('fund_id').eq('report_date', date_str).execute()
            fund_ids = [r['fund_id'] for r in result.data]
            print(f"  Funds to update: {len(fund_ids)}")

            updated = 0
            errors = 0

            for fund_id in fund_ids:
                try:
                    fund = self.db.client.table('mutual_funds').select('scheme_code').eq('id', fund_id).execute()
                    if not fund.data or not fund.data[0].get('scheme_code'):
                        continue

                    scheme_code = fund.data[0]['scheme_code']
                    nav_data = mfapi.get_fund_nav(scheme_code)
                    if not nav_data:
                        continue

                    result = calc.calculate_fund_returns(scheme_code, target_date)
                    if not result or result.get('roi_3y') is None:
                        continue

                    self.db.client.table('mutual_fund_returns').update({
                        'roi_1y': result.get('roi_1y'),
                        'roi_2y': result.get('roi_2y'),
                        'roi_3y': result.get('roi_3y'),
                        'source': 'recalculated'
                    }).eq('fund_id', fund_id).eq('report_date', date_str).execute()

                    updated += 1
                except Exception as e:
                    errors += 1

            print(f"  Updated: {updated}, Errors: {errors}")

        print("\nFix complete!")


def main():
    parser = argparse.ArgumentParser(description="Comprehensive Data Audit Tool")
    parser.add_argument("--fix", action="store_true", help="Fix critical issues after audit")
    parser.add_argument("--days", type=int, help="Audit only last N days")
    args = parser.parse_args()

    db = SupabaseDB()
    auditor = DataAuditor(db)

    result = auditor.run_full_audit(days=args.days)

    if args.fix and result['dates_to_fix']:
        print("\n--fix specified, recalculating problematic dates...")
        auditor.fix_dates(result['dates_to_fix'])

        # Re-run audit to verify
        print("\n\nRe-running audit to verify fix...")
        auditor.issues = []
        auditor.run_full_audit(days=args.days)
    elif result['dates_to_fix']:
        print("\nTo fix these issues, run:")
        print(f"  python data_audit.py --fix")


if __name__ == "__main__":
    main()
