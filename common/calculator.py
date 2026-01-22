"""
ROI Calculator - Calculate returns from NAV data
Provides consistent ROI calculation across all scripts
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from .mfapi import MFAPIClient


# Category mapping for standardization
CATEGORY_MAP = {
    'Large Cap': ['Large Cap'],
    'Mid Cap': ['Mid Cap'],
    'Small Cap': ['Small Cap'],
    'Multi Cap': ['Multi Cap'],
    'Flexi Cap': ['Flexi Cap'],
    'ELSS': ['ELSS'],
    'Hybrid': ['Hybrid', 'Balanced'],
    'Sectoral': ['Sectoral', 'Thematic'],
    'Value/Contra': ['Value', 'Contra'],
    'Focused': ['Focused'],
    'Index': ['Index'],
    'Gold': ['Gold'],
}


def standardize_category(raw_category: str) -> str:
    """Convert raw category to standard category name"""
    if not raw_category:
        return 'Unknown'

    for standard, keywords in CATEGORY_MAP.items():
        for keyword in keywords:
            if keyword in raw_category:
                return standard

    return raw_category  # Keep original if no match


class ROICalculator:
    """
    Calculate ROI from NAV data
    Reusable across all scripts
    """

    def __init__(self, mfapi_client: MFAPIClient):
        """
        Initialize calculator with MFAPI client

        Args:
            mfapi_client: Initialized MFAPIClient with cached NAV data
        """
        self.mfapi = mfapi_client

    def calculate_roi(
        self,
        current_nav: float,
        historical_nav: float,
        years: float = 1.0,
        annualize: bool = True
    ) -> Optional[float]:
        """
        Calculate ROI between two NAV values

        Args:
            current_nav: Current/reference NAV
            historical_nav: Historical NAV
            years: Number of years between values (for annualization)
            annualize: If True, return annualized (CAGR); if False, return absolute

        Returns:
            ROI as percentage, or None if invalid
        """
        if not current_nav or not historical_nav or historical_nav <= 0:
            return None

        if annualize and years > 1:
            # CAGR = (current/historical)^(1/years) - 1
            return ((current_nav / historical_nav) ** (1 / years) - 1) * 100
        else:
            # Simple return
            return ((current_nav - historical_nav) / historical_nav) * 100

    def calculate_fund_returns(
        self,
        scheme_code: int,
        as_of_date: datetime = None
    ) -> Optional[Dict]:
        """
        Calculate 1Y, 2Y, 3Y returns for a fund

        Args:
            scheme_code: AMFI scheme code
            as_of_date: Calculate returns as of this date (default: latest)
                        Must be a trading day (exact NAV match required)

        Returns:
            Dict with roi_1y, roi_2y, roi_3y or None if insufficient data
        """
        # Get fund data
        meta = self.mfapi.get_fund_meta(scheme_code)
        if not meta:
            return None

        # Get reference NAV (exact match required - must be a trading day)
        if as_of_date:
            ref_nav, ref_date = self.mfapi.find_nav_for_date(
                scheme_code, as_of_date, exact=True
            )
        else:
            # Use latest NAV
            data = self.mfapi.nav_cache.get(scheme_code, {}).get('data', [])
            if not data:
                return None
            ref_nav = float(data[0]['nav'])
            ref_date = datetime.strptime(data[0]['date'], '%d-%m-%Y')

        if not ref_nav or not ref_date:
            return None

        # Get historical NAVs (nearest match - may fall on weekend/holiday)
        nav_1y, date_1y = self.mfapi.find_nav_for_date(
            scheme_code, ref_date - timedelta(days=365), exact=False
        )
        nav_2y, date_2y = self.mfapi.find_nav_for_date(
            scheme_code, ref_date - timedelta(days=730), exact=False
        )
        nav_3y, date_3y = self.mfapi.find_nav_for_date(
            scheme_code, ref_date - timedelta(days=1095), exact=False
        )

        # Need at least 3Y data
        if not nav_3y:
            return None

        # Calculate returns using ACTUAL years
        years_1y = (ref_date - date_1y).days / 365.25 if date_1y else 1
        years_2y = (ref_date - date_2y).days / 365.25 if date_2y else 2
        years_3y = (ref_date - date_3y).days / 365.25 if date_3y else 3

        roi_1y = self.calculate_roi(ref_nav, nav_1y, years_1y, annualize=False) if nav_1y else None
        roi_2y = self.calculate_roi(ref_nav, nav_2y, years_2y, annualize=True) if nav_2y else None
        roi_3y = self.calculate_roi(ref_nav, nav_3y, years_3y, annualize=True) if nav_3y else None

        return {
            'fund_name': meta.get('scheme_name', ''),
            'fund_house': meta.get('fund_house', '').replace(' Mutual Fund', ''),
            'category': standardize_category(meta.get('scheme_category', '')),
            'roi_1y': round(roi_1y, 2) if roi_1y is not None else None,
            'roi_2y': round(roi_2y, 2) if roi_2y is not None else None,
            'roi_3y': round(roi_3y, 2) if roi_3y is not None else None,
        }

    def calculate_all_returns(
        self,
        as_of_date: str = None,
        min_3y_roi: float = None
    ) -> List[Dict]:
        """
        Calculate returns for all cached funds

        Args:
            as_of_date: Calculate as of this date (YYYY-MM-DD format)
            min_3y_roi: Minimum 3Y ROI filter (optional)

        Returns:
            List of fund dicts with returns
        """
        target_date = datetime.strptime(as_of_date, '%Y-%m-%d') if as_of_date else None

        funds = []
        for scheme_code, meta, nav_data in self.mfapi.iter_cached_funds():
            result = self.calculate_fund_returns(scheme_code, target_date)
            if result and result.get('roi_3y') is not None:
                if min_3y_roi is None or result['roi_3y'] >= min_3y_roi:
                    funds.append(result)

        return funds

    def get_top_funds(
        self,
        as_of_date: str = None,
        top_n: int = 200
    ) -> List[Dict]:
        """
        Get top N funds by 3Y ROI

        Args:
            as_of_date: Calculate as of this date
            top_n: Number of top funds to return

        Returns:
            List of top funds sorted by roi_3y descending
        """
        funds = self.calculate_all_returns(as_of_date)
        funds.sort(key=lambda x: x.get('roi_3y', 0), reverse=True)
        return funds[:top_n]
