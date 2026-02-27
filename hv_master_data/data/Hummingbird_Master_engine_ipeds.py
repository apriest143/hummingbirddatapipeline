"""
================================================================================
IPEDS Financial Distress Scoring Engine — Hummingbird Project
================================================================================

This script calculates enhanced financial distress scores for IPEDS-reporting
institutions using 5 years (2020-2024) of institutional data. Scores are merged
into the Hummingbird Master Distress Enhanced file alongside 990 scores.

Goal: Identify institutions across the full spectrum of financial and operational
stress — not just imminent closures, but organizations that may be 5-10 years
from crisis and open to partnerships (e.g., land asset revitalization).


================================================================================
PREVIOUS MODEL (9 Binary Flags)
================================================================================

The original Hummingbird distress model used 9 binary indicators:

| # | Indicator                | Threshold               | Weight |
|---|--------------------------|-------------------------|--------|
| 1 | Enrollment Decline       | >10% drop over 2 years  | 15     |
| 2 | Low Retention Rate       | <60%                    | 10     |
| 3 | Operating Losses         | Net income <0 for 2+ yrs| 15     |
| 4 | Negative Net Worth       | Net worth < $0          | 15     |
| 5 | Declining Net Worth      | >20% drop over 2 years  | 10     |
| 6 | Low Equity Ratio         | <30%                    | 10     |
| 7 | High Tuition Dependency  | >80% of revenue         | 10     |
| 8 | Low Admissions Yield     | <15%                    | 5      |
| 9 | High Debt Ratio          | >50% of assets          | 10     |

Limitations:
  - Binary flags miss the spectrum (equity ratio of 29% and -15% score the same)
  - Scores cluster at 0 (no flags) with jumps of 10-15 pts per flag
  - 56% of institutions score "Healthy" with zero flags triggered
  - No visibility into HOW distressed — just whether a threshold was crossed


================================================================================
NEW MODEL (26 Continuous Indicators, 7 Domains)
================================================================================

Each indicator converts its raw value to a 0-1 distress score using calibrated
healthy and distress thresholds, with linear interpolation between them.
Indicators aggregate within domains (weighted average), then domains aggregate
to a 0-100 composite score.

DOMAIN 1: SOLVENCY (Weight: 20%)
Can the institution survive a bad year?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Equity ratio           | ≥ 0.40     | ≤ -0.10    | F2/F1A/F3    | Net assets / total assets           |
| Unrestricted cushion   | ≥ 0.25     | ≤ -0.10    | F2 only      | Unrestricted NA / expenses          |
| Debt ratio             | ≤ 0.50     | ≥ 1.00     | F2/F1A/F3    | Liabilities / assets (inverted)     |
| Expendable NA ratio    | ≥ 0.30     | ≤ -0.05    | F2/F1A       | Expendable net assets / expenses    |
| Debt to PP&E           | ≤ 0.50     | ≥ 1.20     | F2/F3        | Property debt / property value      |

DOMAIN 2: LIQUIDITY (Weight: 10%)
Can the institution meet short-term obligations?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Days cash on hand      | ≥ 90 days  | ≤ 15 days  | F2 only      | Unrestricted NA / daily expenses    |
| Endowment cushion      | ≥ $10K/FTE | ≤ $500/FTE | IPEDS deriv  | Endowment per FTE enrollment        |

DOMAIN 3: OPERATING PERFORMANCE (Weight: 15%)
Is the institution running surpluses or deficits?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Operating margin       | ≥ 5%       | ≤ -15%     | F2/F1A/F3    | (Revenue - Expenses) / Revenue      |
| Instruction ratio      | ≥ 30%      | ≤ 15%      | F2/F1A/F3    | Instruction spend / total expenses  |
| Admin overhead ratio   | ≤ 25%      | ≥ 45%      | F2/F3        | Inst. support / total expenses      |
| Tuition dependency     | ≤ 60%      | ≥ 85%      | IPEDS deriv  | Tuition as % of core revenues       |

DOMAIN 4: ENROLLMENT HEALTH (Weight: 20%)
The leading indicator for tuition-dependent institutions.
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Enrollment trend 1yr   | ≥ 0%       | ≤ -10%     | Enrollment   | Annualized YoY change               |
| Enrollment trend 4yr   | ≥ 0%       | ≤ -8%      | Enrollment   | Annualized change over full window  |
| Full-time share        | ≥ 60%      | ≤ 30%      | Enrollment   | FT / total (PT shift masks decline) |
| Enrollment size        | ≥ 1000     | ≤ 50       | Enrollment   | Small schools more vulnerable       |
| Revenue per student    | ≥ $15K     | ≤ $5K      | Financial    | Revenue / total enrollment          |

DOMAIN 5: ACADEMIC OUTCOMES (Weight: 15%)
Are students staying and completing?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Retention rate         | ≥ 70%      | ≤ 40%      | EF survey    | FT first-year retention             |
| Graduation rate        | ≥ 40%      | ≤ 15%      | GR survey    | Total cohort graduation rate        |
| Student-faculty ratio  | ≤ 20:1     | ≥ 35:1     | EF survey    | High ratio = cutting corners        |

DOMAIN 6: DEMAND & MARKET POSITION (Weight: 10%)
Is the institution attracting students?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Admissions yield       | ≥ 35%      | ≤ 15%      | ADM survey   | Enrolled / admitted                 |
| Selectivity            | ≤ 80%      | ≥ 98%      | ADM survey   | % admitted (inverted)               |

DOMAIN 7: TREND / TRAJECTORY (Weight: 10%)
Is the institution getting better or worse over 5 years?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source       | Notes                              |
|------------------------|------------|------------|--------------|-------------------------------------|
| Revenue trend          | ≥ 0%       | ≤ -10%     | Financial    | Annualized revenue growth           |
| Net asset trend        | ≥ 0%       | ≤ -10%     | Financial    | Annualized NA/equity growth         |
| Retention trend        | ≥ 0 pts/yr | ≤ -5 pts/yr| EF survey    | Annual change in retention rate     |
| Staff trend            | ≥ -2%      | ≤ -15%     | HR survey    | Annualized FTE staff change         |
| Salary trend           | ≥ 2%       | ≤ -3%      | HR survey    | Annualized avg salary change        |


================================================================================
ACCOUNTING STANDARD HANDLING
================================================================================

IPEDS institutions report financials under different frameworks depending on
their control type. Each framework uses different forms with different fields:

| Standard   | Control           | Form | Institutions | Financial Coverage |
|------------|-------------------|------|-------------:|-------------------|
| FASB       | Private nonprofit | F2   |        1,570 | Full (all 7 domains) |
| GASB       | Public            | F1A  |        1,469 | Full (6-7 domains)   |
| For-Profit | Private for-profit| F3   |          520 | Good (5-6 domains)   |
| None       | Mixed (small/vocational) | — | 2,353 | Enrollment + Academic + Trend only |

Key differences:
  - FASB (F2): Has unrestricted/restricted net asset split, expendable NA
  - GASB (F1A): Has net position, expendable NA, but no unrestricted split
  - F3: Has equity/liabilities/revenue/expenses, but no endowment or cushion metrics
  - None: ~1,372 are for-profit less-than-2-year schools (cosmetology, trade, etc.)
    that don't file detailed financials. Score based on enrollment/academic/trend ONLY.

The engine auto-detects accounting standard per institution and pulls from the
correct form. When financial domains cannot be computed, domain weights are
renormalized across available domains. A data_completeness flag indicates
how much of the scoring model could be applied.

IMPORTANT: Scores for institutions with <50% data completeness should be
interpreted with caution. They reflect enrollment and academic health only,
with no visibility into the balance sheet.


================================================================================
SCORING
================================================================================

  0-20  = Healthy
  20-40 = Low Risk (may have structural vulnerabilities worth monitoring)
  40-60 = Moderate Risk (multiple stress signals, potential partnership prospect)
  60-80 = High Risk (significant distress across multiple domains)
  80-100 = Severe Distress (likely approaching closure or major restructuring)

Minimum 4 scored indicators required to produce a score.


================================================================================
OUTPUT COLUMNS ADDED TO MASTER
================================================================================

  distress_score_ipeds         - Enhanced 0-100 composite score
  distress_category_ipeds      - Healthy / Low Risk / Moderate / High / Severe
  accounting_standard_ipeds    - fasb / gasb / for_profit / unknown
  solvency_score_ipeds         - Domain score (0-100)
  liquidity_score_ipeds        - Domain score (0-100)
  operating_score_ipeds        - Domain score (0-100)
  enrollment_score_ipeds       - Domain score (0-100)
  academic_score_ipeds         - Domain score (0-100)
  demand_score_ipeds           - Domain score (0-100)
  trend_score_ipeds            - Domain score (0-100)
  data_completeness_ipeds      - % of 26 indicators that could be computed
  score_year_ipeds             - Which year was scored
  + 13 raw metric columns (*_raw_ipeds) for transparency

The main distress_score and distress_category columns are also updated in-place.


================================================================================
USAGE
================================================================================

  1. Update file paths in CONFIGURATION section at bottom
  2. Run: python distress_ipeds.py

  Inputs:  5 IPEDS CSVs (2020-2024) + Hummingbird_Master_Distress_Enhanced.csv
  Outputs: Hummingbird_Master_Distress_IPEDS.csv (updated master)
           ipeds_distress_scores_detail.csv (year-by-year scores for analysis)
================================================================================
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, List
import warnings
warnings.filterwarnings('ignore')


# =============================================================================
# VARIABLE SEARCH PATTERNS
# =============================================================================
# IPEDS columns include year-specific prefixes (e.g., DRVEF2024, F2324_F2).
# We search by the stable suffix portion to find variables across years.

IPEDS_VARIABLE_SEARCHES = {
    # --- Identifiers ---
    'unitid': 'unitid',
    'institution_name': 'institution name',
    'sector': 'sector of institution',
    'control': 'control of institution',
    'size_category': 'institution size category',

    # --- Enrollment ---
    'total_enrollment': 'total  enrollment',
    'ft_enrollment': 'full-time enrollment',
    'pt_enrollment': 'part-time enrollment',
    'grad_enrollment': 'graduate enrollment',

    # --- Retention & Graduation ---
    'ft_retention_rate': 'full-time retention rate',
    'graduation_rate': 'graduation rate, total cohort',
    'student_faculty_ratio': 'student-to-faculty ratio',

    # --- Admissions ---
    'admissions_yield': 'admissions yield - total',
    'percent_admitted': 'percent admitted - total',

    # --- F2 / FASB (Private Nonprofit) ---
    'f2_total_assets': '_f2.total assets',
    'f2_total_liabilities': '_f2.total liabilities',
    'f2_total_net_assets': '_f2.total net assets',
    'f2_unrestricted_na': '_f2.total unrestricted net assets',
    'f2_restricted_na': '_f2.total restricted net assets',
    'f2_total_revenues': '_f2.total revenues and investment return',
    'f2_total_expenses': '_f2.total expenses',
    'f2_change_na': '_f2.total change in net assets',
    'f2_expendable_na': '_f2.expendable net assets',
    'f2_lt_investments': '_f2.long-term investments',
    'f2_ppe': '_f2.total plant, property',
    'f2_debt_ppe': '_f2.debt related to property',
    'f2_tuition_fees': '_f2.tuition and fees',
    'f2_federal_grants': '_f2.federal grants and contracts - total',
    'f2_state_approp': '_f2.state appropriations - total',
    'f2_private_gifts': '_f2.private gifts, grants, and contracts - total',
    'f2_instruction': '_f2.instruction-total amount',
    'f2_institutional_support': '_f2.institutional support-total amount',
    'f2_student_services': '_f2.student service-total amount',

    # --- F1A / GASB (Public) ---
    'f1a_total_assets': '_f1a.total assets',
    'f1a_total_liabilities': '_f1a.total liabilities',
    'f1a_net_position': '_f1a.net position',
    'f1a_expendable_na': '_f1a.expendable net assets',
    'f1a_operating_income': '_f1a.operating income',
    'f1a_total_revenues': '_f1a.total all revenues',
    'f1a_instruction': '_f1a.instruction - current year total',
    'f1a_tuition_fees': '_f1a.tuition and fees, after deducting',

    # --- F3 / For-Profit ---
    'f3_total_assets': '_f3.total assets',
    'f3_total_liabilities': '_f3.total liabilities',
    'f3_total_equity': '_f3.total equity',
    'f3_total_revenues': '_f3.total revenues and investment return',
    'f3_total_expenses': '_f3.total expenses',
    'f3_net_income': '_f3.net income',
    'f3_ppe': '_f3.total plant, property',
    'f3_debt_ppe': '_f3.plant-related debt',
    'f3_instruction': '_f3.instruction-total amount',
    'f3_institutional_support': '_f3.institutional support-total amount',
    'f3_tuition_fees': '_f3.tuition and fees',

    # --- Derived (IPEDS pre-computed) ---
    'equity_ratio_fasb': 'equity ratio (fasb)',
    'equity_ratio_gasb': 'equity ratio (gasb)',
    'tuition_pct_fasb': 'tuition and fees as a percent of core revenues (fasb)',
    'tuition_pct_gasb': 'tuition and fees as a percent of core revenues (gasb)',
    'endowment_per_fte': 'endowment assets (year end) per fte',

    # --- Staffing ---
    'avg_salary': 'average salary equated to 9 months of full-time instructional staff - all',
    'total_fte_staff': 'total fte staff',
}

# Variables to exclude from numeric conversion
TEXT_FIELDS = {'unitid', 'institution_name', 'sector', 'control', 'size_category'}

# Which variables indicate FASB vs GASB accounting
FASB_INDICATOR = 'f2_total_assets'
GASB_INDICATOR = 'f1a_total_assets'


# =============================================================================
# DISTRESS DOMAIN DEFINITIONS
# =============================================================================

DISTRESS_DOMAINS = {
    # =========================================================================
    # DOMAIN 1: SOLVENCY (Weight: 0.20)
    # =========================================================================
    'solvency': {
        'weight': 0.20,
        'indicators': {
            'equity_ratio': {'weight': 0.30},
            'unrestricted_cushion': {'weight': 0.25},
            'debt_ratio': {'weight': 0.20},
            'expendable_na_ratio': {'weight': 0.15},
            'debt_to_ppe': {'weight': 0.10},
        }
    },

    # =========================================================================
    # DOMAIN 2: LIQUIDITY (Weight: 0.10)
    # =========================================================================
    'liquidity': {
        'weight': 0.10,
        'indicators': {
            'days_cash': {'weight': 0.50},
            'endowment_cushion': {'weight': 0.50},
        }
    },

    # =========================================================================
    # DOMAIN 3: OPERATING PERFORMANCE (Weight: 0.15)
    # =========================================================================
    'operating_performance': {
        'weight': 0.15,
        'indicators': {
            'operating_margin': {'weight': 0.35},
            'instruction_ratio': {'weight': 0.20},
            'admin_overhead_ratio': {'weight': 0.20},
            'tuition_dependency': {'weight': 0.25},
        }
    },

    # =========================================================================
    # DOMAIN 4: ENROLLMENT HEALTH (Weight: 0.20)
    # =========================================================================
    'enrollment_health': {
        'weight': 0.20,
        'indicators': {
            'enrollment_trend_1yr': {'weight': 0.25},
            'enrollment_trend_4yr': {'weight': 0.25},
            'ft_share': {'weight': 0.15},
            'enrollment_size': {'weight': 0.15},
            'revenue_per_student': {'weight': 0.20},
        }
    },

    # =========================================================================
    # DOMAIN 5: ACADEMIC OUTCOMES (Weight: 0.15)
    # =========================================================================
    'academic_outcomes': {
        'weight': 0.15,
        'indicators': {
            'retention_rate': {'weight': 0.40},
            'graduation_rate': {'weight': 0.35},
            'student_faculty_ratio': {'weight': 0.25},
        }
    },

    # =========================================================================
    # DOMAIN 6: DEMAND & MARKET POSITION (Weight: 0.10)
    # =========================================================================
    'demand': {
        'weight': 0.10,
        'indicators': {
            'admissions_yield': {'weight': 0.50},
            'selectivity': {'weight': 0.50},
        }
    },

    # =========================================================================
    # DOMAIN 7: TREND / TRAJECTORY (Weight: 0.10)
    # =========================================================================
    'trend': {
        'weight': 0.10,
        'indicators': {
            'revenue_trend': {'weight': 0.25},
            'net_asset_trend': {'weight': 0.25},
            'retention_trend': {'weight': 0.20},
            'staff_trend': {'weight': 0.15},
            'salary_trend': {'weight': 0.15},
        }
    },
}


class DistressIPEDSEngine:
    """
    Financial distress scoring engine for IPEDS-reporting institutions.

    Produces a 0-100 distress score where:
        0-20  = Healthy
        20-40 = Low Risk
        40-60 = Moderate Risk
        60-80 = High Risk
        80-100 = Severe Distress / Likely Closure
    """

    def __init__(self):
        self.data = {}              # {unitid: {year: {standardized fields}}}
        self.accounting_std = {}    # {unitid: 'fasb'|'gasb'|'for_profit'}

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def load_data(self, file_paths: dict, filter_unitids: set = None):
        """
        Load IPEDS data from CSV files.

        Args:
            file_paths: dict mapping year (int) to file path,
                        e.g. {2020: 'IPEDs20.csv', 2021: 'IPEDS20.csv', ...}
            filter_unitids: Optional set of UNITIDs to keep
        """
        for year, path in sorted(file_paths.items()):
            print(f"Loading {year} from {path}...")
            df = pd.read_csv(path, encoding='latin-1', low_memory=False)

            # Standardize column names via search
            col_map = self._build_column_map(df.columns.tolist())
            df_std = pd.DataFrame()
            df_std['unitid'] = df['unitid'].astype(str).str.strip()

            for std_name, orig_col in col_map.items():
                if std_name == 'unitid':
                    continue  # Already set above as cleaned string
                if std_name in TEXT_FIELDS:
                    df_std[std_name] = df[orig_col]
                else:
                    df_std[std_name] = pd.to_numeric(df[orig_col], errors='coerce')

            if filter_unitids:
                filter_set = {str(u).strip() for u in filter_unitids}
                df_std = df_std[df_std['unitid'].isin(filter_set)]

            # Store by unitid
            loaded = 0
            for _, row in df_std.iterrows():
                uid = row['unitid']
                if uid not in self.data:
                    self.data[uid] = {}
                self.data[uid][year] = row.to_dict()
                loaded += 1

                # Detect accounting standard (from most recent data)
                if pd.notna(row.get(FASB_INDICATOR)):
                    self.accounting_std[uid] = 'fasb'
                elif pd.notna(row.get(GASB_INDICATOR)):
                    self.accounting_std[uid] = 'gasb'
                elif pd.notna(row.get('f3_total_assets')):
                    self.accounting_std[uid] = 'for_profit'

            mapped = len(col_map)
            total = len(IPEDS_VARIABLE_SEARCHES)
            print(f"  → {loaded} institutions, {mapped}/{total} variables mapped")

        # Summary
        multi = sum(1 for d in self.data.values() if len(d) > 1)
        print(f"\nTotal: {len(self.data)} institutions")
        print(f"Multi-year data (enables trends): {multi}/{len(self.data)}")
        acct = pd.Series(list(self.accounting_std.values()))
        print(f"Accounting standards: {dict(acct.value_counts())}")

    def _build_column_map(self, columns: list) -> dict:
        """Map standardized variable names to actual column names via search."""
        col_map = {}
        cols_lower = [c.lower() for c in columns]

        for std_name, search_term in IPEDS_VARIABLE_SEARCHES.items():
            # Special handling: exclude terms to avoid wrong matches
            exclude = []
            if std_name == 'grad_enrollment':
                exclude = ['under', 'full-time']
            elif std_name == 'f2_total_expenses':
                exclude = ['instruction', 'research', 'deduction']
            elif std_name == 'f3_total_expenses':
                exclude = ['instruction', 'research', 'salaries', 'benefits',
                           'depreciation', 'interest', 'operations', 'other']
            elif std_name == 'f2_tuition_fees':
                exclude = ['allowance', 'percent', 'after']
            elif std_name == 'f3_tuition_fees':
                exclude = ['allowance', 'discount', 'after']
            elif std_name == 'f1a_net_position':
                exclude = ['begin', 'change', 'during']
            elif std_name == 'f3_total_equity':
                exclude = ['begin', 'end of year', 'adjusted', '.1']

            for i, cl in enumerate(cols_lower):
                if search_term in cl:
                    if any(ex in cl for ex in exclude):
                        continue
                    col_map[std_name] = columns[i]
                    break

        return col_map

    # =========================================================================
    # HELPER METHODS
    # =========================================================================

    def _safe_get(self, data: dict, field: str, default=np.nan):
        val = data.get(field, default)
        if pd.isna(val):
            return default
        try:
            return float(val)
        except (ValueError, TypeError):
            return default

    def _safe_divide(self, num, denom, default=np.nan):
        if pd.isna(num) or pd.isna(denom) or denom == 0:
            return default
        return num / denom

    def _score(self, value, healthy, distress, invert=False):
        """Convert raw metric to 0-1 distress score."""
        if pd.isna(value) or isinstance(value, complex):
            return np.nan
        if invert:
            if value <= healthy:
                return 0.0
            elif value >= distress:
                return 1.0
            else:
                return (value - healthy) / (distress - healthy)
        else:
            if value >= healthy:
                return 0.0
            elif value <= distress:
                return 1.0
            else:
                return (healthy - value) / (healthy - distress)

    def _get_financial(self, data: dict, uid: str, fasb_field: str,
                       gasb_field: str = None, fp_field: str = None):
        """Get a financial value, choosing FASB, GASB, or F3 based on institution type."""
        acct = self.accounting_std.get(uid, 'unknown')
        if acct == 'fasb':
            return self._safe_get(data, fasb_field)
        elif acct == 'gasb' and gasb_field:
            return self._safe_get(data, gasb_field)
        elif acct == 'for_profit' and fp_field:
            return self._safe_get(data, fp_field)
        return np.nan

    # =========================================================================
    # DOMAIN COMPUTATIONS
    # =========================================================================

    def compute_solvency(self, data: dict, uid: str) -> dict:
        r = {}
        acct = self.accounting_std.get(uid, 'unknown')

        # Equity ratio (IPEDS pre-computed or derived)
        if acct == 'fasb':
            eq = self._safe_get(data, 'equity_ratio_fasb')
        elif acct == 'gasb':
            eq = self._safe_get(data, 'equity_ratio_gasb')
        elif acct == 'for_profit':
            # Derive: equity / assets
            equity = self._safe_get(data, 'f3_total_equity')
            assets = self._safe_get(data, 'f3_total_assets')
            eq = self._safe_divide(equity, assets) * 100 if not pd.isna(self._safe_divide(equity, assets)) else np.nan
        else:
            eq = np.nan
        if not pd.isna(eq):
            eq = eq / 100.0
        r['equity_ratio'] = self._score(eq, 0.40, -0.10)
        r['equity_ratio_raw'] = eq

        # Unrestricted cushion (FASB only — GASB and F3 don't have this split)
        unrestricted = self._get_financial(data, uid, 'f2_unrestricted_na')
        expenses = self._get_financial(data, uid, 'f2_total_expenses', None, 'f3_total_expenses')
        cushion = self._safe_divide(unrestricted, expenses)
        r['unrestricted_cushion'] = self._score(cushion, 0.25, -0.10)
        r['unrestricted_cushion_raw'] = cushion

        # Debt ratio
        assets = self._get_financial(data, uid, 'f2_total_assets', 'f1a_total_assets', 'f3_total_assets')
        liabilities = self._get_financial(data, uid, 'f2_total_liabilities', 'f1a_total_liabilities', 'f3_total_liabilities')
        debt_ratio = self._safe_divide(liabilities, assets)
        r['debt_ratio'] = self._score(debt_ratio, 0.50, 1.0, invert=True)
        r['debt_ratio_raw'] = debt_ratio

        # Expendable net assets ratio
        expendable = self._get_financial(data, uid, 'f2_expendable_na', 'f1a_expendable_na')
        if pd.isna(expenses):
            expenses = self._get_financial(data, uid, 'f2_total_expenses', None, 'f3_total_expenses')
        exp_ratio = self._safe_divide(expendable, expenses) if not pd.isna(expenses) else \
                    self._safe_divide(expendable, assets)
        r['expendable_na_ratio'] = self._score(exp_ratio, 0.30, -0.05)
        r['expendable_na_ratio_raw'] = exp_ratio

        # Debt to PP&E
        debt_ppe = self._get_financial(data, uid, 'f2_debt_ppe', None, 'f3_debt_ppe')
        ppe = self._get_financial(data, uid, 'f2_ppe', None, 'f3_ppe')
        d2ppe = self._safe_divide(debt_ppe, ppe)
        r['debt_to_ppe'] = self._score(d2ppe, 0.50, 1.20, invert=True)
        r['debt_to_ppe_raw'] = d2ppe

        return r

    def compute_liquidity(self, data: dict, uid: str) -> dict:
        r = {}

        # Days cash on hand (unrestricted / daily expenses)
        unrestricted = self._get_financial(data, uid, 'f2_unrestricted_na')
        expenses = self._get_financial(data, uid, 'f2_total_expenses')
        if not pd.isna(unrestricted) and not pd.isna(expenses) and expenses > 0:
            days = max(0, (unrestricted / expenses) * 365)
            r['days_cash'] = self._score(days, 90, 15)
            r['days_cash_raw'] = days
        else:
            r['days_cash'] = np.nan
            r['days_cash_raw'] = np.nan

        # Endowment cushion (endowment per FTE)
        endowment = self._safe_get(data, 'endowment_per_fte')
        # Healthy: >$10K/FTE, Distress: <$500/FTE
        r['endowment_cushion'] = self._score(endowment, 10000, 500)
        r['endowment_cushion_raw'] = endowment

        return r

    def compute_operating(self, data: dict, uid: str) -> dict:
        r = {}
        acct = self.accounting_std.get(uid, 'unknown')

        # Operating margin
        if acct == 'fasb':
            revenue = self._safe_get(data, 'f2_total_revenues')
            expenses = self._safe_get(data, 'f2_total_expenses')
        elif acct == 'gasb':
            revenue = self._safe_get(data, 'f1a_total_revenues')
            op_income = self._safe_get(data, 'f1a_operating_income')
            expenses = (revenue - op_income) if not pd.isna(revenue) and not pd.isna(op_income) else np.nan
        elif acct == 'for_profit':
            revenue = self._safe_get(data, 'f3_total_revenues')
            expenses = self._safe_get(data, 'f3_total_expenses')
        else:
            revenue, expenses = np.nan, np.nan

        margin = self._safe_divide(revenue - expenses, abs(revenue)) if not pd.isna(revenue) and not pd.isna(expenses) and revenue != 0 else np.nan
        r['operating_margin'] = self._score(margin, 0.05, -0.15)
        r['operating_margin_raw'] = margin

        # Instruction ratio
        if acct == 'fasb':
            instruction = self._safe_get(data, 'f2_instruction')
            total_exp = self._safe_get(data, 'f2_total_expenses')
        elif acct == 'gasb':
            instruction = self._safe_get(data, 'f1a_instruction')
            total_exp = expenses
        elif acct == 'for_profit':
            instruction = self._safe_get(data, 'f3_instruction')
            total_exp = self._safe_get(data, 'f3_total_expenses')
        else:
            instruction, total_exp = np.nan, np.nan

        inst_ratio = self._safe_divide(instruction, total_exp)
        r['instruction_ratio'] = self._score(inst_ratio, 0.30, 0.15)
        r['instruction_ratio_raw'] = inst_ratio

        # Admin overhead
        if acct == 'fasb':
            inst_support = self._safe_get(data, 'f2_institutional_support')
        elif acct == 'for_profit':
            inst_support = self._safe_get(data, 'f3_institutional_support')
        else:
            inst_support = np.nan
        admin_ratio = self._safe_divide(inst_support, total_exp)
        r['admin_overhead_ratio'] = self._score(admin_ratio, 0.25, 0.45, invert=True)
        r['admin_overhead_ratio_raw'] = admin_ratio

        # Tuition dependency
        if acct == 'fasb':
            tuition_pct = self._safe_get(data, 'tuition_pct_fasb')
        elif acct == 'gasb':
            tuition_pct = self._safe_get(data, 'tuition_pct_gasb')
        elif acct == 'for_profit':
            # Derive: tuition / total revenue
            tuition = self._safe_get(data, 'f3_tuition_fees')
            tuition_pct = self._safe_divide(tuition, revenue) * 100 if not pd.isna(self._safe_divide(tuition, revenue)) else np.nan
        else:
            tuition_pct = np.nan
        r['tuition_dependency'] = self._score(tuition_pct, 60, 85, invert=True)
        r['tuition_dependency_raw'] = tuition_pct

        return r

    def compute_enrollment(self, data: dict, uid: str, year: int) -> dict:
        r = {}
        years_data = self.data.get(uid, {})

        # Current enrollment
        total_enroll = self._safe_get(data, 'total_enrollment')
        ft_enroll = self._safe_get(data, 'ft_enrollment')

        # Enrollment trend 1-year
        prior_years = sorted([y for y in years_data if y < year], reverse=True)
        if prior_years:
            prior = years_data[prior_years[0]]
            prior_enroll = self._safe_get(prior, 'total_enrollment')
            gap = year - prior_years[0]
            if not pd.isna(total_enroll) and not pd.isna(prior_enroll) and prior_enroll > 0:
                change_1yr = ((total_enroll / prior_enroll) ** (1/gap) - 1) if gap > 0 else 0
                r['enrollment_trend_1yr'] = self._score(change_1yr, 0.0, -0.10)
                r['enrollment_trend_1yr_raw'] = change_1yr
            else:
                r['enrollment_trend_1yr'] = np.nan
                r['enrollment_trend_1yr_raw'] = np.nan
        else:
            r['enrollment_trend_1yr'] = np.nan
            r['enrollment_trend_1yr_raw'] = np.nan

        # Enrollment trend 4-year (longest available)
        oldest_years = sorted(years_data.keys())
        if len(oldest_years) >= 2 and oldest_years[0] < year:
            oldest = years_data[oldest_years[0]]
            oldest_enroll = self._safe_get(oldest, 'total_enrollment')
            gap = year - oldest_years[0]
            if not pd.isna(total_enroll) and not pd.isna(oldest_enroll) and oldest_enroll > 0 and gap > 0:
                change_long = (total_enroll / oldest_enroll) ** (1/gap) - 1
                r['enrollment_trend_4yr'] = self._score(change_long, 0.0, -0.08)
                r['enrollment_trend_4yr_raw'] = change_long
            else:
                r['enrollment_trend_4yr'] = np.nan
                r['enrollment_trend_4yr_raw'] = np.nan
        else:
            r['enrollment_trend_4yr'] = np.nan
            r['enrollment_trend_4yr_raw'] = np.nan

        # FT share (full-time as % of total)
        ft_share = self._safe_divide(ft_enroll, total_enroll)
        # Healthy: >0.60, Distress: <0.30
        r['ft_share'] = self._score(ft_share, 0.60, 0.30)
        r['ft_share_raw'] = ft_share

        # Enrollment size (small schools are more vulnerable)
        if not pd.isna(total_enroll):
            if total_enroll >= 1000:
                r['enrollment_size'] = 0.0
            elif total_enroll >= 500:
                r['enrollment_size'] = 0.2
            elif total_enroll >= 200:
                r['enrollment_size'] = 0.5
            elif total_enroll >= 50:
                r['enrollment_size'] = 0.7
            else:
                r['enrollment_size'] = 0.9
        else:
            r['enrollment_size'] = np.nan
        r['enrollment_size_raw'] = total_enroll

        # Revenue per student
        revenue = self._get_financial(data, uid, 'f2_total_revenues', 'f1a_total_revenues', 'f3_total_revenues')
        rev_per_student = self._safe_divide(revenue, total_enroll)
        # Healthy: >$15K, Distress: <$5K
        r['revenue_per_student'] = self._score(rev_per_student, 15000, 5000)
        r['revenue_per_student_raw'] = rev_per_student

        return r

    def compute_academic(self, data: dict, uid: str) -> dict:
        r = {}

        # Retention rate
        retention = self._safe_get(data, 'ft_retention_rate')
        # Healthy: >70%, Distress: <40%
        r['retention_rate'] = self._score(retention, 70, 40)
        r['retention_rate_raw'] = retention

        # Graduation rate
        grad_rate = self._safe_get(data, 'graduation_rate')
        # Healthy: >40%, Distress: <15%
        r['graduation_rate'] = self._score(grad_rate, 40, 15)
        r['graduation_rate_raw'] = grad_rate

        # Student-faculty ratio (too high = cutting corners)
        sfr = self._safe_get(data, 'student_faculty_ratio')
        # Healthy: <20, Distress: >35
        r['student_faculty_ratio'] = self._score(sfr, 20, 35, invert=True)
        r['student_faculty_ratio_raw'] = sfr

        return r

    def compute_demand(self, data: dict, uid: str) -> dict:
        r = {}

        # Admissions yield
        yld = self._safe_get(data, 'admissions_yield')
        # Healthy: >35%, Distress: <15%
        r['admissions_yield'] = self._score(yld, 35, 15)
        r['admissions_yield_raw'] = yld

        # Selectivity (% admitted — lower = more selective = healthier demand)
        pct_admitted = self._safe_get(data, 'percent_admitted')
        # Open admission (100%) is not itself distress, but very high with low yield is
        # Healthy: <80%, Distress: >95%
        r['selectivity'] = self._score(pct_admitted, 80, 98, invert=True)
        r['selectivity_raw'] = pct_admitted

        return r

    def compute_trends(self, uid: str, year: int) -> dict:
        r = {}
        years_data = self.data.get(uid, {})
        current = years_data.get(year, {})
        prior_years = sorted([y for y in years_data if y < year], reverse=True)

        if not prior_years or not current:
            return {k: np.nan for k in [
                'revenue_trend', 'revenue_trend_raw',
                'net_asset_trend', 'net_asset_trend_raw',
                'retention_trend', 'retention_trend_raw',
                'staff_trend', 'staff_trend_raw',
                'salary_trend', 'salary_trend_raw',
            ]}

        prior = years_data[prior_years[0]]
        gap = year - prior_years[0]
        if gap <= 0:
            gap = 1

        # Revenue trend
        curr_rev = self._get_financial(current, uid, 'f2_total_revenues', 'f1a_total_revenues', 'f3_total_revenues')
        prior_rev = self._get_financial(prior, uid, 'f2_total_revenues', 'f1a_total_revenues', 'f3_total_revenues')
        if not pd.isna(curr_rev) and not pd.isna(prior_rev) and prior_rev > 0 and curr_rev > 0:
            rev_change = (curr_rev / prior_rev) ** (1/gap) - 1
            r['revenue_trend'] = self._score(rev_change, 0.0, -0.10)
            r['revenue_trend_raw'] = rev_change
        else:
            r['revenue_trend'] = np.nan
            r['revenue_trend_raw'] = np.nan

        # Net asset trend
        curr_na = self._get_financial(current, uid, 'f2_total_net_assets', 'f1a_net_position', 'f3_total_equity')
        prior_na = self._get_financial(prior, uid, 'f2_total_net_assets', 'f1a_net_position', 'f3_total_equity')
        if not pd.isna(curr_na) and not pd.isna(prior_na):
            if prior_na > 0 and curr_na > 0:
                na_change = (curr_na / prior_na) ** (1/gap) - 1
            elif prior_na > 0 and curr_na <= 0:
                na_change = -0.30
            elif prior_na < 0 and curr_na < prior_na:
                na_change = -0.20
            elif prior_na < 0 and curr_na > prior_na:
                na_change = 0.05
            else:
                na_change = -0.10 if curr_na <= 0 else 0.0
            r['net_asset_trend'] = self._score(na_change, 0.0, -0.10)
            r['net_asset_trend_raw'] = na_change
        else:
            r['net_asset_trend'] = np.nan
            r['net_asset_trend_raw'] = np.nan

        # Retention trend
        curr_ret = self._safe_get(current, 'ft_retention_rate')
        prior_ret = self._safe_get(prior, 'ft_retention_rate')
        if not pd.isna(curr_ret) and not pd.isna(prior_ret):
            ret_change = (curr_ret - prior_ret) / gap
            # Healthy: stable or rising, Distress: dropping >5 pts/yr
            r['retention_trend'] = self._score(ret_change, 0, -5)
            r['retention_trend_raw'] = ret_change
        else:
            r['retention_trend'] = np.nan
            r['retention_trend_raw'] = np.nan

        # Staff trend
        curr_staff = self._safe_get(current, 'total_fte_staff')
        prior_staff = self._safe_get(prior, 'total_fte_staff')
        if not pd.isna(curr_staff) and not pd.isna(prior_staff) and prior_staff > 0:
            staff_change = (curr_staff / prior_staff) ** (1/gap) - 1
            r['staff_trend'] = self._score(staff_change, -0.02, -0.15)
            r['staff_trend_raw'] = staff_change
        else:
            r['staff_trend'] = np.nan
            r['staff_trend_raw'] = np.nan

        # Salary trend
        curr_sal = self._safe_get(current, 'avg_salary')
        prior_sal = self._safe_get(prior, 'avg_salary')
        if not pd.isna(curr_sal) and not pd.isna(prior_sal) and prior_sal > 0:
            sal_change = (curr_sal / prior_sal) ** (1/gap) - 1
            # Healthy: keeping up with inflation (~3%), Distress: declining
            r['salary_trend'] = self._score(sal_change, 0.02, -0.03)
            r['salary_trend_raw'] = sal_change
        else:
            r['salary_trend'] = np.nan
            r['salary_trend_raw'] = np.nan

        return r

    # =========================================================================
    # SCORE AGGREGATION
    # =========================================================================

    def score_entity(self, uid: str, year: int) -> dict:
        """Compute full distress score for one institution in one year."""
        data = self.data.get(uid, {}).get(year, {})
        if not data:
            return {'unitid': uid, 'year': year, 'distress_score': np.nan, 'error': 'no_data'}

        acct = self.accounting_std.get(uid, 'unknown')

        # Compute all domains
        domain_results = {
            'solvency': self.compute_solvency(data, uid),
            'liquidity': self.compute_liquidity(data, uid),
            'operating_performance': self.compute_operating(data, uid),
            'enrollment_health': self.compute_enrollment(data, uid, year),
            'academic_outcomes': self.compute_academic(data, uid),
            'demand': self.compute_demand(data, uid),
            'trend': self.compute_trends(uid, year),
        }

        # Aggregate within each domain
        domain_scores = {}
        for domain_name, domain_config in DISTRESS_DOMAINS.items():
            indicators = domain_results.get(domain_name, {})
            weighted_sum = 0
            weight_sum = 0

            for ind_name, ind_config in domain_config['indicators'].items():
                score = indicators.get(ind_name, np.nan)
                if not pd.isna(score):
                    w = ind_config['weight']
                    weighted_sum += score * w
                    weight_sum += w

            domain_scores[domain_name] = (weighted_sum / weight_sum * 100) if weight_sum > 0 else np.nan

        # Aggregate across domains
        total_weighted = 0
        total_weight = 0
        for domain_name, domain_config in DISTRESS_DOMAINS.items():
            ds = domain_scores.get(domain_name, np.nan)
            if not pd.isna(ds):
                w = domain_config['weight']
                total_weighted += ds * w
                total_weight += w

        composite = (total_weighted / total_weight) if total_weight > 0 else np.nan

        # Count indicators
        all_ind = {}
        for dr in domain_results.values():
            all_ind.update(dr)
        scored = sum(1 for k, v in all_ind.items() if not k.endswith('_raw') and not pd.isna(v))
        total_possible = sum(1 for k in all_ind if not k.endswith('_raw'))

        # Minimum threshold: need at least 4 indicators for a reliable score
        MIN_INDICATORS = 4
        if scored < MIN_INDICATORS:
            composite = np.nan

        # Build result
        result = {
            'unitid': uid,
            'year': year,
            'accounting_standard': acct,
            'distress_score': round(composite, 1) if not pd.isna(composite) else np.nan,
            'risk_category': self._categorize(composite),
            'data_completeness': round(scored / total_possible * 100, 0) if total_possible > 0 else 0,
            'indicators_scored': scored,
            'indicators_total': total_possible,
        }

        # Domain scores
        for dn in DISTRESS_DOMAINS:
            result[f'{dn}_score'] = round(domain_scores.get(dn, np.nan), 1)

        # Raw values
        for dr in domain_results.values():
            for k, v in dr.items():
                if k.endswith('_raw'):
                    result[k] = round(v, 4) if (not pd.isna(v) and not isinstance(v, complex)) else np.nan

        return result

    def _categorize(self, score):
        if pd.isna(score):
            return 'Insufficient Data'
        elif score < 20:
            return 'Healthy'
        elif score < 40:
            return 'Low Risk'
        elif score < 60:
            return 'Moderate Risk'
        elif score < 80:
            return 'High Risk'
        else:
            return 'Severe Distress'

    def score_all(self, target_year: int = None) -> pd.DataFrame:
        """Score all institutions (most recent year or specified year)."""
        results = []
        for uid in self.data:
            years = sorted(self.data[uid].keys())
            if not years:
                continue
            yr = target_year if (target_year and target_year in years) else years[-1]
            results.append(self.score_entity(uid, yr))

        df = pd.DataFrame(results)
        if len(df) > 0:
            print(f"\nScored {len(df)} institutions")
            print(f"\nRisk Distribution:")
            print(df['risk_category'].value_counts().to_string())
            print(f"\nAvg data completeness: {df['data_completeness'].mean():.0f}%")
            print(f"\nScore statistics:")
            print(df['distress_score'].describe().to_string())
        return df

    def score_all_years(self) -> pd.DataFrame:
        """Score every institution × every year."""
        results = []
        for uid in self.data:
            for year in sorted(self.data[uid].keys()):
                results.append(self.score_entity(uid, year))
        return pd.DataFrame(results)

    def integrate_with_master(self, master_path: str, output_path: str = None,
                              target_year: int = 2024) -> pd.DataFrame:
        """
        Score all IPEDS institutions and merge into the Hummingbird Master file.
        Matches on UNITID.
        """
        print(f"\n{'='*60}")
        print(f"INTEGRATING WITH HUMMINGBIRD MASTER")
        print(f"{'='*60}")

        master = pd.read_csv(master_path, encoding='latin-1', low_memory=False)
        print(f"Master file: {len(master)} institutions")

        mask_ipeds = master['data_source'] == 'IPEDS'
        print(f"IPEDS institutions: {mask_ipeds.sum()}")

        master['unitid_clean'] = master['unitid'].apply(
            lambda x: str(int(x)).strip() if pd.notna(x) else None
        )

        results = []
        matched = 0
        no_data = 0

        for idx, row in master[mask_ipeds].iterrows():
            uid = row['unitid_clean']
            if uid is None or uid not in self.data:
                no_data += 1
                continue

            available = sorted(self.data[uid].keys(), reverse=True)
            score_year = target_year if target_year in available else available[0]

            result = self.score_entity(uid, score_year)
            result['master_idx'] = idx
            results.append(result)
            matched += 1

        print(f"Matched and scored: {matched}")
        print(f"No IPEDS data found: {no_data}")

        if not results:
            return master

        scores_df = pd.DataFrame(results)

        # Columns to add to master
        new_cols = {
            'distress_score_ipeds': 'distress_score',
            'distress_category_ipeds': 'risk_category',
            'accounting_standard_ipeds': 'accounting_standard',
            'solvency_score_ipeds': 'solvency_score',
            'liquidity_score_ipeds': 'liquidity_score',
            'operating_score_ipeds': 'operating_performance_score',
            'enrollment_score_ipeds': 'enrollment_health_score',
            'academic_score_ipeds': 'academic_outcomes_score',
            'demand_score_ipeds': 'demand_score',
            'trend_score_ipeds': 'trend_score',
            'data_completeness_ipeds': 'data_completeness',
            'score_year_ipeds': 'year',
            # Key raw metrics
            'equity_ratio_raw_ipeds': 'equity_ratio_raw',
            'unrestricted_cushion_raw_ipeds': 'unrestricted_cushion_raw',
            'operating_margin_raw_ipeds': 'operating_margin_raw',
            'debt_ratio_raw_ipeds': 'debt_ratio_raw',
            'tuition_dependency_raw_ipeds': 'tuition_dependency_raw',
            'retention_rate_raw_ipeds': 'retention_rate_raw',
            'graduation_rate_raw_ipeds': 'graduation_rate_raw',
            'enrollment_trend_1yr_raw_ipeds': 'enrollment_trend_1yr_raw',
            'enrollment_trend_4yr_raw_ipeds': 'enrollment_trend_4yr_raw',
            'admissions_yield_raw_ipeds': 'admissions_yield_raw',
            'revenue_per_student_raw_ipeds': 'revenue_per_student_raw',
            'days_cash_raw_ipeds': 'days_cash_raw',
            'net_asset_trend_raw_ipeds': 'net_asset_trend_raw',
        }

        for mc in new_cols:
            if mc not in master.columns:
                master[mc] = np.nan

        for _, score_row in scores_df.iterrows():
            idx = score_row['master_idx']
            for mc, sc in new_cols.items():
                if sc in score_row.index:
                    master.at[idx, mc] = score_row[sc]

            # Update main columns
            if not pd.isna(score_row.get('distress_score')):
                master.at[idx, 'distress_score'] = score_row['distress_score']
                cat_map = {
                    'Healthy': 'Healthy', 'Low Risk': 'Low',
                    'Moderate Risk': 'Moderate', 'High Risk': 'High',
                    'Severe Distress': 'Critical', 'Insufficient Data': 'Healthy',
                }
                master.at[idx, 'distress_category'] = cat_map.get(
                    score_row['risk_category'], 'Healthy'
                )

        ipeds_scored = master.loc[mask_ipeds]
        print(f"\n--- Updated Master (IPEDS) ---")
        print(f"Distress distribution:")
        print(ipeds_scored['distress_category'].value_counts().to_string())

        if output_path:
            master.to_csv(output_path, index=False)
            print(f"\nSaved to: {output_path}")

        return master


# =============================================================================
# CONFIGURATION — Update these paths
# =============================================================================

IPEDS_FILES = {
    2020: 'hv_master_data/data/IPEDS/IPEDS20.csv',
    2021: 'hv_master_data/data/IPEDS/IPEDS21.csv',
    2022: 'hv_master_data/data/IPEDS/IPEDS22.csv',
    2023: 'hv_master_data/data/IPEDS/IPEDS23.csv',
    2024: 'hv_master_data/data/IPEDS/IPEDS24.csv',
}

MASTER_FILE = 'hv_master_data/data/Hummingbird_Master_Combined_v4.csv'
OUTPUT_FILE = 'hv_master_data/data/Hummingbird_Master_Combined_v4.csv'
SCORES_DETAIL_FILE = 'hv_master_data/data/ipeds_distress_scores_detail.csv'


# =============================================================================
# RUN
# =============================================================================

if __name__ == '__main__':
    import os

    print("=" * 70)
    print("IPEDS DISTRESS SCORING — HUMMINGBIRD INTEGRATION")
    print("=" * 70)

    # Step 1: Get UNITID list from master
    master = pd.read_csv(MASTER_FILE, encoding='latin-1', low_memory=False)
    ipeds_mask = master['data_source'] == 'IPEDS'
    target_unitids = set(
        str(int(x)) for x in master.loc[ipeds_mask, 'unitid'].dropna()
    )
    print(f"\nTarget UNITIDs from master: {len(target_unitids)}")

    # Step 2: Load IPEDS files
    engine = DistressIPEDSEngine()

    available_files = {yr: p for yr, p in IPEDS_FILES.items() if os.path.exists(p)}
    print(f"Files found: {len(available_files)} / {len(IPEDS_FILES)}")

    if not available_files:
        print("\n⚠️  No IPEDS files found! Update the paths in CONFIGURATION.")
        exit(1)

    engine.load_data(
        file_paths=available_files,
        filter_unitids=target_unitids
    )

    # Step 3: Score and integrate
    updated_master = engine.integrate_with_master(
        master_path=MASTER_FILE,
        output_path=OUTPUT_FILE,
        target_year=2024
    )

    # Step 4: Export detail
    all_scores = engine.score_all_years()
    all_scores.to_csv(SCORES_DETAIL_FILE, index=False)
    print(f"\nYear-by-year detail saved to: {SCORES_DETAIL_FILE}")

    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)