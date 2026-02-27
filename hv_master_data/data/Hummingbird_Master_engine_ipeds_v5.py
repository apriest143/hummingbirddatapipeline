"""
================================================================================
IPEDS Financial Distress Scoring Engine — Hummingbird Project  (v5)
================================================================================

Goal: Identify institutions across the full spectrum of financial and operational
stress — not just imminent closures, but organizations that may be 5-10 years
from crisis and open to partnerships (e.g., land asset revitalization).

================================================================================
v5 CHANGES vs v4
================================================================================

CHANGE 1 — EIN-based parent-subsidiary contamination detection

  134 IPEDS subsidiaries share a parent EIN AND share the parent's balance
  sheet (assets match parent within 1%). Their solvency domain inputs
  (equity_ratio, debt_ratio, days_cash, cushion) all reflect the parent
  institution's finances, not the subsidiary's standalone position.

  Two contamination failure modes:
    Mode A: Parent carries leverage → subsidiary inherits inflated distress
    Mode B: Parent has strong balance sheet → subsidiary looks well-capitalized
            despite operational losses (the primary false-negative problem)

  Detection at ingestion (EIN + asset matching):
    1. Find all EINs shared by 2+ UNITIDs
    2. Within each shared-EIN group, identify the parent as the largest-revenue
       institution
    3. Flag subsidiary if its assets match the parent's within 1%
       (confirms actual balance sheet sharing, not just EIN reuse)

  Outputs new columns:
    is_subsidiary_ipeds        — True if contaminated subsidiary
    parent_unitid_ipeds        — UNITID of parent institution
    parent_name_ipeds          — Name of parent institution
    na_months_expenses_ipeds   — Standalone solvency proxy (net_assets / monthly_exp)
    solvency_source_ipeds      — 'equity_ratio' | 'na_months' (documents which path)

CHANGE 2 — Branched solvency computation for subsidiaries

  For is_subsidiary_ipeds institutions, the full solvency domain is replaced
  with a standalone months-of-reserve calculation. This avoids ALL contaminated
  balance sheet fields (assets, liabilities, equity_ratio, cushion, days_cash).

  Standalone solvency inputs (both revenue-side, never contaminated):
    net_assets_2024   — from IRS990 / revenue-side data (subsidiary-level)
    expenses_2024     — subsidiary-level expenses

  Months-of-reserve → solvency score (0–100 distress scale):
    < 0 months  (negative NA):   100   — critical
    0–1  months:                   93
    1–3  months:                   80
    3–6  months:                   67
    6–12 months:                   47
    12–24 months:                  27
    24–60 months:                   7
    60+  months:                    0   — healthy

CHANGE 3 — Revenue velocity floor for subsidiaries

  For contaminated subsidiaries, enrollment_score is an unreliable signal
  (it looks at 2022–2024 which may show apparent stability even when revenue
  is collapsing). Revenue_2yr_pct captures what enrollment misses.

  Applied post-aggregation, subsidiaries only:
    rev_2yr < -20%:  floor = 45   (Moderate minimum)
    rev_2yr < -40%:  floor = 55
    rev_2yr < -60%:  floor = 65   (High Risk minimum)

  Final = max(floor, composite)  — never lowers a score

  Key cases corrected:
    Simon's Rock (-64% rev):  33.6 → 65  ✓ (margin -30%, closed 2025)
    Remington Online (-62%):  44.1 → 65  ✓
    J&W Charlotte (-22%):     27.2 → 45  ✓ (margin -34%)

CHANGE 4 (inherited from v4) — Enrollment velocity floor for non-subsidiaries
  Unchanged from v4. Still applies to private NP institutions with direct
  2022→2024 enrollment decline > 25% AND ongoing 1yr trend < -5%.

================================================================================
DOMAIN WEIGHTS (v5 — same as v4)
================================================================================
  Solvency 15% | Liquidity 10% | Operating 15% | Enrollment 25%
  Academic 15% | Demand 10%    | Trend 10%

================================================================================
OUTPUT COLUMNS (v5 additions vs v4)
================================================================================
  is_subsidiary_ipeds        — True if EIN/asset contamination confirmed
  parent_unitid_ipeds        — Parent UNITID (subsidiaries only)
  parent_name_ipeds          — Parent institution name (subsidiaries only)
  na_months_expenses_ipeds   — Net assets / (expenses/12) standalone months
  solvency_source_ipeds      — 'equity_ratio' | 'na_months'
  revenue_velocity_floor_ipeds — True if revenue floor was applied (subsidiaries)

================================================================================
USAGE
================================================================================
  1. Run ipeds_crossfill_v2.py first (produces enriched master with 990 fills)
  2. Update CONFIGURATION section paths below
  3. python Hummingbird_Master_engine_ipeds_v5.py

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

IPEDS_VARIABLE_SEARCHES = {
    'unitid': 'unitid',
    'institution_name': 'institution name',
    'sector': 'sector of institution',
    'control': 'control of institution',
    'size_category': 'institution size category',
    'total_enrollment': 'total  enrollment',
    'ft_enrollment': 'full-time enrollment',
    'pt_enrollment': 'part-time enrollment',
    'grad_enrollment': 'graduate enrollment',
    'ft_retention_rate': 'full-time retention rate',
    'graduation_rate': 'graduation rate, total cohort',
    'student_faculty_ratio': 'student-to-faculty ratio',
    'admissions_yield': 'admissions yield - total',
    'percent_admitted': 'percent admitted - total',
    # F2 / FASB
    'f2_total_assets':         '_f2.total assets',
    'f2_total_liabilities':    '_f2.total liabilities',
    'f2_total_net_assets':     '_f2.total net assets',
    'f2_unrestricted_na':      '_f2.total unrestricted net assets',
    'f2_restricted_na':        '_f2.total restricted net assets',
    'f2_total_revenues':       '_f2.total revenues and investment return',
    'f2_total_expenses':       '_f2.total expenses',
    'f2_change_na':            '_f2.total change in net assets',
    'f2_expendable_na':        '_f2.expendable net assets',
    'f2_lt_investments':       '_f2.long-term investments',
    'f2_ppe':                  '_f2.total plant, property',
    'f2_debt_ppe':             '_f2.debt related to property',
    'f2_tuition_fees':         '_f2.tuition and fees',
    'f2_federal_grants':       '_f2.federal grants and contracts - total',
    'f2_state_approp':         '_f2.state appropriations - total',
    'f2_private_gifts':        '_f2.private gifts, grants, and contracts - total',
    'f2_instruction':          '_f2.instruction-total amount',
    'f2_institutional_support':'_f2.institutional support-total amount',
    'f2_student_services':     '_f2.student service-total amount',
    # F1A / GASB
    'f1a_total_assets':        '_f1a.total assets',
    'f1a_total_liabilities':   '_f1a.total liabilities',
    'f1a_net_position':        '_f1a.net position',
    'f1a_expendable_na':       '_f1a.expendable net assets',
    'f1a_operating_income':    '_f1a.operating income',
    'f1a_total_revenues':      '_f1a.total all revenues',
    'f1a_instruction':         '_f1a.instruction - current year total',
    'f1a_tuition_fees':        '_f1a.tuition and fees, after deducting',
    # F3 / For-Profit
    'f3_total_assets':         '_f3.total assets',
    'f3_total_liabilities':    '_f3.total liabilities',
    'f3_total_equity':         '_f3.total equity',
    'f3_total_revenues':       '_f3.total revenues and investment return',
    'f3_total_expenses':       '_f3.total expenses',
    'f3_net_income':           '_f3.net income',
    'f3_ppe':                  '_f3.total plant, property',
    'f3_debt_ppe':             '_f3.plant-related debt',
    'f3_instruction':          '_f3.instruction-total amount',
    'f3_institutional_support':'_f3.institutional support-total amount',
    'f3_tuition_fees':         '_f3.tuition and fees',
    # Derived
    'equity_ratio_fasb':       'equity ratio (fasb)',
    'equity_ratio_gasb':       'equity ratio (gasb)',
    'tuition_pct_fasb':        'tuition and fees as a percent of core revenues (fasb)',
    'tuition_pct_gasb':        'tuition and fees as a percent of core revenues (gasb)',
    'endowment_per_fte':       'endowment assets (year end) per fte',
    # Staffing
    'avg_salary':              'average salary equated to 9 months of full-time instructional staff - all',
    'total_fte_staff':         'total fte staff',
}

TEXT_FIELDS    = {'unitid', 'institution_name', 'sector', 'control', 'size_category'}
FASB_INDICATOR = 'f2_total_assets'
GASB_INDICATOR = 'f1a_total_assets'


# =============================================================================
# DISTRESS DOMAIN DEFINITIONS
# =============================================================================

DISTRESS_DOMAINS = {
    'solvency': {
        'weight': 0.15,
        'indicators': {
            'equity_ratio':         {'weight': 0.28},
            'unrestricted_cushion': {'weight': 0.22},
            'debt_ratio':           {'weight': 0.18},
            'expendable_na_ratio':  {'weight': 0.17},
            'debt_to_ppe':          {'weight': 0.10},
            'revenue_runway':       {'weight': 0.15},
        }
    },
    'liquidity': {
        'weight': 0.10,
        'indicators': {
            'days_cash':         {'weight': 0.50},
            'endowment_cushion': {'weight': 0.50},
        }
    },
    'operating_performance': {
        'weight': 0.15,
        'indicators': {
            'operating_margin':     {'weight': 0.35},
            'instruction_ratio':    {'weight': 0.20},
            'admin_overhead_ratio': {'weight': 0.20},
            'tuition_dependency':   {'weight': 0.25},
        }
    },
    'enrollment_health': {
        'weight': 0.25,
        'indicators': {
            'enrollment_trend_1yr': {'weight': 0.20},
            'enrollment_trend_4yr': {'weight': 0.15},
            'enrollment_chg_3yr':   {'weight': 0.20},
            'ft_share':             {'weight': 0.15},
            'enrollment_size':      {'weight': 0.10},
            'revenue_per_student':  {'weight': 0.20},
        }
    },
    'academic_outcomes': {
        'weight': 0.15,
        'indicators': {
            'retention_rate':        {'weight': 0.40},
            'graduation_rate':       {'weight': 0.35},
            'student_faculty_ratio': {'weight': 0.25},
        }
    },
    'demand': {
        'weight': 0.10,
        'indicators': {
            'admissions_yield': {'weight': 0.50},
            'selectivity':      {'weight': 0.50},
        }
    },
    'trend': {
        'weight': 0.10,
        'indicators': {
            'revenue_trend':   {'weight': 0.25},
            'net_asset_trend': {'weight': 0.25},
            'retention_trend': {'weight': 0.20},
            'staff_trend':     {'weight': 0.15},
            'salary_trend':    {'weight': 0.15},
        }
    },
}

assert abs(sum(d['weight'] for d in DISTRESS_DOMAINS.values()) - 1.0) < 1e-9, \
    "Domain weights must sum to 1.0"


# =============================================================================
# ENGINE CLASS
# =============================================================================

class DistressIPEDSEngine:
    """
    Financial distress scoring engine for IPEDS-reporting institutions.

    v5 key additions over v4:
      - EIN-based parent-subsidiary contamination detection
      - Branched solvency computation: subsidiaries use months-of-reserve
        (standalone), independents use equity_ratio path (unchanged)
      - Revenue velocity floor for subsidiaries with revenue cliff
      - All v4 features retained (enrollment velocity floor, tightened
        likely_closed, direct enrollment_chg_3yr)
    """

    def __init__(self):
        self.data = {}              # {unitid: {year: {field: value}}}
        self.accounting_std = {}    # {unitid: 'fasb'|'gasb'|'for_profit'|'irs990'}
        self._master_rows = {}      # {uid: master_row_series}

        # v5: EIN contamination registry
        # Populated in integrate_with_master() before scoring begins
        self._subsidiary_flags = {}   # {uid: True/False}
        self._parent_uid = {}         # {uid: parent_uid}
        self._parent_name = {}        # {uid: parent_name}

    # =========================================================================
    # DATA LOADING
    # =========================================================================

    def load_data(self, file_paths: dict, filter_unitids: set = None):
        for year, path in sorted(file_paths.items()):
            print(f"Loading {year} from {path}...")
            df = pd.read_csv(path, encoding='latin-1', low_memory=False)
            col_map = self._build_column_map(df.columns.tolist())
            df_std = pd.DataFrame()
            df_std['unitid'] = df['unitid'].astype(str).str.strip()

            for std_name, orig_col in col_map.items():
                if std_name == 'unitid':
                    continue
                if std_name in TEXT_FIELDS:
                    df_std[std_name] = df[orig_col]
                else:
                    df_std[std_name] = pd.to_numeric(df[orig_col], errors='coerce')

            if filter_unitids:
                filter_set = {str(u).strip() for u in filter_unitids}
                df_std = df_std[df_std['unitid'].isin(filter_set)]

            loaded = 0
            for _, row in df_std.iterrows():
                uid = row['unitid']
                if uid not in self.data:
                    self.data[uid] = {}
                self.data[uid][year] = row.to_dict()
                loaded += 1

                if pd.notna(row.get(FASB_INDICATOR)):
                    self.accounting_std[uid] = 'fasb'
                elif pd.notna(row.get(GASB_INDICATOR)):
                    self.accounting_std[uid] = 'gasb'
                elif pd.notna(row.get('f3_total_assets')):
                    self.accounting_std[uid] = 'for_profit'

            mapped = len(col_map)
            total  = len(IPEDS_VARIABLE_SEARCHES)
            print(f"  → {loaded} institutions, {mapped}/{total} variables mapped")

        multi = sum(1 for d in self.data.values() if len(d) > 1)
        print(f"\nTotal: {len(self.data)} institutions")
        print(f"Multi-year data: {multi}/{len(self.data)}")
        acct = pd.Series(list(self.accounting_std.values()))
        print(f"Accounting standards: {dict(acct.value_counts())}")

    def _build_column_map(self, columns: list) -> dict:
        col_map = {}
        cols_lower = [c.lower() for c in columns]
        for std_name, search_term in IPEDS_VARIABLE_SEARCHES.items():
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
    # v5 — EIN PARENT-SUBSIDIARY DETECTION
    # Call this ONCE after master is loaded, before any scoring.
    # =========================================================================

    def detect_subsidiaries(self, master_df: pd.DataFrame) -> int:
        """
        Identify contaminated subsidiaries using EIN + asset matching.

        Logic:
          1. Find EINs shared by 2+ UNITIDs in the master file
          2. Within each shared-EIN group, the parent = institution with
             highest revenue
          3. A sibling is flagged as contaminated if its assets are within
             1% of the parent's assets (confirms balance sheet sharing)

        Populates self._subsidiary_flags, self._parent_uid, self._parent_name.
        Returns count of confirmed subsidiaries.
        """
        ipeds = master_df[master_df['data_source'] == 'IPEDS'].copy()
        ipeds['_uid']    = ipeds['unitid'].apply(
            lambda x: str(int(x)).strip() if pd.notna(x) else None
        )
        ipeds['_rev']    = pd.to_numeric(ipeds.get('revenue_2024'), errors='coerce')
        ipeds['_assets'] = pd.to_numeric(ipeds.get('assets_2024'),  errors='coerce')
        ipeds['_ein']    = ipeds.get('ein_clean', pd.Series(dtype=str))
        ipeds['_name']   = ipeds.get('institution_name', pd.Series(dtype=str))

        # Drop rows without EIN or uid
        ipeds = ipeds.dropna(subset=['_ein', '_uid'])
        ipeds = ipeds[ipeds['_ein'].astype(str).str.strip() != '']

        ein_counts = ipeds['_ein'].value_counts()
        shared_eins = set(ein_counts[ein_counts > 1].index)

        n_flagged = 0
        for ein, group in ipeds.groupby('_ein'):
            if ein not in shared_eins:
                continue

            # Parent = highest revenue in group
            parent_row = group.loc[group['_rev'].fillna(0).idxmax()]
            parent_uid    = str(parent_row['_uid'])
            parent_name   = str(parent_row['_name'])
            parent_assets = parent_row['_assets']

            if pd.isna(parent_assets) or parent_assets == 0:
                continue

            for _, sibling in group.iterrows():
                sib_uid = str(sibling['_uid'])
                if sib_uid == parent_uid:
                    continue

                sib_assets = sibling['_assets']
                if pd.isna(sib_assets):
                    continue

                # Asset match within 1% confirms balance sheet sharing
                if abs(sib_assets - parent_assets) / abs(parent_assets) < 0.01:
                    self._subsidiary_flags[sib_uid] = True
                    self._parent_uid[sib_uid]        = parent_uid
                    self._parent_name[sib_uid]       = parent_name
                    n_flagged += 1

        print(f"EIN subsidiary detection: {n_flagged} contaminated subsidiaries "
              f"identified out of {len(shared_eins)} shared-EIN groups")
        return n_flagged

    # =========================================================================
    # HELPERS
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
        """Convert raw metric to 0-1 distress score via linear interpolation."""
        if pd.isna(value) or isinstance(value, complex):
            return np.nan
        if invert:
            if value <= healthy:  return 0.0
            if value >= distress: return 1.0
            return (value - healthy) / (distress - healthy)
        else:
            if value >= healthy:  return 0.0
            if value <= distress: return 1.0
            return (healthy - value) / (healthy - distress)

    def _get_financial(self, data: dict, uid: str, fasb_field: str,
                       gasb_field: str = None, fp_field: str = None):
        acct = self.accounting_std.get(uid, 'unknown')
        if acct in ('fasb', 'irs990'):
            return self._safe_get(data, fasb_field)
        elif acct == 'gasb' and gasb_field:
            return self._safe_get(data, gasb_field)
        elif acct == 'for_profit' and fp_field:
            return self._safe_get(data, fp_field)
        return np.nan

    # =========================================================================
    # YEAR USABILITY CHECK
    # =========================================================================

    def _year_is_usable(self, uid: str, year: int) -> bool:
        if uid not in self.data or year not in self.data[uid]:
            return False
        d = self.data[uid][year]
        if pd.notna(d.get('total_enrollment')):
            return True
        financial_fields = [
            'f2_total_assets', 'f2_total_revenues',
            'f1a_total_assets', 'f1a_total_revenues',
            'f3_total_assets',  'f3_total_revenues',
        ]
        return any(pd.notna(d.get(f)) for f in financial_fields)

    # =========================================================================
    # LIKELY_CLOSED DETERMINATION  (v4 tightened logic, unchanged)
    # =========================================================================

    def _is_likely_closed(self, uid: str, master_row, target_year: int) -> bool:
        """
        Returns True only when the institution has NO data footprint in either
        of the two most recent years: no enrollment and no revenue for 2023 or 2024.
        """
        for yr in [target_year, target_year - 1]:
            if yr in self.data.get(uid, {}):
                d = self.data[uid][yr]
                if pd.notna(d.get('total_enrollment')):
                    return False
                financial_fields = [
                    'f2_total_revenues', 'f2_total_assets',
                    'f1a_total_revenues', 'f1a_total_assets',
                    'f3_total_revenues', 'f3_total_assets',
                ]
                if any(pd.notna(d.get(f)) for f in financial_fields):
                    return False

        if master_row is not None:
            for yr_suffix in ['2024', '2023']:
                rev = master_row.get(f'revenue_{yr_suffix}')
                enr = master_row.get(f'enrollment_{yr_suffix}')
                if pd.notna(rev) or pd.notna(enr):
                    return False

        return True

    # =========================================================================
    # 990 INJECTION (unchanged from v3/v4)
    # =========================================================================

    def _inject_990_fills(self, uid: str, master_row, target_year: int):
        if master_row is None or uid not in self.data:
            return
        MULTI_YEAR = [
            'f2_total_revenues', 'f2_total_expenses',
            'f2_total_assets', 'f2_total_liabilities', 'f2_total_net_assets',
            'f1a_total_revenues', 'f1a_total_assets',
            'f1a_total_liabilities', 'f1a_net_position',
            'f3_total_revenues', 'f3_total_expenses',
            'f3_total_assets', 'f3_total_liabilities', 'f3_total_equity',
        ]
        for year in list(self.data[uid].keys()):
            yd = self.data[uid][year]
            for col in MULTI_YEAR:
                mc = f'{col}_{year}'
                if mc not in master_row.index:
                    continue
                val = master_row[mc]
                if pd.isna(val):
                    continue
                if pd.isna(yd.get(col, np.nan)):
                    yd[col] = float(val)

        SINGLE_YEAR = [
            'f2_unrestricted_na', 'f2_ppe', 'f2_debt_ppe',
            'f3_ppe', 'f3_debt_ppe',
        ]
        if target_year in self.data[uid]:
            yd = self.data[uid][target_year]
            for col in SINGLE_YEAR:
                if col not in master_row.index:
                    continue
                val = master_row[col]
                if pd.isna(val):
                    continue
                if pd.isna(yd.get(col, np.nan)):
                    yd[col] = float(val)

    # =========================================================================
    # DOMAIN COMPUTATIONS
    # =========================================================================

    def compute_solvency(self, data: dict, uid: str,
                         master_row=None) -> dict:
        """
        v5: Branched computation.
          - If uid is a confirmed subsidiary → standalone months-of-reserve
          - Otherwise → standard equity_ratio path (unchanged from v4)
        """
        is_sub = self._subsidiary_flags.get(uid, False)

        if is_sub:
            return self._compute_solvency_subsidiary(data, uid, master_row)
        else:
            return self._compute_solvency_standard(data, uid)

    def _compute_solvency_standard(self, data: dict, uid: str) -> dict:
        """Standard solvency computation — unchanged from v4."""
        r = {}
        acct = self.accounting_std.get(uid, 'unknown')

        # Equity ratio
        if acct == 'fasb':
            eq = self._safe_get(data, 'equity_ratio_fasb')
        elif acct == 'gasb':
            eq = self._safe_get(data, 'equity_ratio_gasb')
        elif acct == 'for_profit':
            equity = self._safe_get(data, 'f3_total_equity')
            assets = self._safe_get(data, 'f3_total_assets')
            eq = self._safe_divide(equity, assets) * 100 \
                if not pd.isna(self._safe_divide(equity, assets)) else np.nan
        elif acct == 'irs990':
            na     = self._safe_get(data, 'f2_total_net_assets')
            assets = self._safe_get(data, 'f2_total_assets')
            eq = self._safe_divide(na, assets) * 100 \
                if not pd.isna(self._safe_divide(na, assets)) else np.nan
        else:
            eq = np.nan
        if not pd.isna(eq):
            eq = eq / 100.0
        r['equity_ratio']     = self._score(eq, 0.40, -0.10)
        r['equity_ratio_raw'] = eq

        # Unrestricted cushion
        unrestricted = self._get_financial(data, uid, 'f2_unrestricted_na')
        expenses     = self._get_financial(data, uid, 'f2_total_expenses',
                                           None, 'f3_total_expenses')
        cushion = self._safe_divide(unrestricted, expenses)
        r['unrestricted_cushion']     = self._score(cushion, 0.25, -0.10)
        r['unrestricted_cushion_raw'] = cushion

        # Debt ratio
        assets      = self._get_financial(data, uid, 'f2_total_assets',
                                          'f1a_total_assets', 'f3_total_assets')
        liabilities = self._get_financial(data, uid, 'f2_total_liabilities',
                                          'f1a_total_liabilities', 'f3_total_liabilities')
        debt_ratio = self._safe_divide(liabilities, assets)
        r['debt_ratio']     = self._score(debt_ratio, 0.50, 1.0, invert=True)
        r['debt_ratio_raw'] = debt_ratio

        # Expendable net assets ratio
        expendable = self._get_financial(data, uid, 'f2_expendable_na', 'f1a_expendable_na')
        if pd.isna(expenses):
            expenses = self._get_financial(data, uid, 'f2_total_expenses',
                                           None, 'f3_total_expenses')
        exp_ratio = self._safe_divide(expendable, expenses) if not pd.isna(expenses) \
                    else self._safe_divide(expendable, assets)
        r['expendable_na_ratio']     = self._score(exp_ratio, 0.30, -0.05)
        r['expendable_na_ratio_raw'] = exp_ratio

        # Debt to PP&E
        debt_ppe = self._get_financial(data, uid, 'f2_debt_ppe', None, 'f3_debt_ppe')
        ppe      = self._get_financial(data, uid, 'f2_ppe',      None, 'f3_ppe')
        d2ppe    = self._safe_divide(debt_ppe, ppe)
        r['debt_to_ppe']     = self._score(d2ppe, 0.50, 1.20, invert=True)
        r['debt_to_ppe_raw'] = d2ppe

        # Revenue runway
        net_assets = self._get_financial(data, uid, 'f2_total_net_assets',
                                         'f1a_net_position', 'f3_total_equity')
        revenue    = self._get_financial(data, uid, 'f2_total_revenues',
                                         'f1a_total_revenues', 'f3_total_revenues')
        if pd.isna(expenses):
            expenses = self._get_financial(data, uid, 'f2_total_expenses',
                                           None, 'f3_total_expenses')
        runway = np.nan
        if (not pd.isna(net_assets) and not pd.isna(revenue) and
                not pd.isna(expenses) and revenue > 0):
            annual_loss = expenses - revenue
            if annual_loss > 0 and net_assets > 0:
                runway = net_assets / annual_loss
            elif annual_loss <= 0:
                runway = np.nan     # surplus → not penalized
            else:
                runway = 0.0        # net_assets ≤ 0 and losing money → insolvent
        r['revenue_runway']     = self._score(runway, 10.0, 2.0)
        r['revenue_runway_raw'] = runway

        r['_solvency_source'] = 'equity_ratio'
        return r

    def _compute_solvency_subsidiary(self, data: dict, uid: str,
                                     master_row=None) -> dict:
        """
        v5 standalone solvency for contaminated subsidiaries.

        Replaces all balance-sheet-derived inputs with months-of-reserve:
          na_months = net_assets_2024 / (expenses_2024 / 12)

        Both inputs are revenue-side / subsidiary-level and are never
        contaminated by the parent balance sheet.

        Solvency score mapping (0–100 distress scale):
          < 0 months   → 100  (negative net assets: critical)
          0–1          →  93
          1–3          →  80
          3–6          →  67
          6–12         →  47
          12–24        →  27
          24–60        →   7
          60+          →   0  (healthy: 5+ years of reserves)
        """
        r = {}

        # Prefer master flat columns (already 990-enriched and cross-filled)
        na  = np.nan
        exp = np.nan
        if master_row is not None:
            na  = pd.to_numeric(master_row.get('net_assets_2024'),  errors='coerce')
            exp = pd.to_numeric(master_row.get('expenses_2024'),    errors='coerce')
            if pd.isna(na):
                na  = pd.to_numeric(master_row.get('net_assets_2023'), errors='coerce')
            if pd.isna(exp):
                exp = pd.to_numeric(master_row.get('expenses_2023'),   errors='coerce')

        # Fallback to IPEDS data dict
        if pd.isna(na):
            na = self._get_financial(data, uid, 'f2_total_net_assets',
                                     'f1a_net_position', 'f3_total_equity')
        if pd.isna(exp):
            exp = self._get_financial(data, uid, 'f2_total_expenses',
                                      None, 'f3_total_expenses')

        # Compute months of reserve
        if pd.isna(na) or pd.isna(exp) or exp <= 0:
            na_months = np.nan
            sol_score = np.nan
        else:
            na_months = na / (exp / 12.0)
            if na_months < 0:   sol_score = 100.0
            elif na_months < 1:  sol_score =  93.0
            elif na_months < 3:  sol_score =  80.0
            elif na_months < 6:  sol_score =  67.0
            elif na_months < 12: sol_score =  47.0
            elif na_months < 24: sol_score =  27.0
            elif na_months < 60: sol_score =   7.0
            else:                sol_score =   0.0

        # Normalise to 0–1 for indicator weighting
        sol_norm = sol_score / 100.0 if not pd.isna(sol_score) else np.nan

        # We surface a single synthetic indicator 'na_months_score' that
        # covers the full solvency domain for this institution.
        # All standard indicators are set to NaN so they are skipped in
        # the weighted aggregation — only na_months_score contributes.
        r['equity_ratio']          = np.nan
        r['equity_ratio_raw']      = np.nan
        r['unrestricted_cushion']  = np.nan
        r['unrestricted_cushion_raw'] = np.nan
        r['debt_ratio']            = np.nan
        r['debt_ratio_raw']        = np.nan
        r['expendable_na_ratio']   = np.nan
        r['expendable_na_ratio_raw'] = np.nan
        r['debt_to_ppe']           = np.nan
        r['debt_to_ppe_raw']       = np.nan
        r['revenue_runway']        = sol_norm   # reuse runway slot as the single signal
        r['revenue_runway_raw']    = na_months
        r['na_months_score']       = sol_score  # stored for output column
        r['na_months_raw']         = na_months  # stored for output column
        r['_solvency_source']      = 'na_months'
        return r

    def compute_liquidity(self, data: dict, uid: str) -> dict:
        r = {}
        unrestricted = self._get_financial(data, uid, 'f2_unrestricted_na')
        expenses     = self._get_financial(data, uid, 'f2_total_expenses')
        if not pd.isna(unrestricted) and not pd.isna(expenses) and expenses > 0:
            days = max(0, (unrestricted / expenses) * 365)
            r['days_cash']     = self._score(days, 90, 15)
            r['days_cash_raw'] = days
        else:
            r['days_cash']     = np.nan
            r['days_cash_raw'] = np.nan

        endowment = self._safe_get(data, 'endowment_per_fte')
        r['endowment_cushion']     = self._score(endowment, 10000, 500)
        r['endowment_cushion_raw'] = endowment
        return r

    def compute_operating(self, data: dict, uid: str) -> dict:
        r = {}
        acct = self.accounting_std.get(uid, 'unknown')

        if acct == 'fasb':
            revenue  = self._safe_get(data, 'f2_total_revenues')
            expenses = self._safe_get(data, 'f2_total_expenses')
        elif acct == 'gasb':
            revenue    = self._safe_get(data, 'f1a_total_revenues')
            op_income  = self._safe_get(data, 'f1a_operating_income')
            expenses   = (revenue - op_income) \
                if not pd.isna(revenue) and not pd.isna(op_income) else np.nan
        elif acct == 'for_profit':
            revenue  = self._safe_get(data, 'f3_total_revenues')
            expenses = self._safe_get(data, 'f3_total_expenses')
        elif acct == 'irs990':
            revenue  = self._safe_get(data, 'f2_total_revenues')
            expenses = self._safe_get(data, 'f2_total_expenses')
        else:
            revenue, expenses = np.nan, np.nan

        margin = self._safe_divide(revenue - expenses, abs(revenue)) \
            if not pd.isna(revenue) and not pd.isna(expenses) and revenue != 0 else np.nan
        r['operating_margin']     = self._score(margin, 0.05, -0.15)
        r['operating_margin_raw'] = margin

        if acct == 'fasb':
            instruction = self._safe_get(data, 'f2_instruction')
            total_exp   = self._safe_get(data, 'f2_total_expenses')
        elif acct == 'gasb':
            instruction = self._safe_get(data, 'f1a_instruction')
            total_exp   = expenses
        elif acct == 'for_profit':
            instruction = self._safe_get(data, 'f3_instruction')
            total_exp   = self._safe_get(data, 'f3_total_expenses')
        else:
            instruction, total_exp = np.nan, np.nan
        inst_ratio = self._safe_divide(instruction, total_exp)
        r['instruction_ratio']     = self._score(inst_ratio, 0.30, 0.15)
        r['instruction_ratio_raw'] = inst_ratio

        if acct == 'fasb':
            inst_support = self._safe_get(data, 'f2_institutional_support')
        elif acct == 'for_profit':
            inst_support = self._safe_get(data, 'f3_institutional_support')
        else:
            inst_support = np.nan
        admin_ratio = self._safe_divide(inst_support, total_exp)
        r['admin_overhead_ratio']     = self._score(admin_ratio, 0.25, 0.45, invert=True)
        r['admin_overhead_ratio_raw'] = admin_ratio

        if acct == 'fasb':
            tuition_pct = self._safe_get(data, 'tuition_pct_fasb')
        elif acct == 'gasb':
            tuition_pct = self._safe_get(data, 'tuition_pct_gasb')
        elif acct == 'for_profit':
            tuition     = self._safe_get(data, 'f3_tuition_fees')
            tuition_pct = self._safe_divide(tuition, revenue) * 100 \
                if not pd.isna(self._safe_divide(tuition, revenue)) else np.nan
        else:
            tuition_pct = np.nan
        r['tuition_dependency']     = self._score(tuition_pct, 60, 85, invert=True)
        r['tuition_dependency_raw'] = tuition_pct
        return r

    def compute_enrollment(self, data: dict, uid: str, year: int,
                           master_row=None) -> dict:
        """
        v4/v5: enrollment_chg_3yr uses direct 2022→2024 flat columns.
        Unchanged from v4.
        """
        r = {}
        years_data   = self.data.get(uid, {})
        total_enroll = self._safe_get(data, 'total_enrollment')
        ft_enroll    = self._safe_get(data, 'ft_enrollment')

        # 1yr trend
        prior_years = sorted([y for y in years_data if y < year], reverse=True)
        if prior_years:
            prior        = years_data[prior_years[0]]
            prior_enroll = self._safe_get(prior, 'total_enrollment')
            gap          = max(year - prior_years[0], 1)
            if not pd.isna(total_enroll) and not pd.isna(prior_enroll) and prior_enroll > 0:
                change_1yr = (total_enroll / prior_enroll) ** (1/gap) - 1
                r['enrollment_trend_1yr']     = self._score(change_1yr, 0.0, -0.10)
                r['enrollment_trend_1yr_raw'] = change_1yr
            else:
                r['enrollment_trend_1yr']     = np.nan
                r['enrollment_trend_1yr_raw'] = np.nan
        else:
            r['enrollment_trend_1yr']     = np.nan
            r['enrollment_trend_1yr_raw'] = np.nan

        # 4yr trend
        oldest_years = sorted(years_data.keys())
        if len(oldest_years) >= 2 and oldest_years[0] < year:
            oldest        = years_data[oldest_years[0]]
            oldest_enroll = self._safe_get(oldest, 'total_enrollment')
            gap           = max(year - oldest_years[0], 1)
            if not pd.isna(total_enroll) and not pd.isna(oldest_enroll) \
                    and oldest_enroll > 0 and gap > 0:
                change_long = (total_enroll / oldest_enroll) ** (1/gap) - 1
                r['enrollment_trend_4yr']     = self._score(change_long, 0.0, -0.08)
                r['enrollment_trend_4yr_raw'] = change_long
            else:
                r['enrollment_trend_4yr']     = np.nan
                r['enrollment_trend_4yr_raw'] = np.nan
        else:
            r['enrollment_trend_4yr']     = np.nan
            r['enrollment_trend_4yr_raw'] = np.nan

        # Direct 2022→2024 enrollment change
        chg_3yr    = np.nan
        enr_direct = np.nan

        if master_row is not None:
            enr_2024 = master_row.get('enrollment_2024', np.nan)
            enr_2022 = master_row.get('enrollment_2022', np.nan)
            if pd.notna(enr_2024) and pd.notna(enr_2022) and float(enr_2022) > 0:
                chg_3yr    = (float(enr_2024) - float(enr_2022)) / float(enr_2022)
                enr_direct = chg_3yr

        if pd.isna(chg_3yr):
            target_base_yr = year - 3
            candidate_base_yrs = sorted(
                [y for y in years_data if y <= target_base_yr], reverse=True
            )
            if candidate_base_yrs:
                base_yr     = candidate_base_yrs[0]
                base_enroll = self._safe_get(years_data[base_yr], 'total_enrollment')
                if not pd.isna(total_enroll) and not pd.isna(base_enroll) \
                        and base_enroll > 0:
                    chg_3yr    = (total_enroll - base_enroll) / base_enroll
                    enr_direct = chg_3yr

        r['enrollment_chg_3yr']     = self._score(chg_3yr, 0.0, -0.30)
        r['enrollment_chg_3yr_raw'] = chg_3yr
        r['_enr_direct_22_24']      = enr_direct

        # FT share
        ft_share = self._safe_divide(ft_enroll, total_enroll)
        r['ft_share']     = self._score(ft_share, 0.60, 0.30)
        r['ft_share_raw'] = ft_share

        # Enrollment size
        if not pd.isna(total_enroll):
            if   total_enroll >= 1000: r['enrollment_size'] = 0.0
            elif total_enroll >= 500:  r['enrollment_size'] = 0.2
            elif total_enroll >= 200:  r['enrollment_size'] = 0.5
            elif total_enroll >= 50:   r['enrollment_size'] = 0.7
            else:                      r['enrollment_size'] = 0.9
        else:
            r['enrollment_size'] = np.nan
        r['enrollment_size_raw'] = total_enroll

        # Revenue per student
        revenue = self._get_financial(data, uid, 'f2_total_revenues',
                                      'f1a_total_revenues', 'f3_total_revenues')
        rev_per_student = self._safe_divide(revenue, total_enroll)
        r['revenue_per_student']     = self._score(rev_per_student, 15000, 5000)
        r['revenue_per_student_raw'] = rev_per_student

        # Small-school cliff multiplier
        cliff_mult = 1.0
        if not pd.isna(total_enroll) and not pd.isna(chg_3yr):
            if total_enroll < 500 and chg_3yr < -0.20:
                size_factor = max(0.0, (500 - total_enroll) / 300)
                chg_factor  = max(0.0, (-chg_3yr - 0.20) / 0.20)
                cliff_mult  = 1.0 + 0.40 * min(size_factor * chg_factor, 1.0)
        r['_cliff_multiplier'] = cliff_mult

        return r

    def compute_academic(self, data: dict, uid: str) -> dict:
        r = {}
        retention = self._safe_get(data, 'ft_retention_rate')
        r['retention_rate']     = self._score(retention, 70, 40)
        r['retention_rate_raw'] = retention

        grad_rate = self._safe_get(data, 'graduation_rate')
        r['graduation_rate']     = self._score(grad_rate, 40, 15)
        r['graduation_rate_raw'] = grad_rate

        sfr = self._safe_get(data, 'student_faculty_ratio')
        r['student_faculty_ratio']     = self._score(sfr, 20, 35, invert=True)
        r['student_faculty_ratio_raw'] = sfr
        return r

    def compute_demand(self, data: dict, uid: str) -> dict:
        r = {}
        yld = self._safe_get(data, 'admissions_yield')
        r['admissions_yield']     = self._score(yld, 35, 15)
        r['admissions_yield_raw'] = yld

        pct_admitted = self._safe_get(data, 'percent_admitted')
        r['selectivity']     = self._score(pct_admitted, 80, 98, invert=True)
        r['selectivity_raw'] = pct_admitted
        return r

    def compute_trends(self, uid: str, year: int) -> dict:
        r = {}
        years_data  = self.data.get(uid, {})
        current     = years_data.get(year, {})
        prior_years = sorted([y for y in years_data if y < year], reverse=True)

        nan_result = {k: np.nan for k in [
            'revenue_trend', 'revenue_trend_raw',
            'net_asset_trend', 'net_asset_trend_raw',
            'retention_trend', 'retention_trend_raw',
            'staff_trend', 'staff_trend_raw',
            'salary_trend', 'salary_trend_raw',
        ]}
        if not prior_years or not current:
            return nan_result

        prior = years_data[prior_years[0]]
        gap   = max(year - prior_years[0], 1)

        curr_rev  = self._get_financial(current, uid, 'f2_total_revenues',
                                        'f1a_total_revenues', 'f3_total_revenues')
        prior_rev = self._get_financial(prior,   uid, 'f2_total_revenues',
                                        'f1a_total_revenues', 'f3_total_revenues')
        if not pd.isna(curr_rev) and not pd.isna(prior_rev) \
                and prior_rev > 0 and curr_rev > 0:
            rev_change = (curr_rev / prior_rev) ** (1/gap) - 1
            r['revenue_trend']     = self._score(rev_change, 0.0, -0.10)
            r['revenue_trend_raw'] = rev_change
        else:
            r['revenue_trend']     = np.nan
            r['revenue_trend_raw'] = np.nan

        curr_na  = self._get_financial(current, uid, 'f2_total_net_assets',
                                       'f1a_net_position', 'f3_total_equity')
        prior_na = self._get_financial(prior,   uid, 'f2_total_net_assets',
                                       'f1a_net_position', 'f3_total_equity')
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
            r['net_asset_trend']     = self._score(na_change, 0.0, -0.10)
            r['net_asset_trend_raw'] = na_change
        else:
            r['net_asset_trend']     = np.nan
            r['net_asset_trend_raw'] = np.nan

        curr_ret  = self._safe_get(current, 'ft_retention_rate')
        prior_ret = self._safe_get(prior,   'ft_retention_rate')
        if not pd.isna(curr_ret) and not pd.isna(prior_ret):
            ret_change = (curr_ret - prior_ret) / gap
            r['retention_trend']     = self._score(ret_change, 0, -5)
            r['retention_trend_raw'] = ret_change
        else:
            r['retention_trend']     = np.nan
            r['retention_trend_raw'] = np.nan

        curr_staff  = self._safe_get(current, 'total_fte_staff')
        prior_staff = self._safe_get(prior,   'total_fte_staff')
        if not pd.isna(curr_staff) and not pd.isna(prior_staff) and prior_staff > 0:
            staff_change = (curr_staff / prior_staff) ** (1/gap) - 1
            r['staff_trend']     = self._score(staff_change, -0.02, -0.15)
            r['staff_trend_raw'] = staff_change
        else:
            r['staff_trend']     = np.nan
            r['staff_trend_raw'] = np.nan

        curr_sal  = self._safe_get(current, 'avg_salary')
        prior_sal = self._safe_get(prior,   'avg_salary')
        if not pd.isna(curr_sal) and not pd.isna(prior_sal) and prior_sal > 0:
            sal_change = (curr_sal / prior_sal) ** (1/gap) - 1
            r['salary_trend']     = self._score(sal_change, 0.02, -0.03)
            r['salary_trend_raw'] = sal_change
        else:
            r['salary_trend']     = np.nan
            r['salary_trend_raw'] = np.nan

        return r

    # =========================================================================
    # v4 — ENROLLMENT VELOCITY FLOOR (for non-subsidiary private NP schools)
    # Unchanged from v4.
    # =========================================================================

    def _apply_enrollment_floor(self, composite: float, uid: str,
                                 enrollment_domain_score: float,
                                 enr_direct_22_24: float,
                                 enr_trend_1yr: float,
                                 total_enrollment: float) -> tuple:
        """
        Returns (adjusted_composite, floor_applied: bool, severity: str).

        NOT applied to subsidiaries (they get the revenue velocity floor instead).

        Trigger conditions (ALL must be true):
          a. Direct 2022→2024 decline > 25%
          b. 1yr trend < -5%  (ongoing, not recovered)
          c. Private institution (FASB or IRS990) AND NOT a confirmed subsidiary
          d. Enrollment < 10,000

        Floor formula:
          severity_mult = 0.60 (≥50% decline) | 0.45 (≥35%) | 0.30 (≥25%)
          floor_score   = 40 + max(0, enrollment_domain_score - 40) × severity_mult
          final         = max(floor_score, composite)   ← never lowers a score
        """
        # Subsidiaries use the revenue velocity floor instead
        if self._subsidiary_flags.get(uid, False):
            return composite, False, None

        acct = self.accounting_std.get(uid, 'unknown')
        if acct not in ('fasb', 'irs990'):
            return composite, False, None

        if not pd.isna(total_enrollment) and total_enrollment >= 10000:
            return composite, False, None

        if pd.isna(enr_direct_22_24) or pd.isna(enr_trend_1yr):
            return composite, False, None
        if enr_direct_22_24 >= -0.25 or enr_trend_1yr >= -0.05:
            return composite, False, None

        decline = abs(enr_direct_22_24)
        if   decline >= 0.50: severity_mult, severity_label = 0.60, 'severe'
        elif decline >= 0.35: severity_mult, severity_label = 0.45, 'moderate'
        else:                 severity_mult, severity_label = 0.30, 'mild'

        enr_score   = enrollment_domain_score if not pd.isna(enrollment_domain_score) else 40.0
        floor_score = 40.0 + max(0.0, enr_score - 40.0) * severity_mult
        adjusted    = max(floor_score, composite if not pd.isna(composite) else 0.0)
        floor_applied = adjusted > (composite if not pd.isna(composite) else 0.0) + 0.01

        return adjusted, floor_applied, severity_label

    # =========================================================================
    # v5 — REVENUE VELOCITY FLOOR (for confirmed subsidiaries only)
    # =========================================================================

    def _apply_revenue_floor_subsidiary(self, composite: float, uid: str,
                                         master_row) -> tuple:
        """
        Returns (adjusted_composite, floor_applied: bool).

        Applied ONLY to confirmed subsidiaries (is_subsidiary_ipeds = True).

        Revenue_2yr_pct captures what enrollment_score misses for subsidiaries:
        the subsidiary's revenue may be collapsing even while its 2022→2024
        enrollment looks stable (because the collapse was pre-2022 or offset by
        late enrollment data).

        Floor thresholds:
          rev_2yr < -20%:  floor = 45
          rev_2yr < -40%:  floor = 55
          rev_2yr < -60%:  floor = 65
        """
        if not self._subsidiary_flags.get(uid, False):
            return composite, False

        if master_row is None:
            return composite, False

        rev_2yr = pd.to_numeric(master_row.get('revenue_2yr_pct'), errors='coerce')
        if pd.isna(rev_2yr):
            return composite, False

        if   rev_2yr < -60: floor = 65
        elif rev_2yr < -40: floor = 55
        elif rev_2yr < -20: floor = 45
        else:
            return composite, False

        adjusted     = max(floor, composite if not pd.isna(composite) else 0.0)
        floor_applied = adjusted > (composite if not pd.isna(composite) else 0.0) + 0.01

        return adjusted, floor_applied

    # =========================================================================
    # SCORE AGGREGATION
    # =========================================================================

    def score_entity(self, uid: str, year: int, master_row=None) -> dict:
        """Compute full distress score for one institution in one year."""
        data = self.data.get(uid, {}).get(year, {})
        if not data:
            return {'unitid': uid, 'year': year, 'distress_score': np.nan,
                    'error': 'no_data'}

        is_sub = self._subsidiary_flags.get(uid, False)
        acct   = self.accounting_std.get(uid, 'unknown')

        enr_results = self.compute_enrollment(data, uid, year, master_row=master_row)

        domain_results = {
            'solvency':              self.compute_solvency(data, uid, master_row=master_row),
            'liquidity':             self.compute_liquidity(data, uid),
            'operating_performance': self.compute_operating(data, uid),
            'enrollment_health':     enr_results,
            'academic_outcomes':     self.compute_academic(data, uid),
            'demand':                self.compute_demand(data, uid),
            'trend':                 self.compute_trends(uid, year),
        }

        # Extract internal keys before aggregation
        cliff_mult        = domain_results['enrollment_health'].pop('_cliff_multiplier', 1.0)
        enr_direct_2224   = domain_results['enrollment_health'].pop('_enr_direct_22_24', np.nan)
        solvency_source   = domain_results['solvency'].pop('_solvency_source', 'equity_ratio')
        na_months_score   = domain_results['solvency'].pop('na_months_score', np.nan)
        na_months_raw_val = domain_results['solvency'].pop('na_months_raw', np.nan)
        enr_trend_1yr     = domain_results['enrollment_health'].get('enrollment_trend_1yr_raw', np.nan)
        total_enrollment  = domain_results['enrollment_health'].get('enrollment_size_raw', np.nan)

        # Aggregate within each domain
        domain_scores = {}
        for domain_name, domain_config in DISTRESS_DOMAINS.items():
            indicators   = domain_results.get(domain_name, {})
            weighted_sum = 0.0
            weight_sum   = 0.0
            for ind_name, ind_config in domain_config['indicators'].items():
                score = indicators.get(ind_name, np.nan)
                if not pd.isna(score):
                    w = ind_config['weight']
                    weighted_sum += score * w
                    weight_sum   += w
            if weight_sum > 0:
                raw_domain = weighted_sum / weight_sum * 100
                if domain_name == 'enrollment_health':
                    raw_domain = min(raw_domain * cliff_mult, 100.0)
                domain_scores[domain_name] = raw_domain
            else:
                # Subsidiary solvency: if all standard indicators are NaN but
                # na_months_score is available, use it directly as the domain score
                if domain_name == 'solvency' and is_sub and not pd.isna(na_months_score):
                    domain_scores[domain_name] = na_months_score
                else:
                    domain_scores[domain_name] = np.nan

        # Aggregate across domains
        total_weighted = 0.0
        total_weight   = 0.0
        for domain_name, domain_config in DISTRESS_DOMAINS.items():
            ds = domain_scores.get(domain_name, np.nan)
            if not pd.isna(ds):
                w = domain_config['weight']
                total_weighted += ds * w
                total_weight   += w

        composite = (total_weighted / total_weight) if total_weight > 0 else np.nan

        # Count indicators
        all_ind = {}
        for dr in domain_results.values():
            all_ind.update(dr)
        scored = sum(1 for k, v in all_ind.items()
                     if not k.endswith('_raw') and not k.startswith('_')
                     and not pd.isna(v))
        total_possible = sum(1 for k in all_ind
                             if not k.endswith('_raw') and not k.startswith('_'))

        MIN_INDICATORS = 4
        if scored < MIN_INDICATORS:
            composite = np.nan

        # v4: Enrollment velocity floor (non-subsidiaries)
        enr_domain_score = domain_scores.get('enrollment_health', np.nan)
        composite_enr_floored, enr_floor_applied, enr_floor_severity = \
            self._apply_enrollment_floor(
                composite               = composite,
                uid                     = uid,
                enrollment_domain_score = enr_domain_score,
                enr_direct_22_24        = enr_direct_2224,
                enr_trend_1yr           = enr_trend_1yr,
                total_enrollment        = total_enrollment,
            )

        # v5: Revenue velocity floor (subsidiaries only)
        composite_rev_floored, rev_floor_applied = \
            self._apply_revenue_floor_subsidiary(
                composite  = composite_enr_floored,
                uid        = uid,
                master_row = master_row,
            )

        final_composite = composite_rev_floored

        result = {
            'unitid':             uid,
            'year':               year,
            'accounting_standard': acct,
            'distress_score':     round(final_composite, 1) if not pd.isna(final_composite) else np.nan,
            'distress_score_prefloored': round(composite, 1) if not pd.isna(composite) else np.nan,
            'risk_category':      self._categorize(final_composite),
            'data_completeness':  round(scored / total_possible * 100, 0)
                                  if total_possible > 0 else 0,
            'indicators_scored':  scored,
            'indicators_total':   total_possible,
            'cliff_multiplier':   round(cliff_mult, 3),
            # v4
            'enrollment_velocity_floor': enr_floor_applied,
            'floor_severity':     enr_floor_severity,
            'enrollment_chg_direct_22_24': round(enr_direct_2224, 4)
                                           if not pd.isna(enr_direct_2224) else np.nan,
            # v5
            'is_subsidiary':      is_sub,
            'parent_unitid':      self._parent_uid.get(uid),
            'parent_name':        self._parent_name.get(uid),
            'solvency_source':    solvency_source,
            'na_months_expenses': round(na_months_raw_val, 2)
                                  if not pd.isna(na_months_raw_val) else np.nan,
            'revenue_velocity_floor': rev_floor_applied,
        }

        for dn in DISTRESS_DOMAINS:
            result[f'{dn}_score'] = round(domain_scores.get(dn, np.nan), 1)

        for dr in domain_results.values():
            for k, v in dr.items():
                if k.endswith('_raw'):
                    result[k] = round(v, 4) \
                        if (not pd.isna(v) and not isinstance(v, complex)) else np.nan

        return result

    def _categorize(self, score):
        if   pd.isna(score):  return 'Insufficient Data'
        elif score < 20:      return 'Healthy'
        elif score < 40:      return 'Low Risk'
        elif score < 60:      return 'Moderate Risk'
        elif score < 80:      return 'High Risk'
        else:                 return 'Severe Distress'

    def score_all(self, target_year: int = None) -> pd.DataFrame:
        results = []
        for uid in self.data:
            years = sorted(self.data[uid].keys())
            if not years:
                continue
            yr = target_year if (target_year and target_year in years) else years[-1]
            master_row = self._master_rows.get(uid)
            results.append(self.score_entity(uid, yr, master_row=master_row))
        df = pd.DataFrame(results)
        if len(df) > 0:
            print(f"\nScored {len(df)} institutions")
            print(f"Risk Distribution:")
            print(df['risk_category'].value_counts().to_string())
            print(f"\nSubsidiaries scored: {df['is_subsidiary'].sum()}")
            print(f"  → na_months solvency path: {(df['solvency_source']=='na_months').sum()}")
            print(f"  → revenue velocity floor fired: {df['revenue_velocity_floor'].sum()}")
            print(f"\nEnrollment velocity floor fired (non-subsidiaries): "
                  f"{df['enrollment_velocity_floor'].sum()}")
            print(f"  mild: {(df['floor_severity']=='mild').sum()}")
            print(f"  moderate: {(df['floor_severity']=='moderate').sum()}")
            print(f"  severe: {(df['floor_severity']=='severe').sum()}")
            print(f"\nAvg data completeness: {df['data_completeness'].mean():.0f}%")
        return df

    def score_all_years(self) -> pd.DataFrame:
        results = []
        for uid in self.data:
            for year in sorted(self.data[uid].keys()):
                master_row = self._master_rows.get(uid)
                results.append(self.score_entity(uid, year, master_row=master_row))
        return pd.DataFrame(results)

    # =========================================================================
    # MASTER INTEGRATION
    # =========================================================================

    def integrate_with_master(self, master_path: str, output_path: str = None,
                              target_year: int = 2024) -> pd.DataFrame:
        """
        Score all IPEDS institutions and merge into the Hummingbird Master.

        v5 changes vs v4:
          - detect_subsidiaries() is called first to populate EIN contamination
            registry before any scoring begins
          - compute_solvency() branches on is_subsidiary flag
          - Revenue velocity floor applied post-aggregation for subsidiaries
          - New output columns: is_subsidiary_ipeds, parent_unitid_ipeds,
            parent_name_ipeds, na_months_expenses_ipeds, solvency_source_ipeds,
            revenue_velocity_floor_ipeds
        """
        print(f"\n{'='*60}")
        print(f"INTEGRATING WITH HUMMINGBIRD MASTER  (v5)")
        print(f"{'='*60}")

        master = pd.read_csv(master_path, encoding='latin-1', low_memory=False)
        print(f"Master file: {len(master)} institutions")

        mask_ipeds = master['data_source'] == 'IPEDS'
        print(f"IPEDS institutions: {mask_ipeds.sum()}")

        master['unitid_clean'] = master['unitid'].apply(
            lambda x: str(int(x)).strip() if pd.notna(x) else None
        )

        # v5: Run subsidiary detection before scoring
        print("\nRunning EIN subsidiary detection...")
        self.detect_subsidiaries(master)

        # Build uid → master_row lookup
        flat_data = {}
        for _, row in master[mask_ipeds].iterrows():
            uid = row.get('unitid_clean')
            if uid:
                flat_data[uid]         = row
                self._master_rows[uid] = row

        # Sync IRS990 accounting standard from master
        for _, row in master[mask_ipeds].iterrows():
            uid  = row.get('unitid_clean')
            acct = str(row.get('accounting_standard_ipeds', '')).lower().strip()
            if uid and acct == 'irs990':
                self.accounting_std[uid] = 'irs990'

        # Initialise output columns
        bool_cols = ['likely_closed_ipeds', 'enrollment_velocity_floor_ipeds',
                     'is_subsidiary_ipeds', 'revenue_velocity_floor_ipeds']
        for col in bool_cols:
            if col not in master.columns:
                master[col] = False
        str_cols = ['floor_severity_ipeds', 'solvency_source_ipeds',
                    'parent_name_ipeds', 'parent_unitid_ipeds']
        for col in str_cols:
            if col not in master.columns:
                master[col] = np.nan
        for col in ['enrollment_chg_direct_ipeds', 'na_months_expenses_ipeds']:
            if col not in master.columns:
                master[col] = np.nan

        results         = []
        matched         = 0
        no_data         = 0
        injected        = 0
        closed_fallback = 0
        flagged_closed  = 0
        enr_floor_fired = 0
        rev_floor_fired = 0
        subsidiaries_scored = 0

        FALLBACK_YEARS = [target_year - 1, target_year - 2]

        for idx, row in master[mask_ipeds].iterrows():
            uid = row['unitid_clean']
            if uid is None or uid not in self.data:
                no_data += 1
                continue

            master_row = flat_data.get(uid)

            # Inject 990 fills
            if master_row is not None:
                before = sum(1 for yr in self.data[uid]
                             for v in self.data[uid][yr].values() if not pd.isna(v))
                self._inject_990_fills(uid, master_row, target_year)
                after = sum(1 for yr in self.data[uid]
                            for v in self.data[uid][yr].values() if not pd.isna(v))
                if after > before:
                    injected += 1

            # v4: tightened likely_closed check
            if self._is_likely_closed(uid, master_row, target_year):
                flagged_closed += 1
                master.at[idx, 'likely_closed_ipeds'] = True
                no_data += 1
                continue

            # Determine score year with fallback
            available  = sorted(self.data[uid].keys(), reverse=True)
            score_year = target_year if target_year in available else available[0]

            if not self._year_is_usable(uid, score_year):
                fallback_used = False
                for fb_yr in FALLBACK_YEARS:
                    if fb_yr in self.data[uid] and self._year_is_usable(uid, fb_yr):
                        score_year    = fb_yr
                        fallback_used = True
                        closed_fallback += 1
                        break
                if not fallback_used:
                    flagged_closed += 1
                    master.at[idx, 'likely_closed_ipeds'] = True
                    no_data += 1
                    continue

            result = self.score_entity(uid, score_year, master_row=master_row)
            result['master_idx'] = idx
            results.append(result)
            matched += 1

            if result.get('enrollment_velocity_floor'):
                enr_floor_fired += 1
            if result.get('revenue_velocity_floor'):
                rev_floor_fired += 1
            if result.get('is_subsidiary'):
                subsidiaries_scored += 1

            # Write subsidiary metadata directly to master
            if result.get('is_subsidiary'):
                master.at[idx, 'is_subsidiary_ipeds']   = True
                master.at[idx, 'parent_name_ipeds']      = result.get('parent_name')
                master.at[idx, 'parent_unitid_ipeds']    = result.get('parent_unitid')

        print(f"\nMatched and scored:               {matched}")
        print(f"No IPEDS data / unscoreable:      {no_data}")
        print(f"Enriched by 990 injection:        {injected}")
        print(f"Flagged likely_closed (v5):       {flagged_closed}  (was ~124 in v3)")
        print(f"  scored on prior year:           {closed_fallback}")
        print(f"Subsidiaries scored (v5 new):     {subsidiaries_scored}")
        print(f"  → na_months solvency path:      {subsidiaries_scored}")
        print(f"  → revenue velocity floor fired: {rev_floor_fired}")
        print(f"Enrollment velocity floor fired:  {enr_floor_fired}  (non-subsidiaries)")

        if not results:
            return master

        scores_df = pd.DataFrame(results)

        new_cols = {
            'distress_score_ipeds':                'distress_score',
            'distress_score_prefloored_ipeds':     'distress_score_prefloored',
            'distress_category_ipeds':             'risk_category',
            'accounting_standard_ipeds':           'accounting_standard',
            'solvency_score_ipeds':                'solvency_score',
            'liquidity_score_ipeds':               'liquidity_score',
            'operating_score_ipeds':               'operating_performance_score',
            'enrollment_score_ipeds':              'enrollment_health_score',
            'academic_score_ipeds':                'academic_outcomes_score',
            'demand_score_ipeds':                  'demand_score',
            'trend_score_ipeds':                   'trend_score',
            'data_completeness_ipeds':             'data_completeness',
            'score_year_ipeds':                    'year',
            'cliff_multiplier_ipeds':              'cliff_multiplier',
            'enrollment_velocity_floor_ipeds':     'enrollment_velocity_floor',
            'floor_severity_ipeds':                'floor_severity',
            'enrollment_chg_direct_ipeds':         'enrollment_chg_direct_22_24',
            # v5 new
            'solvency_source_ipeds':               'solvency_source',
            'na_months_expenses_ipeds':            'na_months_expenses',
            'revenue_velocity_floor_ipeds':        'revenue_velocity_floor',
            # Raw metrics
            'equity_ratio_raw_ipeds':              'equity_ratio_raw',
            'unrestricted_cushion_raw_ipeds':      'unrestricted_cushion_raw',
            'operating_margin_raw_ipeds':          'operating_margin_raw',
            'debt_ratio_raw_ipeds':                'debt_ratio_raw',
            'tuition_dependency_raw_ipeds':        'tuition_dependency_raw',
            'retention_rate_raw_ipeds':            'retention_rate_raw',
            'graduation_rate_raw_ipeds':           'graduation_rate_raw',
            'enrollment_trend_1yr_raw_ipeds':      'enrollment_trend_1yr_raw',
            'enrollment_trend_4yr_raw_ipeds':      'enrollment_trend_4yr_raw',
            'enrollment_chg_3yr_raw_ipeds':        'enrollment_chg_3yr_raw',
            'revenue_runway_raw_ipeds':            'revenue_runway_raw',
            'admissions_yield_raw_ipeds':          'admissions_yield_raw',
            'revenue_per_student_raw_ipeds':       'revenue_per_student_raw',
            'days_cash_raw_ipeds':                 'days_cash_raw',
            'net_asset_trend_raw_ipeds':           'net_asset_trend_raw',
        }

        for mc in new_cols:
            if mc not in master.columns:
                master[mc] = np.nan

        for _, score_row in scores_df.iterrows():
            idx = score_row['master_idx']
            for mc, sc in new_cols.items():
                if sc in score_row.index:
                    master.at[idx, mc] = score_row[sc]

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

        # Summary stats
        ipeds_scored = master.loc[mask_ipeds]
        active       = ipeds_scored[~ipeds_scored['likely_closed_ipeds'].fillna(False)]
        closed       = ipeds_scored[ipeds_scored['likely_closed_ipeds'].fillna(False)]
        subs         = ipeds_scored[ipeds_scored['is_subsidiary_ipeds'].fillna(False)]

        print(f"\n--- Updated Master (IPEDS active institutions) ---")
        print(active['distress_category'].value_counts().to_string())
        print(f"\nlikely_closed: {len(closed)} institutions")
        print(f"is_subsidiary: {len(subs)} institutions")
        print(f"\ndata_completeness mean (active): "
              f"{active['data_completeness_ipeds'].mean():.1f}%")
        print(f"enrollment_velocity_floor fired: "
              f"{ipeds_scored['enrollment_velocity_floor_ipeds'].sum()}")
        print(f"revenue_velocity_floor fired:    "
              f"{ipeds_scored['revenue_velocity_floor_ipeds'].sum()}")

        if output_path:
            master.to_csv(output_path, index=False)
            print(f"\nSaved to: {output_path}")

        return master


# =============================================================================
# CONFIGURATION  — update paths before running
# =============================================================================

IPEDS_FILES = {
    2020: 'hv_master_data/data/IPEDS/IPEDS20.csv',
    2021: 'hv_master_data/data/IPEDS/IPEDS21.csv',
    2022: 'hv_master_data/data/IPEDS/IPEDS22.csv',
    2023: 'hv_master_data/data/IPEDS/IPEDS23.csv',
    2024: 'hv_master_data/data/IPEDS/IPEDS24.csv',
}

MASTER_FILE        = 'hv_master_data/data/Hummingbird_Master_Combined_v5.csv'
OUTPUT_FILE        = 'hv_master_data/data/Hummingbird_Master_Combined_v6.csv'
SCORES_DETAIL_FILE = 'hv_master_data/data/ipeds_distress_scores_detail_v5.csv'


# =============================================================================
# RUN
# =============================================================================

if __name__ == '__main__':
    import os

    print("=" * 70)
    print("IPEDS DISTRESS SCORING — HUMMINGBIRD  (v5)")
    print("=" * 70)
    print("Changes vs v4:")
    print("  1. EIN-based parent-subsidiary contamination detection")
    print("     134 subsidiaries identified via EIN + asset matching")
    print("  2. Branched solvency: subsidiaries use months-of-reserve,")
    print("     independents unchanged (equity_ratio path)")
    print("  3. Revenue velocity floor for subsidiaries:")
    print("     rev_2yr < -20% → floor 45 | < -40% → 55 | < -60% → 65")
    print("  4. All v4 features retained (enrollment velocity floor, etc.)")
    print("=" * 70)
    print("NOTE: Run ipeds_crossfill_v2.py first.")
    print("=" * 70)

    master      = pd.read_csv(MASTER_FILE, encoding='latin-1', low_memory=False)
    ipeds_mask  = master['data_source'] == 'IPEDS'
    target_unitids = set(
        str(int(x)) for x in master.loc[ipeds_mask, 'unitid'].dropna()
    )
    print(f"\nTarget UNITIDs from master: {len(target_unitids)}")

    engine = DistressIPEDSEngine()

    available_files = {yr: p for yr, p in IPEDS_FILES.items() if os.path.exists(p)}
    print(f"IPEDS files found: {len(available_files)} / {len(IPEDS_FILES)}")

    if not available_files:
        print("\n⚠  No IPEDS files found — update paths in CONFIGURATION.")
        exit(1)

    engine.load_data(file_paths=available_files, filter_unitids=target_unitids)

    updated_master = engine.integrate_with_master(
        master_path  = MASTER_FILE,
        output_path  = OUTPUT_FILE,
        target_year  = 2024,
    )

    all_scores = engine.score_all_years()
    all_scores.to_csv(SCORES_DETAIL_FILE, index=False)
    print(f"\nYear-by-year detail saved: {SCORES_DETAIL_FILE}")

    print("\n" + "=" * 70)
    print("DONE — output: " + OUTPUT_FILE)
    print("=" * 70)