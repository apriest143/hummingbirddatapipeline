"""
================================================================================
990 Financial Distress Scoring Engine — Hummingbird Project
================================================================================

This script calculates enhanced financial distress scores for non-IPEDS
institutions (religious organizations, wellness/retreat centers, tribal centers,
vocational programs) using IRS 990 filing data across 5 years (2020-2024).

Scores are merged into the Hummingbird Master Distress file. This engine runs
FIRST, producing the "Enhanced" master. The IPEDS engine then runs on top of it.

Goal: Identify institutions across the full spectrum of financial stress —
not just imminent closures, but organizations that may be years from crisis
and open to land asset partnerships or revitalization efforts.


================================================================================
PREVIOUS MODEL (7 Binary Flags)
================================================================================

The original Hummingbird 990 distress model used 7 binary indicators:

| # | Indicator                | Threshold               | Weight |
|---|--------------------------|-------------------------|--------|
| 1 | Operating Losses         | Net income < 0          | 15     |
| 2 | Consecutive Losses       | Losses for 2+ years     | 20     |
| 3 | Negative Net Assets      | Net assets < $0         | 20     |
| 4 | Low Equity Ratio         | <20%                    | 10     |
| 5 | Revenue Decline (1yr)    | >15% drop YoY           | 15     |
| 6 | Revenue Decline (2yr)    | >25% drop over 2 years  | 10     |
| 7 | High Debt Ratio          | >70%                    | 10     |

Limitations:
  - Binary flags miss the spectrum (equity ratio of 19% and -50% score the same)
  - Scores cluster at 0 (no flags) — 69.6% scored "Healthy" with median of 0
  - No visibility into HOW distressed — just whether a threshold was crossed
  - No liquidity metrics (an org can be "solvent" but unable to pay next month's bills)
  - No revenue diversification signal (single-donor dependency is invisible)
  - No red flags for insider activity, ceased operations, or asset liquidation


================================================================================
NEW MODEL (19 Continuous Indicators, 5 Domains)
================================================================================

Each indicator converts its raw value to a 0-1 distress score using calibrated
healthy and distress thresholds, with linear interpolation between them.
Indicators aggregate within domains (weighted average), then domains aggregate
to a 0-100 composite score.

DOMAIN 1: SOLVENCY (Weight: 30%)
Can the organization pay its debts and survive a bad year?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source      | Notes                              |
|------------------------|------------|------------|-------------|-------------------------------------|
| Equity ratio           | ≥ 0.40     | ≤ -0.10    | All types   | Net assets / total assets           |
| Unrestricted cushion   | ≥ 0.25     | ≤ 0.00     | Std only    | Unrestricted NA / annual expenses   |
| Debt ratio             | ≤ 0.50     | ≥ 1.00     | All types   | Liabilities / assets (inverted)     |
| Debt to fixed assets   | ≤ 0.60     | ≥ 1.50     | Std only    | (Secured + unsecured) / property    |

Why 30%: Solvency is the foundation. A church with negative net assets is
technically insolvent even if it's still operating. This is the strongest
long-term predictor of organizational failure.

DOMAIN 2: LIQUIDITY (Weight: 20%)
Can the organization meet short-term obligations?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source      | Notes                              |
|------------------------|------------|------------|-------------|-------------------------------------|
| Days cash on hand      | ≥ 90 days  | ≤ 15 days  | All types   | (Cash + savings) / daily expenses   |
| Liquid ratio           | ≥ 1.50     | ≤ 0.50     | Std only    | Liquid assets / short-term liab     |
| Deferred revenue risk  | ≤ 0.15     | ≥ 0.50     | Std only    | Deferred revenue / total revenue    |

Why 20%: An organization can be technically solvent (positive net assets) but
unable to pay next month's bills. Churches with <15 days of cash are in acute
stress regardless of their balance sheet.

DOMAIN 3: OPERATING PERFORMANCE (Weight: 25%)
Is the organization running surpluses or deficits?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source      | Notes                              |
|------------------------|------------|------------|-------------|-------------------------------------|
| Operating margin       | ≥ 5%       | ≤ -20%     | All types   | (Revenue - Expenses) / Revenue      |
| Program revenue ratio  | 10-90%     | Extremes   | Std/EZ      | Program rev / total (diversity)     |
| Revenue concentration  | ≤ 0.50 HHI | ≥ 0.90 HHI| Std only   | Herfindahl index of revenue sources |
| Compensation burden    | 30-65%     | >85% or <10%| Std only   | Total comp / total expenses         |

Why 25%: Persistent deficits erode reserves. But the continuous scoring
matters here — a -2% margin is "watch carefully" while -20% is "crisis."
Revenue concentration catches single-donor dependency, which is invisible
in binary flag models.

DOMAIN 4: TREND / TRAJECTORY (Weight: 20%)
Is the organization getting better or worse over 5 years?
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Healthy    | Distress   | Source      | Notes                              |
|------------------------|------------|------------|-------------|-------------------------------------|
| Revenue trend          | ≥ 0%       | ≤ -15%     | Multi-year  | Annualized revenue growth rate      |
| Net asset trend        | ≥ 0%       | ≤ -10%     | Multi-year  | Annualized net asset growth rate    |
| Expense growth gap     | ≤ 0%       | ≥ 10%      | Multi-year  | Expense growth minus revenue growth |
| Employee trend         | ≥ -2%      | ≤ -20%     | Multi-year  | Annualized employee count change    |

Why 20%: Direction of travel matters as much as current position. A church
running thin margins but with growing revenue is fundamentally different from
one running thin margins with accelerating decline. Requires 2+ years of data.

DOMAIN 5: RED FLAGS (Weight: 5%)
Binary warning signals for acute issues.
─────────────────────────────────────────────────────────────────────────────────
| Indicator              | Flag Condition              | Source      | Notes                    |
|------------------------|-----------------------------|-------------|--------------------------|
| Ceased operations      | Organization reported cease | Std only    | Immediate closure signal |
| Insider loans          | Payable/receivable to officers| Std only  | Governance concern       |
| Fundraising efficiency | Fees > 50% of contributions | Std only    | Unsustainable model      |
| Asset liquidation      | Sold assets flag set        | Std only    | Possible wind-down       |

Why 5%: These are rare but severe. A ceased operations flag is an immediate
signal. Low weight because most organizations never trigger these.


================================================================================
990 FILING TYPE COVERAGE
================================================================================

Different organizations file different 990 forms based on their size. Each
form provides different levels of financial detail:

| Filing Type   | Who Files                           | Indicators | Coverage |
|---------------|-------------------------------------|------------|----------|
| 990 Standard  | Gross receipts >$200K or assets >$500K | 19/19    | 100%     |
| 990-EZ        | Gross receipts <$200K, assets <$500K   |  8/19    | 42%      |
| 990-PF        | Private foundations                     |  6/19    | 32%      |

When indicators cannot be computed (e.g., 990-EZ lacks unrestricted net asset
split), domain weights are automatically renormalized across available
indicators. This means a 990-EZ filer still gets a valid score — it's just
based on fewer data points, which is reflected in the data_completeness field.

Distribution in Hummingbird dataset:
  - 990 Standard: 8,703 institutions (98.5%)
  - 990-EZ: 105 institutions (1.2%)
  - 990-PF: 32 institutions (0.4%)


================================================================================
990 ↔ IPEDS METRIC CROSSWALK
================================================================================

| 990 Metric              | IPEDS Equivalent              | Match Type |
|-------------------------|-------------------------------|------------|
| Equity ratio            | Equity ratio (FASB/GASB)      | Direct     |
| Unrestricted cushion    | Unrestricted cushion          | Direct     |
| Days cash on hand       | Days cash on hand             | Direct     |
| Operating margin        | Operating margin              | Direct     |
| Debt ratio              | Debt ratio                    | Direct     |
| Net asset trend         | Net asset trend               | Direct     |
| Employee trend          | Staff trend                   | Direct     |
| Revenue trend           | Revenue trend                 | Direct     |
| Program revenue ratio   | Tuition dependency            | Proxy      |
| Revenue concentration   | (no equivalent)               | 990 only   |
| Fundraising efficiency  | (no equivalent)               | 990 only   |
| Deferred revenue risk   | (no equivalent)               | 990 only   |
| (no equivalent)         | Retention rate                | IPEDS only |
| (no equivalent)         | Graduation rate               | IPEDS only |
| (no equivalent)         | Enrollment trend              | IPEDS only |
| (no equivalent)         | Admissions yield              | IPEDS only |


================================================================================
SCORING
================================================================================

  0-20  = Healthy
  20-40 = Low Risk (may have structural vulnerabilities worth monitoring)
  40-60 = Moderate Risk (multiple stress signals, potential partnership prospect)
  60-80 = High Risk (significant distress across multiple domains)
  80-100 = Severe Distress (likely approaching closure or major restructuring)


================================================================================
OUTPUT COLUMNS ADDED TO MASTER
================================================================================

  distress_score_990           - Enhanced 0-100 composite score
  distress_category_990        - Healthy / Low Risk / Moderate / High / Severe
  solvency_score_990           - Domain score (0-100)
  liquidity_score_990          - Domain score (0-100)
  operating_score_990          - Domain score (0-100)
  trend_score_990              - Domain score (0-100)
  red_flag_score_990           - Domain score (0-100)
  data_completeness_990        - % of 19 indicators that could be computed
  filing_type_990              - standard / ez / pf
  score_year_990               - Which filing year was scored
  + 9 raw metric columns (*_raw_990) for transparency

The main distress_score and distress_category columns are also updated in-place.


================================================================================
USAGE
================================================================================

  1. Update file paths in CONFIGURATION section at bottom
  2. Run: python distress_990.py

  Inputs:  15 IRS 990 CSVs (5 years × 3 types) + Hummingbird_Master_Distress.csv
  Outputs: Hummingbird_Master_Distress_Enhanced.csv (updated master)
           990_distress_scores_detail.csv (year-by-year scores for analysis)

  Pipeline order: Run this FIRST, then run distress_ipeds.py on the Enhanced output.
================================================================================
"""


import pandas as pd
import numpy as np
from typing import Optional, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')


# =============================================================================
# VARIABLE MAPPINGS: Raw 990 column names → Standardized metric names
# =============================================================================

# Each filing type maps its own column names to a common set of standardized names.
# This allows us to compute the same distress metrics regardless of filing type.

STANDARD_990_MAP = {
    # --- Identifiers ---
    'EIN': 'ein',
    'tax_pd': 'tax_period',
    
    # --- Revenue ---
    'totrevenue': 'total_revenue',
    'totprgmrevnue': 'program_revenue',
    'totcntrbgfts': 'contributions',
    'invstmntinc': 'investment_income',
    'netincfndrsng': 'net_fundraising_income',
    'netrntlinc': 'net_rental_income',
    'netgnls': 'net_gains_securities',
    'netincsales': 'net_inventory_sales',
    'grsincgaming': 'gross_gaming_income',
    
    # --- Expenses ---
    'totfuncexpns': 'total_expenses',
    'compnsatncurrofcr': 'officer_compensation',
    'compnsatnandothr': 'other_compensation',
    'othrsalwages': 'other_salaries',
    'pensionplancontrb': 'pension_contributions',
    'othremplyeebenef': 'other_employee_benefits',
    'payrolltx': 'payroll_tax',
    'profndraising': 'fundraising_fees',
    'feesforsrvcmgmt': 'management_fees',
    'legalfees': 'legal_fees',
    'accntingfees': 'accounting_fees',
    'feesforsrvclobby': 'lobbying_fees',
    'feesforsrvcinvstmgmt': 'investment_mgmt_fees',
    'feesforsrvcothr': 'other_service_fees',
    'advrtpromo': 'advertising',
    'occupancy': 'occupancy',
    'travel': 'travel',
    'deprcatndepletn': 'depreciation',
    'insurance': 'insurance',
    'interestamt': 'interest_expense',
    'pymtoaffiliates': 'payments_to_affiliates',
    'grntstogovt': 'grants_to_govt',
    'grnsttoindiv': 'grants_to_individuals',
    'grntstofrgngovt': 'grants_to_foreign',
    
    # --- Balance Sheet ---
    'totassetsend': 'total_assets',
    'totliabend': 'total_liabilities',
    'totnetassetend': 'total_net_assets',
    'unrstrctnetasstsend': 'unrestricted_net_assets',
    'temprstrctnetasstsend': 'temp_restricted_net_assets',
    'permrstrctnetasstsend': 'perm_restricted_net_assets',
    'nonintcashend': 'cash',
    'svngstempinvend': 'savings_temp_investments',
    'accntsrcvblend': 'accounts_receivable',
    'pldgegrntrcvblend': 'pledges_receivable',
    'currfrmrcvblend': 'current_receivables_from_officers',
    'invntriesalesend': 'inventory',
    'prepaidexpnsend': 'prepaid_expenses',
    'lndbldgsequipend': 'land_buildings_equipment',
    'invstmntsend': 'investments_securities',
    'invstmntsothrend': 'investments_other',
    'invstmntsprgmend': 'investments_program',
    'intangibleassetsend': 'intangible_assets',
    'othrassetsend': 'other_assets',
    'accntspayableend': 'accounts_payable',
    'grntspayableend': 'grants_payable',
    'deferedrevnuend': 'deferred_revenue',
    'txexmptbndsend': 'tax_exempt_bonds',
    'secrdmrtgsend': 'secured_mortgages',
    'unsecurednotesend': 'unsecured_notes',
    'paybletoffcrsend': 'payable_to_officers',
    'othrliabend': 'other_liabilities',
    
    # --- Operational Flags ---
    'noemplyeesw3cnt': 'employee_count',
    'ceaseoperationscd': 'ceased_operations',
    'sellorexchcd': 'sold_assets',
    'ownsepentcd': 'owns_separate_entity',
    'reltdorgcd': 'related_organization',
    'operateschools170cd': 'operates_schools',
    'operatehosptlcd': 'operates_hospital',
    'subseccd': 'subsection_code',
    'fw2gcnt': 'w2_count',
    'noindiv100kcnt': 'individuals_over_100k',
    'nocontractor100kcnt': 'contractors_over_100k',
}

EZ_990_MAP = {
    # --- Identifiers ---
    'EIN': 'ein',
    'taxpd': 'tax_period',
    
    # --- Revenue ---
    'totrevnue': 'total_revenue',
    'prgmservrev': 'program_revenue',
    'totcntrbs': 'contributions',
    'othrinvstinc': 'investment_income',
    'grsincgaming': 'gross_gaming_income',
    'grsrevnuefndrsng': 'gross_fundraising_revenue',
    'direxpns': 'direct_fundraising_expenses',
    'netincfndrsng': 'net_fundraising_income',
    'grsamtsalesastothr': 'gross_asset_sales',
    'gnsaleofastothr': 'gain_on_asset_sales',
    'duesassesmnts': 'dues_assessments',
    'othrevnue': 'other_revenue',
    
    # --- Expenses ---
    'totexpns': 'total_expenses',
    'totexcessyr': 'surplus_deficit',  # Revenue minus expenses
    
    # --- Balance Sheet ---
    'totassetsend': 'total_assets',
    'totliabend': 'total_liabilities',
    'totnetassetsend': 'total_net_assets',
    'networthend': 'net_worth',
    
    # --- Flags ---
    'contractioncd': 'ceased_operations',
    'unrelbusincd': 'unrelated_business',
    'subseccd': 'subsection_code',
    'loanstoofficerscd': 'loans_to_officers_flag',
    'loanstoofficers': 'loans_to_officers_amount',
    'politicalexpend': 'political_expenditures',
}

PF_990_MAP = {
    # --- Identifiers ---
    'EIN': 'ein',
    'TAX_PRD': 'tax_period',
    
    # --- Revenue ---
    'TOTRCPTPERBKS': 'total_revenue',
    'GRSCONTRGIFTS': 'contributions',
    'INTRSTRVNUE': 'interest_income',
    'DIVIDNDSAMT': 'dividend_income',
    'GRSRENTS': 'gross_rents',
    'GRSSLSPRAMT': 'gross_sales',
    'COSTSOLD': 'cost_of_goods_sold',
    'GRSPROFITBUS': 'gross_profit_business',
    'OTHERINCAMT': 'other_income',
    'NETINVSTINC': 'net_investment_income',
    
    # --- Expenses ---
    'TOTEXPNSPBKS': 'total_expenses',
    'COMPOFFICERS': 'officer_compensation',
    'PENSPLEMPLBENF': 'pension_benefits',
    'LEGALFEESAMT': 'legal_fees',
    'ACCOUNTINGFEES': 'accounting_fees',
    'INTERESTAMT': 'interest_expense',
    'DEPRECIATIONAMT': 'depreciation',
    'OCCUPANCYAMT': 'occupancy',
    'TRAVLCONFMTNGS': 'travel',
    'CONTRPDPBKS': 'contributions_paid',
    
    # --- Balance Sheet ---
    'TOTASSETSEND': 'total_assets',
    'TOTLIABEND': 'total_liabilities',
    'TFUNDNWORTH': 'total_net_assets',
    'FAIRMRKTVALEOY': 'fair_market_value',
    'OTHRCASHAMT': 'cash',
    'INVSTGOVTOBLIG': 'govt_obligations',
    'INVSTCORPSTK': 'corp_stock',
    'INVSTCORPBND': 'corp_bonds',
    'TOTINVSTSEC': 'total_investments_securities',
    'MRTGLOANS': 'mortgage_loans',
    'OTHRINVSTEND': 'other_investments',
    'OTHRASSETSEOY': 'other_assets',
    'MRTGNOTESPAY': 'mortgage_notes_payable',
    'OTHRLIABLTSEOY': 'other_liabilities',
    
    # --- Flags ---
    'OPERATINGCD': 'is_operating',
    'CONTRACTNCD': 'ceased_operations',
    'SUBCD': 'subsection_code',
    'EOSTATUS': 'eo_status',
}


# =============================================================================
# DISTRESS INDICATOR DEFINITIONS
# =============================================================================

# Each indicator is computed from standardized field names and produces a 
# continuous score from 0 (healthy) to 1 (severe distress).
# Indicators are grouped into domains with domain-level weights.

DISTRESS_INDICATORS = {
    # =========================================================================
    # DOMAIN 1: SOLVENCY (Weight: 0.30)
    # Can the organization pay its debts? Is it technically insolvent?
    # =========================================================================
    'solvency': {
        'weight': 0.30,
        'indicators': {
            # Net assets relative to total assets (equity ratio)
            # Negative = technically insolvent, <0.10 = danger zone
            'equity_ratio': {
                'weight': 0.35,
                'fields': ['total_net_assets', 'total_assets'],
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Unrestricted net assets relative to expenses
            # Negative = can't fund operations from own resources
            'unrestricted_cushion': {
                'weight': 0.30,
                'fields': ['unrestricted_net_assets', 'total_expenses'],
                'available_in': ['standard'],  # Only 990 standard has this
            },
            # Debt burden: total liabilities relative to assets
            'debt_ratio': {
                'weight': 0.20,
                'fields': ['total_liabilities', 'total_assets'],
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Secured + unsecured debt relative to fixed assets
            'debt_to_fixed_assets': {
                'weight': 0.15,
                'fields': ['secured_mortgages', 'unsecured_notes', 'land_buildings_equipment'],
                'available_in': ['standard'],
            },
        }
    },
    
    # =========================================================================
    # DOMAIN 2: LIQUIDITY (Weight: 0.20)
    # Can the organization meet short-term obligations?
    # =========================================================================
    'liquidity': {
        'weight': 0.20,
        'indicators': {
            # Days cash on hand: liquid assets / (daily expenses)
            'days_cash': {
                'weight': 0.40,
                'fields': ['cash', 'savings_temp_investments', 'total_expenses'],
                'available_in': ['standard', 'pf'],
            },
            # Current-ish ratio: liquid assets vs short-term liabilities
            'liquid_ratio': {
                'weight': 0.35,
                'fields': ['cash', 'savings_temp_investments', 'accounts_receivable',
                           'accounts_payable', 'deferred_revenue'],
                'available_in': ['standard'],
            },
            # Deferred revenue as % of total revenue (over-reliance on prepayments)
            'deferred_revenue_risk': {
                'weight': 0.25,
                'fields': ['deferred_revenue', 'total_revenue'],
                'available_in': ['standard'],
            },
        }
    },
    
    # =========================================================================
    # DOMAIN 3: OPERATING PERFORMANCE (Weight: 0.25)
    # Is the organization running at a surplus or deficit?
    # =========================================================================
    'operating_performance': {
        'weight': 0.25,
        'indicators': {
            # Operating margin: (revenue - expenses) / revenue
            'operating_margin': {
                'weight': 0.40,
                'fields': ['total_revenue', 'total_expenses'],
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Program revenue as % of total (self-sustaining vs donation-dependent)
            'program_revenue_ratio': {
                'weight': 0.25,
                'fields': ['program_revenue', 'total_revenue'],
                'available_in': ['standard', 'ez'],
            },
            # Revenue concentration: how dependent on a single source?
            'revenue_concentration': {
                'weight': 0.20,
                'fields': ['contributions', 'program_revenue', 'investment_income', 'total_revenue'],
                'available_in': ['standard', 'ez'],
            },
            # Compensation burden: total comp / total expenses
            'compensation_burden': {
                'weight': 0.15,
                'fields': ['officer_compensation', 'other_salaries', 'pension_contributions',
                           'other_employee_benefits', 'payroll_tax', 'total_expenses'],
                'available_in': ['standard'],
            },
        }
    },
    
    # =========================================================================
    # DOMAIN 4: TREND / TRAJECTORY (Weight: 0.20)
    # Is the organization getting better or worse over time?
    # Requires multi-year data (computed at scoring time)
    # =========================================================================
    'trend': {
        'weight': 0.20,
        'indicators': {
            # Revenue trajectory (year-over-year change)
            'revenue_trend': {
                'weight': 0.30,
                'fields': ['total_revenue'],  # Needs multi-year
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Net asset trajectory 
            'net_asset_trend': {
                'weight': 0.30,
                'fields': ['total_net_assets'],  # Needs multi-year
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Expense growth vs revenue growth (are costs outpacing income?)
            'expense_growth_gap': {
                'weight': 0.20,
                'fields': ['total_revenue', 'total_expenses'],  # Needs multi-year
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Employee count trend (shrinking workforce)
            'employee_trend': {
                'weight': 0.20,
                'fields': ['employee_count'],  # Needs multi-year
                'available_in': ['standard'],
            },
        }
    },
    
    # =========================================================================
    # DOMAIN 5: RED FLAGS (Weight: 0.05)
    # Binary warning signals
    # =========================================================================
    'red_flags': {
        'weight': 0.05,
        'indicators': {
            # Ceased operations flag
            'ceased_operations': {
                'weight': 0.30,
                'fields': ['ceased_operations'],
                'available_in': ['standard', 'ez', 'pf'],
            },
            # Loans to officers (governance concern)
            'insider_loans': {
                'weight': 0.20,
                'fields': ['payable_to_officers', 'current_receivables_from_officers'],
                'available_in': ['standard'],
            },
            # Fundraising efficiency (spending too much to raise too little)
            'fundraising_efficiency': {
                'weight': 0.25,
                'fields': ['fundraising_fees', 'contributions'],
                'available_in': ['standard'],
            },
            # Asset liquidation signals
            'asset_liquidation': {
                'weight': 0.25,
                'fields': ['sold_assets'],
                'available_in': ['standard'],
            },
        }
    },
}


class Distress990Engine:
    """
    Financial distress scoring engine for IRS 990 filers.
    
    Produces a 0-100 distress score where:
        0-20  = Healthy
        20-40 = Low Risk  
        40-60 = Moderate Risk
        60-80 = High Risk
        80-100 = Severe Distress / Likely Closure
    """
    
    def __init__(self):
        self.data = {}          # {ein: {year: {standardized fields}}}
        self.filing_types = {}  # {ein: 'standard'|'ez'|'pf'}
        self.scores = {}        # {ein: {year: score_dict}}
        
    # =========================================================================
    # DATA LOADING
    # =========================================================================
    
    def load_data(self, 
                  standard_paths=None,
                  ez_paths=None,
                  pf_paths=None,
                  filter_eins: Optional[set] = None):
        """
        Load 990 data from CSV files and standardize column names.
        
        Accepts either a single path (string) or a list of paths per filing type.
        This supports multi-year loading: pass 5 annual standard files, 5 EZ files, etc.
        
        Args:
            standard_paths: Path or list of paths to 990 standard CSVs
            ez_paths: Path or list of paths to 990-EZ CSVs
            pf_paths: Path or list of paths to 990-PF CSVs  
            filter_eins: Optional set of EINs to filter to (e.g., your master list)
        """
        # Normalize to lists
        def _to_list(x):
            if x is None:
                return []
            if isinstance(x, str):
                return [x]
            return list(x)
        
        for path in _to_list(standard_paths):
            self._load_filing_type(path, STANDARD_990_MAP, 'standard', filter_eins)
            
        for path in _to_list(ez_paths):
            self._load_filing_type(path, EZ_990_MAP, 'ez', filter_eins)
            
        for path in _to_list(pf_paths):
            self._load_filing_type(path, PF_990_MAP, 'pf', filter_eins)
            
        print(f"\nLoaded data for {len(self.data)} unique EINs")
        filing_counts = {}
        for ft in self.filing_types.values():
            filing_counts[ft] = filing_counts.get(ft, 0) + 1
        print(f"Filing types: {filing_counts}")
        
        # Show year coverage
        all_years = set()
        for ein_data in self.data.values():
            all_years.update(ein_data.keys())
        print(f"Years covered: {sorted(all_years)}")
        
        # Show how many EINs have multi-year data
        multi_year = sum(1 for d in self.data.values() if len(d) > 1)
        print(f"EINs with multi-year data (enables trend scoring): {multi_year}/{len(self.data)}")
        
    def _load_filing_type(self, path: str, column_map: dict, filing_type: str, 
                          filter_eins: Optional[set] = None):
        """Load a single filing type and standardize its columns."""
        print(f"Loading {filing_type} 990 from {path}...")
        
        # Read only the columns we need
        available_cols = pd.read_csv(path, nrows=0, encoding='latin-1').columns.tolist()
        cols_to_read = [c for c in column_map.keys() if c in available_cols]
        
        # Also always read EIN
        ein_col = [c for c in available_cols if c.upper() == 'EIN'][0]
        if ein_col not in cols_to_read:
            cols_to_read.insert(0, ein_col)
            
        df = pd.read_csv(path, usecols=cols_to_read, encoding='latin-1', low_memory=False)
        
        # Standardize column names
        rename_map = {k: v for k, v in column_map.items() if k in df.columns}
        df = df.rename(columns=rename_map)
        
        # Clean EIN: strip leading zeros, convert to string for matching
        df['ein'] = df['ein'].astype(str).str.strip().str.lstrip('0')
        
        # Extract year from tax period (format: YYYYMM)
        if 'tax_period' in df.columns:
            df['tax_period'] = pd.to_numeric(df['tax_period'], errors='coerce')
            df['filing_year'] = (df['tax_period'] // 100).astype('Int64')
        
        # Filter to target EINs if provided
        if filter_eins:
            filter_eins_clean = {str(e).strip().lstrip('0') for e in filter_eins}
            df = df[df['ein'].isin(filter_eins_clean)]
        
        # Convert numeric fields
        for col in df.columns:
            if col not in ['ein', 'filing_year', 'tax_period', 'ceased_operations',
                           'sold_assets', 'owns_separate_entity', 'related_organization',
                           'operates_schools', 'operates_hospital', 'subsection_code',
                           'is_operating', 'eo_status', 'unrelated_business',
                           'loans_to_officers_flag']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Store by EIN and year
        for _, row in df.iterrows():
            ein = row['ein']
            year = row.get('filing_year', None)
            if pd.isna(year):
                continue
            year = int(year)
            
            if ein not in self.data:
                self.data[ein] = {}
                self.filing_types[ein] = filing_type
                
            self.data[ein][year] = row.to_dict()
            
            # If this is a richer filing type, upgrade
            if filing_type == 'standard' and self.filing_types.get(ein) != 'standard':
                self.filing_types[ein] = 'standard'
        
        print(f"  → Loaded {len(df)} filings for {df['ein'].nunique()} EINs")
    
    # =========================================================================
    # INDICATOR COMPUTATION
    # =========================================================================
    
    def _safe_divide(self, numerator, denominator, default=np.nan):
        """Safe division avoiding divide-by-zero."""
        if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
            return default
        return numerator / denominator
    
    def _safe_get(self, data: dict, field: str, default=np.nan):
        """Safely get a numeric value from a data dict."""
        val = data.get(field, default)
        if pd.isna(val):
            return default
        return float(val)
    
    def _score_to_distress(self, value: float, healthy_threshold: float, 
                           distress_threshold: float, invert: bool = False) -> float:
        """
        Convert a raw metric value to a 0-1 distress score.
        
        For normal metrics (higher = healthier, like equity ratio):
            - Above healthy_threshold → 0 (no distress)
            - Below distress_threshold → 1 (max distress)
            - Between → linear interpolation
            
        For inverted metrics (higher = worse, like debt ratio):
            - Set invert=True
            - Below healthy_threshold → 0
            - Above distress_threshold → 1
        """
        if pd.isna(value) or isinstance(value, complex):
            return np.nan
            
        if invert:
            # Higher is worse
            if value <= healthy_threshold:
                return 0.0
            elif value >= distress_threshold:
                return 1.0
            else:
                return (value - healthy_threshold) / (distress_threshold - healthy_threshold)
        else:
            # Higher is healthier
            if value >= healthy_threshold:
                return 0.0
            elif value <= distress_threshold:
                return 1.0
            else:
                return (healthy_threshold - value) / (healthy_threshold - distress_threshold)
    
    def compute_solvency(self, data: dict, filing_type: str) -> Dict[str, float]:
        """Compute solvency indicators."""
        results = {}
        
        # Equity ratio: net assets / total assets
        total_assets = self._safe_get(data, 'total_assets')
        total_net_assets = self._safe_get(data, 'total_net_assets')
        equity_ratio = self._safe_divide(total_net_assets, total_assets)
        # Healthy: >0.40, Distress: <0.0 (negative equity)
        results['equity_ratio'] = self._score_to_distress(equity_ratio, 0.40, -0.10)
        results['equity_ratio_raw'] = equity_ratio
        
        # Unrestricted cushion: unrestricted net assets / annual expenses
        if filing_type == 'standard':
            unrestricted = self._safe_get(data, 'unrestricted_net_assets')
            expenses = self._safe_get(data, 'total_expenses')
            cushion = self._safe_divide(unrestricted, expenses)
            # Healthy: >0.25 (3 months), Distress: <0 (negative)
            results['unrestricted_cushion'] = self._score_to_distress(cushion, 0.25, -0.10)
            results['unrestricted_cushion_raw'] = cushion
        else:
            results['unrestricted_cushion'] = np.nan
            results['unrestricted_cushion_raw'] = np.nan
        
        # Debt ratio: liabilities / assets
        total_liabilities = self._safe_get(data, 'total_liabilities')
        debt_ratio = self._safe_divide(total_liabilities, total_assets)
        # Healthy: <0.50, Distress: >1.0 (insolvent)
        results['debt_ratio'] = self._score_to_distress(debt_ratio, 0.50, 1.0, invert=True)
        results['debt_ratio_raw'] = debt_ratio
        
        # Debt to fixed assets
        if filing_type == 'standard':
            secured = self._safe_get(data, 'secured_mortgages', 0)
            unsecured = self._safe_get(data, 'unsecured_notes', 0)
            fixed = self._safe_get(data, 'land_buildings_equipment')
            if not pd.isna(secured) and not pd.isna(unsecured):
                total_debt = secured + unsecured
                d2fa = self._safe_divide(total_debt, fixed) if fixed and fixed > 0 else (2.0 if total_debt > 0 else 0.0)
                results['debt_to_fixed_assets'] = self._score_to_distress(d2fa, 0.60, 1.50, invert=True)
                results['debt_to_fixed_assets_raw'] = d2fa
            else:
                results['debt_to_fixed_assets'] = np.nan
                results['debt_to_fixed_assets_raw'] = np.nan
        else:
            results['debt_to_fixed_assets'] = np.nan
            results['debt_to_fixed_assets_raw'] = np.nan
        
        return results
    
    def compute_liquidity(self, data: dict, filing_type: str) -> Dict[str, float]:
        """Compute liquidity indicators."""
        results = {}
        
        # Days cash on hand
        cash = self._safe_get(data, 'cash', 0)
        savings = self._safe_get(data, 'savings_temp_investments', 0)
        expenses = self._safe_get(data, 'total_expenses')
        
        if not pd.isna(cash) and not pd.isna(expenses) and expenses > 0:
            liquid = cash + (savings if not pd.isna(savings) else 0)
            days_cash = (liquid / expenses) * 365
            # Healthy: >90 days, Distress: <15 days
            results['days_cash'] = self._score_to_distress(days_cash, 90, 15)
            results['days_cash_raw'] = days_cash
        else:
            results['days_cash'] = np.nan
            results['days_cash_raw'] = np.nan
        
        # Liquid ratio (quick ratio approximation)
        if filing_type == 'standard':
            ar = self._safe_get(data, 'accounts_receivable', 0)
            ap = self._safe_get(data, 'accounts_payable', 0)
            deferred = self._safe_get(data, 'deferred_revenue', 0)
            
            if not pd.isna(cash):
                liquid_assets = cash + (savings if not pd.isna(savings) else 0) + (ar if not pd.isna(ar) else 0)
                short_term_liab = (ap if not pd.isna(ap) else 0) + (deferred if not pd.isna(deferred) else 0)
                
                if short_term_liab > 0:
                    liquid_ratio = liquid_assets / short_term_liab
                else:
                    liquid_ratio = 10.0 if liquid_assets > 0 else 0.0
                    
                # Healthy: >1.5, Distress: <0.5
                results['liquid_ratio'] = self._score_to_distress(liquid_ratio, 1.5, 0.5)
                results['liquid_ratio_raw'] = liquid_ratio
            else:
                results['liquid_ratio'] = np.nan
                results['liquid_ratio_raw'] = np.nan
        else:
            results['liquid_ratio'] = np.nan
            results['liquid_ratio_raw'] = np.nan
        
        # Deferred revenue risk
        if filing_type == 'standard':
            deferred = self._safe_get(data, 'deferred_revenue', 0)
            revenue = self._safe_get(data, 'total_revenue')
            if not pd.isna(deferred) and not pd.isna(revenue) and revenue > 0:
                deferred_pct = deferred / revenue
                # Healthy: <0.15, Distress: >0.50 (half of "revenue" is actually prepaid)
                results['deferred_revenue_risk'] = self._score_to_distress(deferred_pct, 0.15, 0.50, invert=True)
                results['deferred_revenue_risk_raw'] = deferred_pct
            else:
                results['deferred_revenue_risk'] = np.nan
                results['deferred_revenue_risk_raw'] = np.nan
        else:
            results['deferred_revenue_risk'] = np.nan
            results['deferred_revenue_risk_raw'] = np.nan
        
        return results
    
    def compute_operating_performance(self, data: dict, filing_type: str) -> Dict[str, float]:
        """Compute operating performance indicators."""
        results = {}
        
        # Operating margin
        revenue = self._safe_get(data, 'total_revenue')
        expenses = self._safe_get(data, 'total_expenses')
        if not pd.isna(revenue) and not pd.isna(expenses) and revenue != 0:
            margin = (revenue - expenses) / abs(revenue)
            # Healthy: >0.05 (5% surplus), Distress: <-0.20 (20% deficit)
            results['operating_margin'] = self._score_to_distress(margin, 0.05, -0.20)
            results['operating_margin_raw'] = margin
        else:
            results['operating_margin'] = np.nan
            results['operating_margin_raw'] = np.nan
        
        # Program revenue ratio (self-sustainability)
        if filing_type in ('standard', 'ez'):
            prog_rev = self._safe_get(data, 'program_revenue', 0)
            if not pd.isna(revenue) and revenue > 0:
                # For schools: program revenue IS tuition. Low = donation dependent.
                # But very high can also be risky (no diversification).
                prog_ratio = prog_rev / revenue if not pd.isna(prog_rev) else 0
                # Score: moderate program revenue is healthiest
                # <10% = very donation dependent (risky for schools)
                # >90% = no diversification (also risky)
                if prog_ratio < 0.10:
                    results['program_revenue_ratio'] = 0.6  # Moderate concern
                elif prog_ratio > 0.90:
                    results['program_revenue_ratio'] = 0.4  # Mild concern (concentrated)
                else:
                    results['program_revenue_ratio'] = 0.0  # Healthy mix
                results['program_revenue_ratio_raw'] = prog_ratio
            else:
                results['program_revenue_ratio'] = np.nan
                results['program_revenue_ratio_raw'] = np.nan
        else:
            results['program_revenue_ratio'] = np.nan
            results['program_revenue_ratio_raw'] = np.nan
        
        # Revenue concentration (Herfindahl-like index)
        if filing_type in ('standard', 'ez') and not pd.isna(revenue) and revenue > 0:
            sources = []
            for field in ['contributions', 'program_revenue', 'investment_income']:
                val = self._safe_get(data, field, 0)
                if not pd.isna(val) and val > 0:
                    sources.append(val / revenue)
            
            if sources:
                # HHI: sum of squared shares. 1.0 = single source, <0.33 = diverse
                hhi = sum(s**2 for s in sources)
                # Healthy: <0.50, Distress: >0.90
                results['revenue_concentration'] = self._score_to_distress(hhi, 0.50, 0.90, invert=True)
                results['revenue_concentration_raw'] = hhi
            else:
                results['revenue_concentration'] = np.nan
                results['revenue_concentration_raw'] = np.nan
        else:
            results['revenue_concentration'] = np.nan
            results['revenue_concentration_raw'] = np.nan
        
        # Compensation burden
        if filing_type == 'standard':
            officer_comp = self._safe_get(data, 'officer_compensation', 0)
            other_sal = self._safe_get(data, 'other_salaries', 0)
            pension = self._safe_get(data, 'pension_contributions', 0)
            benefits = self._safe_get(data, 'other_employee_benefits', 0)
            payroll = self._safe_get(data, 'payroll_tax', 0)
            
            total_comp = sum(x for x in [officer_comp, other_sal, pension, benefits, payroll] if not pd.isna(x))
            
            if not pd.isna(expenses) and expenses > 0:
                comp_ratio = total_comp / expenses
                # Healthy: 0.30-0.65, Distress: >0.85 (almost all money goes to people)
                # or <0.10 (no real staff = shell organization)
                if comp_ratio > 0.85:
                    results['compensation_burden'] = self._score_to_distress(comp_ratio, 0.65, 0.90, invert=True)
                elif comp_ratio < 0.10:
                    results['compensation_burden'] = 0.5  # Suspiciously low
                else:
                    results['compensation_burden'] = 0.0
                results['compensation_burden_raw'] = comp_ratio
            else:
                results['compensation_burden'] = np.nan
                results['compensation_burden_raw'] = np.nan
        else:
            results['compensation_burden'] = np.nan
            results['compensation_burden_raw'] = np.nan
        
        return results
    
    def compute_trends(self, ein: str, current_year: int) -> Dict[str, float]:
        """
        Compute trend indicators using multi-year data.
        Looks back 1-3 years from the current year.
        """
        results = {}
        years_data = self.data.get(ein, {})
        
        # Find available prior years
        prior_years = sorted([y for y in years_data.keys() if y < current_year], reverse=True)
        current = years_data.get(current_year, {})
        
        if not prior_years or not current:
            return {
                'revenue_trend': np.nan, 'revenue_trend_raw': np.nan,
                'net_asset_trend': np.nan, 'net_asset_trend_raw': np.nan,
                'expense_growth_gap': np.nan, 'expense_growth_gap_raw': np.nan,
                'employee_trend': np.nan, 'employee_trend_raw': np.nan,
            }
        
        # Use most recent prior year
        prior = years_data[prior_years[0]]
        years_gap = current_year - prior_years[0]
        
        # Revenue trend (annualized % change)
        curr_rev = self._safe_get(current, 'total_revenue')
        prior_rev = self._safe_get(prior, 'total_revenue')
        if not pd.isna(curr_rev) and not pd.isna(prior_rev) and prior_rev != 0:
            rev_change = ((curr_rev / prior_rev) ** (1/years_gap) - 1) if years_gap > 0 else 0
            # Healthy: >0.0 (growing), Distress: <-0.15 (shrinking fast)
            results['revenue_trend'] = self._score_to_distress(rev_change, 0.0, -0.15)
            results['revenue_trend_raw'] = rev_change
        else:
            results['revenue_trend'] = np.nan
            results['revenue_trend_raw'] = np.nan
        
        # Net asset trend
        curr_na = self._safe_get(current, 'total_net_assets')
        prior_na = self._safe_get(prior, 'total_net_assets')
        if not pd.isna(curr_na) and not pd.isna(prior_na):
            if prior_na > 0 and curr_na > 0:
                na_change = ((curr_na / prior_na) ** (1/years_gap) - 1) if years_gap > 0 else 0
            elif prior_na > 0 and curr_na <= 0:
                na_change = -0.30  # Crossed from positive to negative — severe
            elif prior_na < 0 and curr_na < prior_na:
                na_change = -0.20  # Getting worse from already negative
            elif prior_na < 0 and curr_na > prior_na:
                na_change = 0.05   # Improving from negative
            else:
                na_change = -0.10 if curr_na <= 0 else 0.0
            # Healthy: >0.0, Distress: <-0.10
            results['net_asset_trend'] = self._score_to_distress(na_change, 0.0, -0.10)
            results['net_asset_trend_raw'] = na_change
        else:
            results['net_asset_trend'] = np.nan
            results['net_asset_trend_raw'] = np.nan
        
        # Expense growth gap (expense growth minus revenue growth)
        curr_exp = self._safe_get(current, 'total_expenses')
        prior_exp = self._safe_get(prior, 'total_expenses')
        if (not pd.isna(curr_rev) and not pd.isna(prior_rev) and prior_rev != 0 and
            not pd.isna(curr_exp) and not pd.isna(prior_exp) and prior_exp != 0):
            rev_growth = (curr_rev / prior_rev) ** (1/years_gap) - 1 if years_gap > 0 else 0
            exp_growth = (curr_exp / prior_exp) ** (1/years_gap) - 1 if years_gap > 0 else 0
            gap = exp_growth - rev_growth
            # Healthy: <0.0 (expenses growing slower), Distress: >0.10 (expenses outpacing revenue)
            results['expense_growth_gap'] = self._score_to_distress(gap, 0.0, 0.10, invert=True)
            results['expense_growth_gap_raw'] = gap
        else:
            results['expense_growth_gap'] = np.nan
            results['expense_growth_gap_raw'] = np.nan
        
        # Employee trend
        curr_emp = self._safe_get(current, 'employee_count')
        prior_emp = self._safe_get(prior, 'employee_count')
        if not pd.isna(curr_emp) and not pd.isna(prior_emp) and prior_emp > 0:
            emp_change = ((curr_emp / prior_emp) ** (1/years_gap) - 1) if years_gap > 0 else 0
            # Healthy: >-0.02, Distress: <-0.20
            results['employee_trend'] = self._score_to_distress(emp_change, -0.02, -0.20)
            results['employee_trend_raw'] = emp_change
        else:
            results['employee_trend'] = np.nan
            results['employee_trend_raw'] = np.nan
        
        return results
    
    def compute_red_flags(self, data: dict, filing_type: str) -> Dict[str, float]:
        """Compute binary/categorical red flag indicators."""
        results = {}
        
        # Ceased operations
        ceased = data.get('ceased_operations', 'N')
        results['ceased_operations'] = 1.0 if str(ceased).upper() in ('Y', 'YES', '1', 'TRUE') else 0.0
        
        # Insider loans
        if filing_type == 'standard':
            payable_officers = self._safe_get(data, 'payable_to_officers', 0)
            recv_officers = self._safe_get(data, 'current_receivables_from_officers', 0)
            total_assets = self._safe_get(data, 'total_assets', 1)
            
            insider = (payable_officers if not pd.isna(payable_officers) else 0) + \
                      (recv_officers if not pd.isna(recv_officers) else 0)
            
            if total_assets > 0 and insider > 0:
                insider_pct = insider / total_assets
                results['insider_loans'] = min(1.0, insider_pct / 0.10)  # >10% of assets = max flag
            else:
                results['insider_loans'] = 0.0
        else:
            results['insider_loans'] = np.nan
        
        # Fundraising efficiency
        if filing_type == 'standard':
            fr_fees = self._safe_get(data, 'fundraising_fees', 0)
            contributions = self._safe_get(data, 'contributions', 0)
            if not pd.isna(fr_fees) and contributions > 0:
                fr_ratio = fr_fees / contributions
                # >50% of contributions going to fundraising = bad
                results['fundraising_efficiency'] = min(1.0, fr_ratio / 0.50)
            else:
                results['fundraising_efficiency'] = 0.0
        else:
            results['fundraising_efficiency'] = np.nan
        
        # Asset liquidation
        sold = data.get('sold_assets', 'N')
        results['asset_liquidation'] = 0.5 if str(sold).upper() in ('Y', 'YES', '1', 'TRUE') else 0.0
        
        return results
    
    # =========================================================================
    # SCORE AGGREGATION
    # =========================================================================
    
    def score_entity(self, ein: str, year: int) -> Dict:
        """
        Compute the full distress score for a single entity in a single year.
        
        Returns dict with:
            - distress_score: 0-100 composite score
            - domain scores: 0-100 per domain
            - raw indicator values
            - data quality metrics
        """
        data = self.data.get(ein, {}).get(year, {})
        if not data:
            return {'ein': ein, 'year': year, 'distress_score': np.nan, 'error': 'no_data'}
        
        filing_type = self.filing_types.get(ein, 'unknown')
        
        # Compute all indicator domains
        solvency = self.compute_solvency(data, filing_type)
        liquidity = self.compute_liquidity(data, filing_type)
        operating = self.compute_operating_performance(data, filing_type)
        trends = self.compute_trends(ein, year)
        red_flags = self.compute_red_flags(data, filing_type)
        
        # Aggregate within each domain (weighted average of non-null indicators)
        domain_scores = {}
        domain_configs = {
            'solvency': (solvency, DISTRESS_INDICATORS['solvency']['indicators']),
            'liquidity': (liquidity, DISTRESS_INDICATORS['liquidity']['indicators']),
            'operating_performance': (operating, DISTRESS_INDICATORS['operating_performance']['indicators']),
            'trend': (trends, DISTRESS_INDICATORS['trend']['indicators']),
            'red_flags': (red_flags, DISTRESS_INDICATORS['red_flags']['indicators']),
        }
        
        for domain_name, (indicator_values, indicator_configs) in domain_configs.items():
            weighted_sum = 0
            weight_sum = 0
            
            for ind_name, ind_config in indicator_configs.items():
                score = indicator_values.get(ind_name, np.nan)
                if not pd.isna(score):
                    w = ind_config['weight']
                    weighted_sum += score * w
                    weight_sum += w
            
            if weight_sum > 0:
                domain_scores[domain_name] = (weighted_sum / weight_sum) * 100
            else:
                domain_scores[domain_name] = np.nan
        
        # Aggregate across domains (weighted, renormalize if some domains are missing)
        total_weighted = 0
        total_weight = 0
        for domain_name, domain_config in DISTRESS_INDICATORS.items():
            domain_score = domain_scores.get(domain_name, np.nan)
            if not pd.isna(domain_score):
                w = domain_config['weight']
                total_weighted += domain_score * w
                total_weight += w
        
        composite = (total_weighted / total_weight) if total_weight > 0 else np.nan
        
        # Count how many indicators we actually computed vs total possible
        all_indicators = {**solvency, **liquidity, **operating, **trends, **red_flags}
        scored = sum(1 for k, v in all_indicators.items() 
                     if not k.endswith('_raw') and not pd.isna(v))
        total_possible = sum(1 for k in all_indicators.keys() if not k.endswith('_raw'))
        
        # Build result
        result = {
            'ein': ein,
            'year': year,
            'filing_type': filing_type,
            'distress_score': round(composite, 1) if not pd.isna(composite) else np.nan,
            'risk_category': self._categorize_risk(composite),
            'data_completeness': round(scored / total_possible * 100, 0) if total_possible > 0 else 0,
            'indicators_scored': scored,
            'indicators_total': total_possible,
            
            # Domain scores
            'solvency_score': round(domain_scores.get('solvency', np.nan), 1),
            'liquidity_score': round(domain_scores.get('liquidity', np.nan), 1),
            'operating_score': round(domain_scores.get('operating_performance', np.nan), 1),
            'trend_score': round(domain_scores.get('trend', np.nan), 1),
            'red_flag_score': round(domain_scores.get('red_flags', np.nan), 1),
        }
        
        # Add raw values for transparency
        for k, v in all_indicators.items():
            if k.endswith('_raw'):
                result[k] = round(v, 4) if (not pd.isna(v) and not isinstance(v, complex)) else np.nan
        
        return result
    
    def _categorize_risk(self, score: float) -> str:
        """Convert numeric score to risk category."""
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
    
    def score_all(self, target_year: Optional[int] = None) -> pd.DataFrame:
        """
        Score all loaded entities.
        
        Args:
            target_year: If provided, score only this year. 
                        Otherwise, score the most recent year for each EIN.
        """
        results = []
        
        for ein in self.data:
            years = sorted(self.data[ein].keys())
            if not years:
                continue
                
            if target_year:
                if target_year in years:
                    results.append(self.score_entity(ein, target_year))
            else:
                # Score most recent year
                results.append(self.score_entity(ein, years[-1]))
        
        df = pd.DataFrame(results)
        
        if len(df) > 0:
            print(f"\nScored {len(df)} entities")
            print(f"\nRisk Distribution:")
            print(df['risk_category'].value_counts().to_string())
            print(f"\nAverage data completeness: {df['data_completeness'].mean():.0f}%")
            print(f"Score statistics:")
            print(df['distress_score'].describe().to_string())
        
        return df
    
    def score_all_years(self) -> pd.DataFrame:
        """Score every entity × every year they have data for."""
        results = []
        for ein in self.data:
            for year in sorted(self.data[ein].keys()):
                results.append(self.score_entity(ein, year))
        return pd.DataFrame(results)
    
    def integrate_with_master(self, master_path: str, output_path: str = None,
                              target_year: int = 2024) -> pd.DataFrame:
        """
        Score all entities and merge results back into the Hummingbird Master file.
        
        Matches on EIN. Updates distress scores and adds new columns for
        990-only institutions (data_source == 'Hummingbird_990').
        
        Args:
            master_path: Path to Hummingbird_Master_Distress.csv
            output_path: Where to save the updated master (optional)
            target_year: Primary year to score (falls back to most recent available)
            
        Returns:
            Updated master DataFrame
        """
        print(f"\n{'='*60}")
        print(f"INTEGRATING WITH HUMMINGBIRD MASTER")
        print(f"{'='*60}")
        
        # Load master
        master = pd.read_csv(master_path, encoding='latin-1', low_memory=False)
        print(f"Master file: {len(master)} institutions")
        
        # Identify 990-only institutions
        mask_990 = master['data_source'] == 'Hummingbird_990'
        print(f"990-only institutions: {mask_990.sum()}")
        
        # Clean master EINs to match our format
        master['ein_clean'] = master['ein'].apply(
            lambda x: str(int(x)).lstrip('0') if pd.notna(x) else None
        )
        
        # Score all entities, preferring target year but falling back
        results = []
        matched = 0
        no_data = 0
        
        for idx, row in master[mask_990].iterrows():
            ein = row['ein_clean']
            if ein is None or ein not in self.data:
                no_data += 1
                continue
            
            # Find best year to score (prefer target, fall back to most recent)
            available_years = sorted(self.data[ein].keys(), reverse=True)
            if target_year in available_years:
                score_year = target_year
            else:
                score_year = available_years[0]  # Most recent
            
            result = self.score_entity(ein, score_year)
            result['master_idx'] = idx
            results.append(result)
            matched += 1
        
        print(f"Matched and scored: {matched}")
        print(f"No 990 data found: {no_data}")
        
        if not results:
            print("WARNING: No matches found. Check EIN formats.")
            return master
        
        scores_df = pd.DataFrame(results)
        
        # Map scores back to master file columns
        # New enhanced columns
        new_cols = {
            'distress_score_990': 'distress_score',
            'distress_category_990': 'risk_category',
            'solvency_score_990': 'solvency_score',
            'liquidity_score_990': 'liquidity_score',
            'operating_score_990': 'operating_score',
            'trend_score_990': 'trend_score',
            'red_flag_score_990': 'red_flag_score',
            'data_completeness_990': 'data_completeness',
            'filing_type_990': 'filing_type',
            'score_year_990': 'year',
            # Raw metrics for transparency
            'equity_ratio_raw_990': 'equity_ratio_raw',
            'unrestricted_cushion_raw_990': 'unrestricted_cushion_raw',
            'days_cash_raw_990': 'days_cash_raw',
            'operating_margin_raw_990': 'operating_margin_raw',
            'debt_ratio_raw_990': 'debt_ratio_raw',
            'revenue_trend_raw_990': 'revenue_trend_raw',
            'net_asset_trend_raw_990': 'net_asset_trend_raw',
            'revenue_concentration_raw_990': 'revenue_concentration_raw',
            'program_revenue_ratio_raw_990': 'program_revenue_ratio_raw',
        }
        
        # Initialize new columns
        for master_col in new_cols:
            if master_col not in master.columns:
                master[master_col] = np.nan
        
        # Write scores to master
        for _, score_row in scores_df.iterrows():
            idx = score_row['master_idx']
            for master_col, score_col in new_cols.items():
                if score_col in score_row.index:
                    master.at[idx, master_col] = score_row[score_col]
            
            # Also update the main distress_score and distress_category columns
            if not pd.isna(score_row.get('distress_score')):
                master.at[idx, 'distress_score'] = score_row['distress_score']
                master.at[idx, 'distress_category'] = self._map_category_to_master(
                    score_row['risk_category']
                )
        
        # Summary
        scored_990 = master.loc[mask_990]
        print(f"\n--- Updated Master Summary (990-only) ---")
        print(f"Distress distribution:")
        print(scored_990['distress_category'].value_counts().to_string())
        print(f"\nScore statistics:")
        print(scored_990['distress_score'].describe().to_string())
        
        if output_path:
            master.to_csv(output_path, index=False)
            print(f"\nSaved updated master to: {output_path}")
        
        return master
    
    def _map_category_to_master(self, risk_category: str) -> str:
        """Map engine risk categories to master file categories."""
        mapping = {
            'Healthy': 'Healthy',
            'Low Risk': 'Low',
            'Moderate Risk': 'Moderate',
            'High Risk': 'High',
            'Severe Distress': 'Critical',
            'Insufficient Data': 'Healthy',  # Conservative default
        }
        return mapping.get(risk_category, 'Healthy')


# =============================================================================
# COMPARISON TABLE: 990 vs IPEDS Distress Metrics
# =============================================================================

METRIC_CROSSWALK = """
990 Distress Metric              | IPEDS Equivalent                    | Notes
---------------------------------|-------------------------------------|------
Equity ratio                     | Equity ratio (FASB/GASB)            | Direct equivalent
Unrestricted cushion             | Unrestricted net assets / expenses  | Direct equivalent  
Days cash on hand                | Days cash (computed)                | Direct equivalent
Operating margin                 | (Revenue - Expenses) / Revenue      | Direct equivalent
Debt ratio                       | Total liabilities / Total assets    | Direct equivalent
Revenue trend                    | Enrollment trend (proxy)            | 990 has revenue; IPEDS has enrollment
Net asset trend                  | Net asset trajectory                | Direct equivalent
Program revenue ratio            | Tuition dependency                  | Inverted: 990 prog_rev ≈ IPEDS tuition
Revenue concentration (HHI)      | Tuition % of core revenues          | Similar concept
Employee trend                   | FTE staff trend                     | Direct equivalent
Compensation burden              | Instruction ratio                   | Similar concept
Fundraising efficiency           | (not available)                     | 990-only metric
Deferred revenue risk            | (not available)                     | 990-only metric
(not available)                  | Retention rate                      | IPEDS-only metric
(not available)                  | Graduation rate                     | IPEDS-only metric
(not available)                  | Admissions yield                    | IPEDS-only metric
"""


# =============================================================================
# CONFIGURATION — Update these paths to match your file locations
# =============================================================================

STANDARD_990_FILES = [
    'hv_master_data/data/990s/20eoextract990.csv',
    'hv_master_data/data/990s/21eoextract990.csv',
    'hv_master_data/data/990s/22eoextract990.csv',
    'hv_master_data/data/990s/23eoextract990.csv',
    'hv_master_data/data/990s/24eoextract990.csv',
]

EZ_990_FILES = [
    'hv_master_data/data/990s/20eoextract990EZ.csv',
    'hv_master_data/data/990s/21eoextract990EZ.csv',
    'hv_master_data/data/990s/22eoextract990EZ.csv',
    'hv_master_data/data/990s/23eoextract990EZ.csv',
    'hv_master_data/data/990s/24eoextract990EZ.csv',
]

PF_990_FILES = [
    'hv_master_data/data/990s/20eoextract990pf.csv',
    'hv_master_data/data/990s/21eoextract990pf.csv',
    'hv_master_data/data/990s/22eoextract990pf.csv',
    'hv_master_data/data/990s/23eoextract990pf.csv',
    'hv_master_data/data/990s/24eoextract990pf.csv',
]

MASTER_FILE = 'hv_master_data/data/Hummingbird_990_BMF_Integrated.csv'
OUTPUT_FILE = 'hv_master_data/data/Hummingbird_bmf_scored.csv'
SCORES_DETAIL_FILE = 'hv_master_data/data/990_distress_scores_detail.csv'

# =============================================================================
# RUN
# =============================================================================

if __name__ == '__main__':
    import os

    print("=" * 70)
    print("990 DISTRESS SCORING — HUMMINGBIRD INTEGRATION")
    print("=" * 70)

    # --- Step 1: Get EIN list from master file ---
    master = pd.read_csv(MASTER_FILE, encoding='latin-1', low_memory=False)
    hb990_mask = master['data_source'] == 'Hummingbird_990'
    target_eins = set(
        str(int(x)) for x in master.loc[hb990_mask, 'ein'].dropna()
    )
    print(f"\nTarget EINs from master file: {len(target_eins)}")

    # --- Step 2: Load all 990 files (filtering to only our EINs) ---
    engine = Distress990Engine()

    std_files = [f for f in STANDARD_990_FILES if os.path.exists(f)]
    ez_files = [f for f in EZ_990_FILES if os.path.exists(f)]
    pf_files = [f for f in PF_990_FILES if os.path.exists(f)]

    print(f"\nFiles found: {len(std_files)} standard, {len(ez_files)} EZ, {len(pf_files)} PF")

    if not std_files and not ez_files and not pf_files:
        print("\n⚠️  No 990 files found! Update the file paths at the top of this script.")
        print("   Expected paths like: data/990_standard_2020.csv")
        exit(1)

    engine.load_data(
        standard_paths=std_files,
        ez_paths=ez_files,
        pf_paths=pf_files,
        filter_eins=target_eins
    )

    # --- Step 3: Score and integrate with master ---
    updated_master = engine.integrate_with_master(
        master_path=MASTER_FILE,
        output_path=OUTPUT_FILE,
        target_year=2024
    )

    # --- Step 4: Export detailed year-by-year scores ---
    all_scores = engine.score_all_years()
    all_scores.to_csv(SCORES_DETAIL_FILE, index=False)
    print(f"\nDetailed year-by-year scores saved to: {SCORES_DETAIL_FILE}")

    # --- Summary ---
    print("\n" + "=" * 70)
    print("DONE!")
    print("=" * 70)
    print(f"\nOutputs:")
    print(f"  1. {OUTPUT_FILE}")
    print(f"     → Updated master with enhanced 990 distress scores")
    print(f"  2. {SCORES_DETAIL_FILE}")
    print(f"     → Year-by-year scores for trend analysis")