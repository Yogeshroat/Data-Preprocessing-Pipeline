#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import urllib.parse
from datetime import datetime

# Project paths
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
MANUAL_DIR = OUTPUT_DIR / "manual"
for p in [OUTPUT_DIR, LOGS_DIR, MANUAL_DIR]:
    p.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "workflow.log"


def log(message: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"[task3b_verify_employment] {message}\n")


def linkedin_search_url(name: str, company: str) -> str:
    name = name or ""
    company = company or ""
    q = urllib.parse.quote(f"{name} {company}")
    return f"https://www.linkedin.com/search/results/all/?keywords={q}"


# ============================================================================
# TASK 3b: VERIFY CURRENT EMPLOYMENT (SEMI-MANUAL)
# ============================================================================

def task3b_verify_employment(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each row, generate a LinkedIn search URL and prepare verification columns.
    If a manual override file exists at outputs/manual/verification_overrides.csv,
    merge it to set 'Employment Verified' to 'yes' and capture 'Verification Source'.

    Columns added:
      - LinkedIn Search URL
      - Employment Verified (yes/no)
      - Verification Source (URL)
      - Verified At (ISO date)
    """
    print("\n" + "=" * 70)
    print("▶ Task 3b: Verify Employment")
    print("=" * 70)
    log("Starting employment verification step")

    # Build base verification columns
    df_ver = df.copy()
    df_ver['LinkedIn Search URL'] = df_ver.apply(
        lambda r: linkedin_search_url(str(r.get('Name', '')), str(r.get('Company', ''))), axis=1
    )
    df_ver['Employment Verified'] = 'no'
    df_ver['Verification Source'] = ''
    df_ver['Verified At'] = ''

    # Write a template for manual verification
    template_path = MANUAL_DIR / 'verification_template.csv'
    template_cols = ['Name', 'Company', 'LinkedIn Search URL', 'Verification Source', 'Employment Verified', 'Verified At']
    try:
        df_ver[template_cols].to_csv(template_path, index=False)
        log(f"Wrote verification template to {template_path}")
    except Exception as e:
        log(f"Failed writing template: {e}")

    # If overrides exist, merge them
    overrides_path = MANUAL_DIR / 'verification_overrides.csv'
    if overrides_path.exists():
        try:
            ov = pd.read_csv(overrides_path)
            # Normalize join keys
            def norm(s):
                return '' if pd.isna(s) else str(s).strip().lower()
            df_ver['_k'] = df_ver['Name'].map(norm) + '|' + df_ver['Company'].map(norm)
            ov['_k'] = ov['Name'].map(norm) + '|' + ov['Company'].map(norm)
            ov = ov.set_index('_k')

            # Apply overrides
            for idx, row in df_ver.iterrows():
                k = row['_k']
                if k in ov.index:
                    ver = ov.loc[k]
                    emp_verified = str(ver.get('Employment Verified', '')).strip().lower()
                    if emp_verified in ['yes', 'true', 'y', '1']:
                        df_ver.at[idx, 'Employment Verified'] = 'yes'
                        src = str(ver.get('Verification Source', '')).strip()
                        if src:
                            df_ver.at[idx, 'Verification Source'] = src
                        if not str(df_ver.at[idx, 'Verified At']).strip():
                            df_ver.at[idx, 'Verified At'] = datetime.utcnow().date().isoformat()
            df_ver.drop(columns=['_k'], inplace=True)
            log(f"Applied overrides from {overrides_path}")
        except Exception as e:
            log(f"Failed reading overrides: {e}")

    # Persist step artifact
    step_csv = OUTPUT_DIR / 'step3b_verified.csv'
    try:
        df_ver.to_csv(step_csv, index=False)
        log(f"Saved step3b_verified.csv with {len(df_ver)} rows")
    except Exception as e:
        log(f"Failed writing step3b_verified.csv: {e}")

    print(f"✓ Employment verification step prepared for {len(df_ver)} executives.")
    return df_ver
