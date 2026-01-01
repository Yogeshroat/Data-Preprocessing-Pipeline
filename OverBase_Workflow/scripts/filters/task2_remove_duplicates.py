#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import re

# Get project root directory and ensure outputs dir exists
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
for p in [OUTPUT_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "workflow.log"


def log(message: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"[task2_remove_duplicates] {message}\n")

# ============================================================================
# TASK 2: REMOVE DUPLICATES
# ============================================================================
def task2_remove_duplicates(df):
    """Task 2: Remove duplicates"""
    print("\n" + "=" * 70)
    print("▶ Task 2: Remove Duplicates")
    print("=" * 70)
    
    print(f"Loaded {len(df)} senior executives")
    log(f"Starting dedup with {len(df)} rows")
    
    # Preserve current order to restore after deduplication
    df['_original_order'] = range(len(df))
    
    def normalize_name(name):
        """Normalize name for comparison"""
        if pd.isna(name) or name == "":
            return ""
        name = str(name).strip().lower()
        name = name.replace('"', '').replace("'", '').replace('"', '')
        name = name.replace(' (jj)', '').replace('(jj)', '')
        name = ' '.join(name.split())
        return name
    
    def normalize_company(company):
        """Normalize company name for comparison"""
        if pd.isna(company) or company == "":
            return ""
        company = str(company).strip().lower()
        company = company.split('—')[0].split('-')[0].strip()
        company = ' '.join(company.split())
        return company
    
    df['_normalized_name'] = df['Name'].apply(normalize_name)
    df['_normalized_company'] = df['Company'].apply(normalize_company)
    
    # Calculate completeness score
    completeness_scores = (
        (df['Name'] != '').astype(int) +
        (df['Title'] != '').astype(int) +
        (df['Company'] != '').astype(int)
    )
    
    # Add YouTube URL score if column exists
    if 'Youtube URL' in df.columns:
        completeness_scores += (df['Youtube URL'] != '').astype(int)
    
    df['_completeness'] = completeness_scores
    
    df_sorted = df.sort_values(['_completeness', '_normalized_name'], ascending=[False, True])
    df_unique = df_sorted.drop_duplicates(
        subset=['_normalized_name', '_normalized_company'],
        keep='first'
    ).drop(columns=['_normalized_name', '_normalized_company', '_completeness'])
    
    df_unique = df_unique.sort_values('_original_order').drop(columns=['_original_order']).reset_index(drop=True)
    
    OUTPUT_CSV = OUTPUT_DIR / "senior_execs_no_duplicates.csv"
    STEP_CSV = OUTPUT_DIR / "step2_dedup.csv"
    df_unique.to_csv(str(OUTPUT_CSV), index=False)
    df_unique.to_csv(str(STEP_CSV), index=False)
    
    print("✔ Task 2 completed")
    removed = len(df) - len(df_unique)
    print(f"Removed {removed} duplicates")
    print(f"Remaining senior execs: {len(df_unique)}")
    print(f"Saved to: {OUTPUT_CSV}")
    log(f"Removed {removed} duplicates -> saved {OUTPUT_CSV} and {STEP_CSV}")
    return df_unique