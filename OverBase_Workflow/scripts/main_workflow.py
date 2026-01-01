#!/usr/bin/env python3
"""
Main Workflow Script for OverBase Data Cleaning Task

This script orchestrates the complete workflow:
1. Load and clean raw data (using initial_cleanup.load_and_clean_data)
2. Filter senior executives
3. Remove duplicates
4. Validate companies and find websites
5. Generate email addresses
6. Quality checks and final output

Usage:
    python scripts/main_workflow.py
"""

import os
import sys
from pathlib import Path
import pandas as pd

# Import the load_and_clean_data function from initial_cleanup
sys.path.insert(0, str(Path(__file__).parent))
from initial_cleanup.initial_cleanup import load_and_clean_data
from filters.task1_filter_senior_execs import task1_filter_senior_execs as filter_task1_filter_senior_execs
from filters.task2_remove_duplicates import task2_remove_duplicates as filter_task2_remove_duplicates
from filters.task3_validate_companies import task3_validate_companies as filter_task3_validate_companies
from filters.task3b_verify_employment import task3b_verify_employment as filter_task3b_verify_employment
from filters.task3c_verify_employment_webscrape import (
    task3c_verify_employment_webscrape as filter_task3c_verify_employment_webscrape,
)
from filters.task4_generate_emails import task4_generate_emails as filter_task4_generate_emails
from filters.task5_quality_check import task5_quality_check as filter_task5_quality_check

# Get project root directory
script_dir = Path(__file__).parent
workflow_dir = script_dir.parent
PROJECT_ROOT = workflow_dir.resolve()

# Change to workflow directory
os.chdir(PROJECT_ROOT)

# Paths
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "outputs"
RAW_CSV = DATA_DIR / "cmo_videos_names.csv"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# STEP 0: INITIAL DATA LOADING
# ============================================================================
def step0_load_raw_data():
    """Step 0: Load and clean raw data using initial_cleanup function"""
    print("\n" + "=" * 70)
    print("▶ Step 0: Load and Clean Raw Data")
    print("=" * 70)
    
    if not RAW_CSV.exists():
        print(f"❌ Error: Raw data file '{RAW_CSV}' not found!")
        return None
    
    print(f"Loading data from: {RAW_CSV}")
    df = load_and_clean_data(RAW_CSV)
    print(f"✓ Loaded {len(df)} rows")
    
    # Preserve original input order for downstream steps
    if 'Original Order' not in df.columns:
        df.insert(0, 'Original Order', range(len(df)))

    # Save initial cleaned data
    initial_output = OUTPUT_DIR / "final_cleaned_data.csv"
    df.to_csv(str(initial_output), index=False)
    print(f"✓ Saved initial cleaned data to: {initial_output}")
    
    return df


# ============================================================================
# SENIOR EXECUTIVE FILTERING
# ============================================================================
 


# ============================================================================
# EMAIL GENERATION HELPERS
# ============================================================================
 


# ============================================================================
# LINKEDIN SOURCE URL GENERATION
# ============================================================================
 


# ============================================================================
# COMPANY WEBSITE EXTRACTION
# ============================================================================
 


# ============================================================================
# TASK 1: FILTER SENIOR EXECUTIVES
# ============================================================================
 


# ============================================================================
# TASK 2: REMOVE DUPLICATES
# ============================================================================
 


# ============================================================================
# TASK 3: VALIDATE COMPANIES
# ============================================================================
 


# ============================================================================
# TASK 4: GENERATE EMAIL ADDRESSES
# ============================================================================
 


# ============================================================================
# TASK 5: QUALITY CHECK & FINAL OUTPUT
# ============================================================================
 


# ============================================================================
# MAIN WORKFLOW
# ============================================================================
def main():
    """Main workflow execution"""
    print("\n" + "=" * 70)
    print("OVERBASE DATA CLEANING WORKFLOW")
    print("=" * 70)
    print("\nStarting workflow execution...\n")
    
    # Step 0: Load raw data using initial_cleanup function
    df = step0_load_raw_data()
    if df is None:
        return 1
    
    # Run tasks sequentially
    try:
        df = filter_task1_filter_senior_execs(df)
        df = filter_task2_remove_duplicates(df)
        df = filter_task3_validate_companies(df)
        df = filter_task3b_verify_employment(df)
        df = filter_task3c_verify_employment_webscrape(df)
        df = filter_task4_generate_emails(df)
        df = filter_task5_quality_check(df)
    except Exception as e:
        print(f"\n❌ Error during workflow execution: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Final summary
    print("\n" + "=" * 70)
    print("WORKFLOW SUMMARY")
    print("=" * 70)
    print("\n✅ All tasks completed successfully!")
    final_output = OUTPUT_DIR / "final_cleaned_data.csv"
    quality_report = OUTPUT_DIR / "quality_report.txt"
    email_patterns = OUTPUT_DIR / "email_patterns_used.csv"
    print(f"\nFinal output file: {final_output}")
    print(f"Quality report: {quality_report}")
    print(f"Email patterns log: {email_patterns}")
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
