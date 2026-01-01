#!/usr/b    in/env python3

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
        f.write(f"[task1_filter_senior_execs] {message}\n")

# ============================================================================
# SENIOR EXECUTIVE FILTERING
# ============================================================================
SENIOR_POSITIVE = [
    r"\bCEO\b",
    r"\bCMO\b",
    r"\bCFO\b",
    r"\bCTO\b",
    r"\bCOO\b",
    r"\bChief\b",
    r"\bC-?Suite\b",
    r"\bVP\b",
    r"\bVice President\b",
    r"\bSVP\b",
    r"\bEVP\b",
    r"\bPresident\b",
    r"\bDirector\b",
    r"\bManaging Director\b",
    r"\bGlobal Head\b",
    r"\bHead of\b",
    r"\bHead\b",
    r"\bGM\b",
    r"\bFounder\b",
]

SENIOR_NEGATIVE = [
    r"\bAssistant\b",
    r"\bAssociate\b",
    r"\bJunior\b",
    r"\bIntern\b",
    r"\bAnalyst\b",
    r"\bConsultant\b",
    r"\bAdvisor\b",
    r"\bLecturer\b",
    r"\bProfessor\b",
    r"\bFormer\b",
    r"\bEx-",
    r"\bRetired\b",
    r"\bAssistant to\b",
]

SENIOR_POS_PATTERN = re.compile("|".join(SENIOR_POSITIVE), flags=re.IGNORECASE)
SENIOR_NEG_PATTERN = re.compile("|".join(SENIOR_NEGATIVE), flags=re.IGNORECASE)


def is_senior_executive(title: str) -> bool:
    if pd.isna(title) or not str(title).strip():
        return False
    title_str = str(title).strip()
    if any(phrase in title_str for phrase in ["(Title not stated)", "(Title not specified)", "—"]):
        return False
    if SENIOR_NEG_PATTERN.search(title_str):
        return False
    return bool(SENIOR_POS_PATTERN.search(title_str))


# ============================================================================
# TASK 1: FILTER SENIOR EXECUTIVES
# ============================================================================
def task1_filter_senior_execs(df):
    """Task 1: Filter senior executives"""
    print("\n" + "=" * 70)
    print("▶ Task 1: Filter Senior Executives")
    print("=" * 70)
    
    df_senior = df[df["Title"].apply(is_senior_executive)].copy()
    
    OUTPUT_CSV = OUTPUT_DIR / "senior_execs_only.csv"
    STEP_CSV = OUTPUT_DIR / "step1_senior.csv"
    df_senior.to_csv(str(OUTPUT_CSV), index=False)
    df_senior.to_csv(str(STEP_CSV), index=False)
    log(f"Filtered senior execs: {len(df_senior)} -> saved {OUTPUT_CSV} and {STEP_CSV}")
    
    print("✔ Task 1 completed")
    print(f"Senior execs found: {len(df_senior)}")
    print(f"Saved to: {OUTPUT_CSV}")
    return df_senior
