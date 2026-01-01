#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task3b_verify_employment import task3b_verify_employment as t3b
from filters.task3c_verify_employment_webscrape import task3c_verify_employment_webscrape as t3c
from filters.task4_generate_emails import task4_generate_emails as t4
from filters.task6_youtube_osint import task6_youtube_osint as t6


def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    outputs = project_root / "outputs"
    inputs = outputs / "senior_execs_validated.csv"
    if not inputs.exists():
        print(f"❌ Missing input: {inputs}. Run task 3 first.")
        return 1

    # 3B
    df = pd.read_csv(inputs)
    df = t3b(df)

    # 3C (web scrape; honors OVERBASE_SCRAPE_MODE)
    df = t3c(df)

    # Task 4 (emails)
    df = t4(df)

    # YouTube OSINT scoring
    t6(df)

    # Separate outputs (no combine)
    df_emails = pd.read_csv(outputs / 'senior_execs_with_emails.csv')

    web = df_emails[df_emails['Employment Verified'].astype(str).str.lower()=='yes'].copy()
    if not web.empty:
        web['Web Confidence'] = '100%'
    web_out = outputs / 'final_web_verified.csv'
    web.to_csv(web_out, index=False)

    os_top15_src = outputs / 'step6_osint_top15.csv'
    os_top15 = pd.read_csv(os_top15_src) if os_top15_src.exists() else pd.DataFrame()
    os_out = outputs / 'final_osint_top15.csv'
    os_top15.to_csv(os_out, index=False)

    print(f"✓ Wrote {web_out} ({len(web)} rows) and {os_out} ({len(os_top15)} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
