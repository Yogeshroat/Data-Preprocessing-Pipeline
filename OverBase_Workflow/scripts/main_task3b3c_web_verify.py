#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task3b_verify_employment import task3b_verify_employment as t3b
from filters.task3c_verify_employment_webscrape import task3c_verify_employment_webscrape as t3c


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
    df = t3b(df)  # writes outputs/step3b_verified.csv

    # 3C (web scrape; honors OVERBASE_SCRAPE_MODE)
    df = t3c(df)  # writes outputs/step3c_verified_web.csv

    # Persist a consolidated web-verified snapshot
    web_csv = outputs / 'senior_execs_web_verified.csv'
    df.to_csv(web_csv, index=False)
    print(f"✓ Wrote {web_csv} with {len(df)} rows")
    print("Note: step3b_verified.csv and step3c_verified_web.csv were also written by the tasks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
