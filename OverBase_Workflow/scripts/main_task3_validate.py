#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task3_validate_companies import task3_validate_companies as t3

def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    inputs = project_root / "outputs" / "senior_execs_no_duplicates.csv"
    if not inputs.exists():
        print(f"❌ Missing input: {inputs}. Run task 2 first.")
        return 1
    df = pd.read_csv(inputs)
    t3(df)  # writes outputs/step3_domains.csv and outputs/senior_execs_validated.csv
    print("✓ Task 3 completed (outputs/step3_domains.csv, outputs/senior_execs_validated.csv)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
