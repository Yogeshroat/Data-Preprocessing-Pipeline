#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task1_filter_senior_execs import task1_filter_senior_execs as t1

def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    inputs = project_root / "outputs" / "final_cleaned_data.csv"
    if not inputs.exists():
        print(f"❌ Missing input: {inputs}. Run step 1 first.")
        return 1
    df = pd.read_csv(inputs)
    t1(df)  # writes outputs/senior_execs_only.csv
    print("✓ Task 1 completed (outputs/senior_execs_only.csv)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
