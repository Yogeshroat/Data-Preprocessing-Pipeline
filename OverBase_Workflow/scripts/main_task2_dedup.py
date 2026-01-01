#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task2_remove_duplicates import task2_remove_duplicates as t2

def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    inputs = project_root / "outputs" / "senior_execs_only.csv"
    if not inputs.exists():
        print(f"❌ Missing input: {inputs}. Run task 1 first.")
        return 1
    df = pd.read_csv(inputs)
    t2(df)  # writes outputs/senior_execs_no_duplicates.csv
    print("✓ Task 2 completed (outputs/senior_execs_no_duplicates.csv)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
