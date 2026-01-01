#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task1_filter_senior_execs import task1_filter_senior_execs as t1
from filters.task2_remove_duplicates import task2_remove_duplicates as t2


def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    input_csv = project_root / 'outputs' / 'final_cleaned_data.csv'
    if not input_csv.exists():
        print(f"❌ Missing input: {input_csv}. Run step 1 first.")
        return 1
    df = pd.read_csv(input_csv)
    df = t1(df)  # writes outputs/senior_execs_only.csv
    df = t2(df)  # writes outputs/senior_execs_no_duplicates.csv
    print('✓ Task 1+2 completed (outputs/senior_execs_no_duplicates.csv)')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
