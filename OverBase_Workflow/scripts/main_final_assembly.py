#!/usr/bin/env python3
from pathlib import Path
import sys
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from filters.task4_generate_emails import task4_generate_emails as t4


def main() -> int:
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.resolve()
    outputs = project_root / "outputs"

    web_verified_csv = outputs / 'senior_execs_web_verified.csv'
    osint_top15_csv = outputs / 'step6_osint_top15.csv'

    if not web_verified_csv.exists() or not osint_top15_csv.exists():
        print(f"❌ Missing inputs: ensure both {web_verified_csv.name} and {osint_top15_csv.name} exist.")
        print("Run web verification (main_task3b3c_web_verify.py) and OSINT (main_task6_osint_youtube.py) first.")
        return 1

    df_web = pd.read_csv(web_verified_csv)
    df_osint = pd.read_csv(osint_top15_csv)

    # Combine, prioritize web-verified, and select top 50 unique execs
    df_web['VerificationSource'] = 'Website'
    df_osint['VerificationSource'] = 'OSINT'

    df_combined = pd.concat([df_web, df_osint], ignore_index=True)
    df_combined.drop_duplicates(subset=['Name', 'Company'], keep='first', inplace=True)
    df_top50 = df_combined.head(50).copy()

    print(f"✓ Assembled a top {len(df_top50)} list from web-verified and OSINT sources.")

    # Generate emails for the final list
    df_final = t4(df_top50)

    # Save the final deliverable with a clear name
    final_csv = outputs / 'final_top50_execs_with_emails.csv'
    df_final.to_csv(final_csv, index=False)

    print(f"\n✓ Final deliverable created: {final_csv}")
    print("This file contains the top 50 senior executives with 2 likely email addresses.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
