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
    df = t3b(df)  # writes outputs/step3b_verified.csv

    # 3C (web scrape; honors OVERBASE_SCRAPE_MODE)
    df = t3c(df)  # writes outputs/step3c_verified_web.csv

    # Task 4 (emails)
    df = t4(df)   # writes outputs/senior_execs_with_emails.csv

    # YouTube OSINT scoring and selection artifacts
    t6(df)        # writes outputs/step6_osint_scored.csv and outputs/step6_osint_top15.csv

    # Combine 35 website-verified + best 15 OSINT (>=60), unique companies
    df0 = pd.read_csv(outputs / 'senior_execs_with_emails.csv')
    sc = pd.read_csv(outputs / 'step6_osint_scored.csv')

    ver = df0[df0['Employment Verified'].astype(str).str.lower()=='yes'].copy()
    ver['Confidence'] = '100%'
    if 'Verification Source' not in ver.columns:
        ver['Verification Source'] = ''
    if 'Evidence' not in ver.columns:
        ver['Evidence'] = ''

    cand = sc[(sc['Employment Verified'].astype(str).str.lower()!='yes')
              & (sc['OSINT Confidence'] >= 60)
              & (sc['OSINT Verification Source']=='YouTube')].copy()
    cand = cand.sort_values(['OSINT Confidence','OSINT Video Published'], ascending=[False, False])

    seen=set(); picks=[]
    for _, r in cand.iterrows():
        comp = str(r.get('Company','')).strip().lower()
        if comp in seen:
            continue
        seen.add(comp); picks.append(r)
        if len(picks) >= 15:
            break

    os15 = pd.DataFrame(picks)
    if not os15.empty:
        os15['Verification Source'] = 'YouTube'
        os15['Evidence'] = os15['OSINT Evidence']
        os15['Confidence'] = os15['OSINT Confidence'].map(lambda x: f"{int(x)}%")

    final = pd.concat([ver, os15], ignore_index=True, sort=False)
    out_csv = outputs / 'final_50_with_osint.csv'
    final.to_csv(out_csv, index=False)
    print(f"✓ Wrote {out_csv} with {len(final)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
