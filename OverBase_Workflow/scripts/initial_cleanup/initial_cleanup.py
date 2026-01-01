"""
Initial Data Cleanup - Data Loading Function

This module provides the load_and_clean_data function for loading CSV files
and handling bad rows gracefully.
"""

import pandas as pd
from pathlib import Path


def load_and_clean_data(csv_path: Path) -> pd.DataFrame:
    """
    Load CSV file and handle bad rows gracefully.
    
    Parameters
    ----------
    csv_path : Path
        Path to input CSV file
        
    Returns
    -------
    pd.DataFrame
        Loaded and cleaned DataFrame
    """
    bad_rows = []
    row_counter = 0
    
    def bad_line_handler(line):
        nonlocal row_counter
        bad_rows.append((row_counter, line))
        row_counter += 1
        return None
    
    # Read CSV with error handling
    try:
        df = pd.read_csv(
            str(csv_path),
            engine="python",
            on_bad_lines=bad_line_handler
        )
    except FileNotFoundError:
        raise FileNotFoundError(f"Input file not found: {csv_path}")
    
    df["_row_order"] = range(len(df))
    
    # Fix bad rows
    fixed_bad_rows = []
    for order, row in bad_rows:
        if len(row) >= 5:
            name = row[0]
            title = row[1]
            company = str(row[2]).strip() if len(row) > 2 else ""
            extra = str(row[3]).strip() if len(row) > 3 else ""
            url = row[4] if len(row) > 4 else ""
            
            # Normalize empties
            company = "" if company.lower() == "nan" else company
            extra = "" if extra.lower() == "nan" else extra
            
            # Apply rules
            if company and extra:
                final_company = f"{company} — {extra}"
            elif company:
                final_company = company
            elif extra:
                final_company = extra
            else:
                final_company = ""
            
            fixed_bad_rows.append([
                name,
                title,
                final_company,
                url,
                order
            ])
    
    if fixed_bad_rows:
        fixed_bad_df = pd.DataFrame(
            fixed_bad_rows,
            columns=["Name", "Title", "Company", "Youtube URL", "_row_order"]
        )
        df = pd.concat([df, fixed_bad_df], ignore_index=True)
    
    # Restore order
    df = df.sort_values("_row_order").drop(columns="_row_order")
    
    return df
