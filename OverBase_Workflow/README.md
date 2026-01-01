# OverBase Data Cleaning Workflow

This workflow processes a list of CMO/executive video participants to create a cleaned, validated list of 50 senior executives with company websites and email addresses.

## Project Structure

```
OverBase_Workflow/
├── data/
│   └── cmo_videos_names.csv                       # Original raw data
├── outputs/
│   ├── final_cleaned_data.csv                     # After initial import/cleanup
│   ├── senior_execs_only.csv                      # After filtering senior roles
│   ├── senior_execs_no_duplicates.csv             # After removing duplicates
│   ├── senior_execs_validated.csv                 # After company validation
│   ├── step1_senior.csv                           # Artifact: step 1
│   ├── step2_dedup.csv                            # Artifact: step 2
│   ├── step3_domains.csv                          # Artifact: step 3
│   ├── step3b_verified.csv                        # Artifact: step 3b (employment verification)
│   ├── step4_emails.csv                           # Artifact: step 4
│   ├── senior_execs_with_emails.csv               # After email generation
│   ├── final_executives_list.csv                  # Final output (50 execs)
│   ├── quality_report.txt                         # Quality check report
│   ├── email_patterns_used.csv                    # Email pattern documentation
│   ├── logs/
│   │   └── workflow.log                           # Structured log
│   └── manual/
│       ├── verification_template.csv              # Template to fill
│       └── verification_overrides.csv             # Your filled overrides (optional)
├── scripts/
│   ├── initial_cleanup/
│   │   └── initial_cleanup.py                     # Initial data loading/cleanup
│   ├── filters/
│   │   ├── task1_filter_senior_execs.py           # Filter senior execs
│   │   ├── task2_remove_duplicates.py             # Remove duplicates
│   │   ├── task3_validate_companies.py            # Validate companies & websites
│   │   ├── task3b_verify_employment.py            # Verify employment (semi-manual)
│   │   ├── task4_generate_emails.py               # Generate email addresses
│   │   └── task5_quality_check.py                 # Quality checks & final 50
│   └── main_workflow.py                           # Main orchestration script
├── requirements.txt                               # Python dependencies
└── README.md                                      # This file
```

## Prerequisites

- Python 3.8+
- pip (Python package installer)

## Installation

1. Install required dependencies:
```bash
pip install -r requirements.txt
```

## Workflow Steps

### Step 1: Initial Data Load (automatic)

Running the main workflow will load and clean `data/cmo_videos_names.csv` using `scripts/initial_cleanup/initial_cleanup.py` and write `outputs/final_cleaned_data.csv`.

### Step 2: Run Complete Workflow

Run all tasks in sequence using the main workflow script:
```bash
python scripts/main_workflow.py
```

Or run individual tasks manually:

#### Task 1: Filter Senior Executives
Filters the list to only include senior executive roles (C-level, VP, SVP, EVP, Director, Head, President, Founder, etc.)
```bash
python scripts/filters/task1_filter_senior_execs.py
```

#### Task 2: Remove Duplicates
Removes duplicate entries based on normalized name and company matching
```bash
python scripts/filters/task2_remove_duplicates.py
```

#### Task 3: Validate Companies
For each executive, finds and validates the company's official website. Adds Domain Notes and Confidence.
```bash
python scripts/filters/task3_validate_companies.py
```

**Note:** This step requires internet access and may take several minutes as it validates each company website.

#### Task 3b: Verify Employment (semi-manual)
Generates a LinkedIn search URL for each person and prepares verification columns.

Artifacts written:
- `outputs/manual/verification_template.csv` – fill `Employment Verified` (yes/no), `Verification Source` (profile/search URL), and `Verified At` (YYYY-MM-DD) per person.
- Optionally save your edits as `outputs/manual/verification_overrides.csv` to apply verification on the next run.

```bash
python scripts/filters/task3b_verify_employment.py
```

#### Task 4: Generate Email Addresses
Generates multiple patterns and records the two most likely (domain-aware). Also logs patterns tried per person.
```bash
python scripts/filters/task4_generate_emails.py
```

#### Task 5: Quality Check & Final Output
Enforces an exact target of 50 executives using tiered selection:
- strict: verified employment + 2 emails + website
- A: verified + ≥1 email + website
- B: verified + ≥1 email (website optional)
- C: unverified + 2 emails + website
- fallback: any with ≥1 email

Produces `final_executives_list.csv` and `quality_report.txt` with tier breakdown and any issues.
```bash
python scripts/filters/task5_quality_check.py
```

## Output Files

### Final Output: `outputs/final_executives_list.csv`

Columns:
- **Name**, **Title**, **Company**, **Company Website**
- **Verification Source**, **Employment Verified**, **Verified At**, **LinkedIn Search URL**
- **Candidate Email 1**, **Candidate Email 2**, **Quality Tier**, **Source**, **Domain Notes**, **Confidence**

### Supporting Files

- **quality_report.txt**: Detailed quality check report
- **email_patterns_used.csv**: Documentation of email patterns used for each executive

## Task Details

### Task 1: Filter Senior Executives

**Criteria for Senior Executives:**
- C-level roles: CEO, CMO, CFO, CTO, COO, Chief [anything]
- VP roles: VP, Vice President, SVP, EVP
- Director roles (excluding junior directors)
- Head roles
- President
- Founder
- Managing Director

**Exclusions:**
- Junior roles (Manager, Coordinator, Specialist, Analyst, Associate, Assistant) - unless they have senior modifiers
- Entries with "(Title not stated)" or "(Title not specified)"

### Task 2: Remove Duplicates

**Strategy:**
- Normalizes names and company names for comparison
- Removes extra spaces, quotes, and special characters
- Keeps the entry with the most complete information
- Uses normalized name + company as the duplicate key

### Task 3: Validate Companies

**Process:**
1. Cleans company names (removes extra info, parentheticals)
2. Uses a mapping of known company names to domains
3. Attempts to construct likely domains from company names
4. Validates websites by checking HTTP response
5. Records the source URL for each validation

**Note:** This requires internet connectivity and may take time. The script includes delays between requests to be respectful.

### Task 4: Generate Email Addresses

**Email Patterns:**
- **Pattern 1**: `first.last@domain.com` (most common)
- **Pattern 2**: `f.last@domain.com` (first initial + last name)

**Process:**
1. Extracts domain from company website URL
2. Normalizes executive name (removes nicknames, quotes, etc.)
3. Generates both email patterns
4. Documents patterns used in `email_patterns_used.csv`

### Task 5: Quality Check

**Checks Performed:**
1. Ensures all required columns exist
2. Filters entries missing critical information
3. Ensures exactly 50 executives (or as many as available)
4. Validates email format consistency
5. Checks domain consistency between website and emails
6. Creates final sorted output

## Success Criteria

The final output should meet these criteria:
- ✅ Contains 50 senior executives (or maximum available if fewer)
- ✅ Each executive has 2 candidate email addresses
- ✅ Each executive has a validated company website
- ✅ Each executive has a source URL for validation
- ✅ All emails follow valid format patterns
- ✅ Domain consistency between company website and emails

## Notes for Loom Video Recording

When recording the Loom video, make sure to:
1. Show the complete workflow running from start to finish
2. Demonstrate each task's execution and output
3. Show validation of company websites (Task 3) and employment verification (Task 3b)
4. Show email generation process (Task 4) and patterns tried log
5. Show quality check results (Task 5) including tier composition
6. Display the final output CSV file and logs in outputs/logs/workflow.log
7. Verify all 50 executives are present (or document shortfall and why) with complete data

## Troubleshooting

### Issue: Script fails with "file not found"
- **Solution**: Make sure you're running scripts from the project root directory, or use the absolute path structure

### Issue: Task 3 takes too long or fails
- **Solution**: Some company websites may not be accessible. The script will continue with available data. You may need to manually research some companies.

### Issue: Less than 50 executives in final output
- **Solution**: This means fewer than 50 executives met all quality criteria. Check the quality_report.txt for details. You may need to adjust filtering criteria or manually validate more companies.

### Issue: Email generation fails
- **Solution**: Ensure company websites are properly validated first (Task 3). Check that names are properly formatted in the input data.

## License

This project is for the OverBase data cleaning assignment.

