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
