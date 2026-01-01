#!/usr/bin/env python3

from pathlib import Path
import pandas as pd
import re
import requests
import time

# Get project root directory and ensure outputs dir exists
script_dir = Path(__file__).parent
scripts_dir = script_dir.parent
PROJECT_ROOT = scripts_dir.parent.resolve()
OUTPUT_DIR = PROJECT_ROOT / "outputs"
LOGS_DIR = OUTPUT_DIR / "logs"
for p in [OUTPUT_DIR, LOGS_DIR]:
    p.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOGS_DIR / "workflow.log"


def log(message: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"[task3_validate_companies] {message}\n")

def task3_validate_companies(df):
    """Task 3: Validate companies and find websites"""
    print("\n" + "=" * 70)
    print("▶ Task 3: Validate Companies")
    print("=" * 70)
    
    REQUEST_TIMEOUT = 10
    REQUEST_DELAY = 1
    USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    
    COMPANY_DOMAIN_MAP = {
        # Merged: existing entries plus user-provided authoritative mappings
        'inseego': 'inseego.com',
        'infineon': 'infineon.com',
        'aws': 'aws.amazon.com',
        'world surf league': 'worldsurfleague.com',
        'allcloud': 'allcloud.io',
        'honeycomb.io': 'honeycomb.io',
        'spacelift': 'spacelift.io',
        'fabrix.ai': 'fabrix.ai',
        'vultr': 'vultr.com',
        'ge aerospace': 'geaerospace.com',
        'auditboard': 'auditboard.com',
        'uipath': 'uipath.com',
        'salt security': 'salt.security',
        'ebay': 'ebay.com',
        'crowdstrike': 'crowdstrike.com',
        'tensor': 'tensorsecurity.com',
        'ddn': 'ddn.com',
        'redline advisors': 'redlineadvisors.com',
        'dynatrace': 'dynatrace.com',
        'ledger': 'ledger.com',
        'google cloud': 'cloud.google.com',
        'heroku': 'heroku.com',
        'triptych info': 'triptychinfo.com',
        'spectra logic': 'spectralogic.com',
        'infinidat': 'infinidat.com',
        'index engines': 'indexengines.com',
        'thecube research': 'thecuberesearch.com',
        'kiteworks': 'kiteworks.com',
        'equinix': 'equinix.com',
        'couchbase': 'couchbase.com',
        'broadforward': 'broadforward.com',
        'cato networks': 'catonetworks.com',
        'stackpane': 'stackpane.com',
        'neo4j': 'neo4j.com',
        'arrcus': 'arrcus.com',
        'adobe enterprise': 'adobe.com',
        'amd': 'amd.com',
        'denexus': 'denexus.io',
        'applied intuition': 'appliedintuition.com',
        'scaleflux': 'scaleflux.com',
        'nutanix': 'nutanix.com',
        'cerebras': 'cerebras.net',
        'transcarent': 'transcarent.com',
        'airmdr': 'airmdr.com',
        'at-bay': 'at-bay.com',
        'typeface': 'typeface.ai',
        'arm': 'arm.com',
        'early growth advisory': 'earlygrowthadvisory.com',
        'together ai': 'together.ai',
        'groq': 'groq.com',
        'ingram micro cloud': 'ingrammicrocloud.com',
        'deloitte': 'deloitte.com',
        'logicmonitor': 'logicmonitor.com',
        'escala 24x7': 'escala24x7.com',
        'commercetools': 'commercetools.com',
        'prophix': 'prophix.com',
        'netapp': 'netapp.com',
        'san francisco 49ers': '49ers.com',
        'boomi': 'boomi.com',
        'sas institute': 'sas.com',
        'ericsson': 'ericsson.com',
        'teradata': 'teradata.com',
        'newtonx': 'newtonx.com',
        'aruba': 'arubanetworks.com',
        'cobalt iron': 'cobaltiron.com',
        'ibm': 'ibm.com',
        'cloudian': 'cloudian.com',
        'forrester research': 'forrester.com',
        'nvidia': 'nvidia.com',
        'idc': 'idc.com',
        'snowflake': 'snowflake.com',
        'qlik': 'qlik.com',
        'chronosphere': 'chronosphere.io',
        'juniper networks': 'juniper.net',
        'dartmouth college': 'dartmouth.edu',
        'intel': 'intel.com',
        'hpe': 'hpe.com',
        'impetus technologies': 'impetus.com',
        'zillow': 'zillow.com',
        'informatica': 'informatica.com',
        'cribl': 'cribl.io',
        'mongodb': 'mongodb.com',
        'mitel': 'mitel.com',
        'sdvi corporation': 'sdvi.com',
        'lacework': 'lacework.com',
        'messagebird': 'messagebird.com',
        'datastax': 'datastax.com',
        'releasehub': 'releasehub.com',
        'sisense': 'sisense.com',
        'influxdata': 'influxdata.com',
        'commvault': 'commvault.com',
        'syncreon': 'syncreon.com',
        'veeam': 'veeam.com',
        'explorium': 'explorium.ai',
        'mitchell international': 'mitchell.com',
        'kyndryl': 'kyndryl.com',
        'fortinet': 'fortinet.com',
        'agero': 'agero.com',
        'acoustic': 'acoustic.com',
        'citrix': 'citrix.com',
        'actifio': 'actifio.com',
        'cockroach labs': 'cockroachlabs.com',
        'automation anywhere': 'automationanywhere.com',
        'kenna security': 'kennasecurity.com',
        'cohesity': 'cohesity.com',
        'coupa': 'coupa.com',
        'uniphore': 'uniphore.com',
        'vlocity': 'vlocity.com',
        'splunk': 'splunk.com',
        'acronis': 'acronis.com',
        'smartsheet': 'smartsheet.com',
        'tintri by ddn': 'tintri.com',
        'veritas': 'veritas.com',
        'us signal': 'ussignal.com',
        'sequoia capital': 'sequoiacap.com',
        'tempered networks': 'temperednetworks.com',
        'five9': 'five9.com',
        'keysight': 'keysight.com',
        'tripactions': 'tripactions.com',
        'sciencelogic': 'sciencelogic.com',
        'sap': 'sap.com',
        'alteryx': 'alteryx.com',
        'zerto': 'zerto.com',
        'mirantis': 'mirantis.com',
        'wandisco': 'wandisco.com',
        'tableau': 'tableau.com',
        'rackspace': 'rackspace.com',
        'ge': 'ge.com',
        'servicenow': 'servicenow.com',
        'service now': 'servicenow.com',
        'emc': 'delltechnologies.com',
        'csc': 'dxctechnology.com',
        'hcl': 'hcltech.com',
        'ifs': 'ifs.com',
        'techdivision': 'techdivision.com',
        'gabor shoes': 'gabor.com',
        'new relic': 'newrelic.com',
        'openlink': 'openlinksw.com',
        'softwareone': 'softwareone.com',
        'cyxtera': 'cyxtera.com',
        'druva': 'druva.com',
        'robin.io': 'robin.io',
        'panviva': 'panviva.com',
        'mesosphere': 'd2iq.com',
        'qad': 'qad.com',
        'turbonomic': 'turbonomic.com',
        'igel': 'igel.com',
        'locus robotics': 'locusrobotics.com',
        'marketo': 'marketo.com',
        'zuora': 'zuora.com',
        'attunity': 'attunity.com',
        'verizon': 'verizon.com',
        'qubole': 'qubole.com',
        'sonatype': 'sonatype.com',
        'oracle': 'oracle.com',
        'time warner': 'warnermedia.com',
        'octane ai': 'octaneai.com',
        'redis labs': 'redis.com',
        'avanade': 'avanade.com',
        'ixia': 'ixiacom.com',
        'continuum analytics': 'continuum.io',
        'igneous systems': 'igneous.io',
        'riverbed': 'riverbed.com',
        'noobaa': 'noobaa.io',
        'predix': 'predix.io',
        'talend': 'talend.com',
        'basho': 'basho.com',
        'the clorox company': 'thecloroxcompany.com',
        'cafex': 'cafex.com',
        'local motors': 'localmotors.com',
        'pentaho': 'pentaho.com',
        'atscale': 'atscale.com',
        'tegile': 'tegile.com',
        # Existing entries kept for completeness
        'vmware': 'vmware.com',
        'salesforce': 'salesforce.com',
        'microsoft': 'microsoft.com',
        'adobe': 'adobe.com',
        'palo alto networks': 'paloaltonetworks.com',
        'dell technologies': 'dell.com',
        'dell': 'dell.com',
        'twilio': 'twilio.com',
        'zscaler': 'zscaler.com',
        'mcafee': 'mcafee.com',
    }
    
    def clean_company_name(company):
        """Clean and normalize company name"""
        if pd.isna(company) or company == "":
            return ""
        company = str(company).strip()
        parts = [p.strip() for p in re.split(r'[—\-–]', company) if p and p.strip()]
        if parts:
            company = parts[-1]
        else:
            company = company
        company = re.sub(r'\([^)]*\)', '', company).strip()
        company = ' '.join(company.split())
        return company
    
    def find_company_domain(company_name):
        """Try to find company domain through known mappings"""
        company_lower = company_name.lower()
        for key, domain in COMPANY_DOMAIN_MAP.items():
            if key in company_lower:
                return domain
        return None
    
    def search_company_website(company_name):
        """Search for company website"""
        domain = find_company_domain(company_name)
        if domain:
            return f"https://{domain}"
        return None
    
    def validate_company_website(url):
        """Validate that a company website exists and is accessible"""
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers={'User-Agent': USER_AGENT},
                allow_redirects=True
            )
            if response.status_code == 200:
                return True, response.url
            return False, None
        except Exception:
            return False, None
    
    def validate_executive(row):
        """Validate executive and get company website"""
        company = clean_company_name(row['Company'])
        
        if not company or company.lower() in ['', '(company not stated)', '–']:
            return {
                'Company': company,
                'Company Website': '',
                'Source': 'No company information available',
                'Domain Notes': 'no_company_info',
                'Confidence': 'low'
            }
        
        website = search_company_website(company)
        
        if website:
            is_valid, final_url = validate_company_website(website)
            if is_valid:
                return {
                    'Company': company,
                    'Company Website': final_url,
                    'Source': final_url,
                    'Domain Notes': 'mapped_known_domain',
                    'Confidence': 'high'
                }
        
        domain = find_company_domain(company)
        if domain:
            website = f"https://{domain}"
            is_valid, final_url = validate_company_website(website)
            if is_valid:
                return {
                    'Company': company,
                    'Company Website': final_url,
                    'Source': final_url,
                    'Domain Notes': 'verified_from_mapping',
                    'Confidence': 'high'
                }
        
        company_clean = re.sub(r'[^a-z0-9]', '', company.lower())
        if company_clean:
            likely_domain = f"https://www.{company_clean}.com"
            is_valid, final_url = validate_company_website(likely_domain)
            if is_valid:
                return {
                    'Company': company,
                    'Company Website': final_url,
                    'Source': final_url,
                    'Domain Notes': 'verified_from_slug',
                    'Confidence': 'medium'
                }
        
        return {
            'Company': company,
            'Company Website': '',
            'Source': 'Company website not found - manual research required',
            'Domain Notes': 'not_found',
            'Confidence': 'low'
        }
    
    print(f"Loaded {len(df)} executives for validation")
    
    results = []
    for idx, row in df.iterrows():
        print(f"Validating {idx+1}/{len(df)}: {row['Name']} @ {row['Company']}")
        
        validation_result = validate_executive(row)
        
        row['Company'] = validation_result['Company']
        row['Company Website'] = validation_result['Company Website']
        row['Source'] = validation_result['Source']
        row['Domain Notes'] = validation_result.get('Domain Notes', '')
        row['Confidence'] = validation_result.get('Confidence', '')
        
        results.append(row)
        time.sleep(REQUEST_DELAY)
    
    df_validated = pd.DataFrame(results)
    
    # Persist artifacts
    STEP_CSV = OUTPUT_DIR / "step3_domains.csv"
    OUTPUT_CSV = OUTPUT_DIR / "senior_execs_validated.csv"
    df_validated.to_csv(str(STEP_CSV), index=False)
    df_validated.to_csv(str(OUTPUT_CSV), index=False)
    log(f"Validated companies for {len(df_validated)} rows -> saved {STEP_CSV} and {OUTPUT_CSV}")
    
    print("✔ Task 3 completed")
    print(f"Validated {len(df_validated)} executives with company websites")
    print(f"Saved to: {OUTPUT_CSV}")
    return df_validated
