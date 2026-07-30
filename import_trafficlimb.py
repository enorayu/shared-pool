"""
Import TraffiClimb supplier CSV into quote_pool via Supabase REST API.
Handles multiline CSV, cleans domains, deduplicates by email.
"""
import csv
import re
import time
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import SUPABASE_URL, SUPABASE_ANON_KEY
import io

try:
    import requests
except ImportError:
    os.system(f"{sys.executable} -m pip install requests -q")
    import requests

API = f"{SUPABASE_URL}/rest/v1"
HEADERS = {
    "apikey": SUPABASE_ANON_KEY,
    "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

BATCH_SIZE = 30

def clean_domain(raw):
    """Extract clean domain from messy Domain field"""
    raw = (raw or "").strip().lower()
    # Remove protocol
    raw = re.sub(r'^https?://', '', raw)
    # Remove www.
    raw = re.sub(r'^www\.', '', raw)
    # Remove trailing slash and path
    raw = raw.split('/')[0].split('?')[0].split('#')[0]
    # Remove text before domain (eg "News / Magazine Websites\nhello.com")
    # Find first valid domain-like pattern
    match = re.search(r'[a-z0-9][a-z0-9.-]*\.[a-z]{2,}', raw)
    if match:
        return match.group(0)
    return raw

def extract_domains(raw):
    """Extract all domain names from a messy field, return list"""
    raw = (raw or "").strip()
    domain_pattern = re.compile(r'[a-z0-9][a-z0-9.-]*\.[a-z]{2,}', re.IGNORECASE)
    return domain_pattern.findall(raw)

def extract_contact_email(raw):
    """Extract email from messy contact field"""
    raw = (raw or "").strip()
    # Email pattern
    email_match = re.search(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', raw)
    if email_match:
        return email_match.group(0).lower()
    return raw.lower() if '@' in raw else ""

def parse_csv():
    path = r"C:\Users\admin\TraffiClimb供应商信息表.csv"
    with open(path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    print(f"Parsed {len(rows)} rows from CSV")
    return rows

def map_row(row):
    """Map CSV row to quote_pool record"""
    name = (row.get('Name', '') or '').strip()
    domain_raw = (row.get('Domain', '') or '').strip()
    site_type = (row.get('Site Type', '') or '').strip()
    country = (row.get('Counrty / Language', '') or '').strip()
    da = (row.get('DA', '') or '').strip()
    collab = (row.get('Collaboration Types', '') or '').strip()
    pricing = (row.get('Pricing', '') or '').strip()
    tat = (row.get('Turnaround Time', '') or '').strip()
    content_reqs = (row.get('Content/Link Requirements', '') or '').strip()
    pub_guide = (row.get('Publishing Guidelines', '') or '').strip()
    contact_raw = (row.get('Contact Email / other', '') or '').strip()
    payment = (row.get('Payment methods', '') or '').strip()
    notes = (row.get('Notes', '') or '').strip()
    multi = (row.get('Multiple Sites', '') or '').strip()
    site_count = (row.get('站点数量', '') or '').strip()
    nature = (row.get('性质', '') or '').strip()

    # Clean domain - sometimes has multiple domains separated by newlines
    domains = extract_domains(domain_raw)
    if not domains:
        return None  # Skip rows without valid domain
    
    primary_domain = domains[0]
    
    # Contact email
    contact_email = extract_contact_email(contact_raw)
    if not contact_email and '@' not in contact_raw:
        # Try to find email in Notes or other fields
        all_text = f"{contact_raw} {notes}"
        contact_email = extract_contact_email(all_text)
    if not contact_email:
        contact_email = f"unknown@{primary_domain}"
    
    # Build notes
    full_notes = notes
    if multi and multi.strip().lower() != 'no':
        full_notes += f" | Multiple Sites: {multi}"
    if site_count:
        full_notes += f" | Sites: {site_count}"
    if nature and nature.strip().lower() != '供应商':
        full_notes += f" | Type: {nature}"
    
    # DA as traffic
    da_val = da if da else ''
    
    record = {
        "domain": primary_domain,
        "supplier": name,
        "contact_email": contact_email,
        "email": contact_email,
        "site_category": site_type,
        "country": country,
        "traffic": da_val,
        "cooperation_type": collab,
        "price": pricing,
        "tat": tat,
        "requirements": content_reqs,
        "link_rules": pub_guide,
        "payment": payment,
        "status": "New",
        "priority": 0,
        "notes": full_notes[:500] if full_notes else "",
        "discovered_by": "enora",
        "discovered_at": time.strftime("%Y-%m-%dT%H:%M:%S.000+00:00", time.gmtime()),
    }
    return record

def import_records(records):
    """Batch insert records into quote_pool via API"""
    # First, get existing emails for dedup
    print("Fetching existing emails for dedup...")
    existing = set()
    page = 0
    while True:
        resp = requests.get(
            f"{API}/quote_pool?select=email&limit=1000&offset={page*1000}",
            headers={"apikey": SUPABASE_ANON_KEY},
            timeout=30
        )
        if resp.status_code != 200:
            print(f"  Warning: status {resp.status_code}")
            break
        data = resp.json()
        if not data:
            break
        for r in data:
            if r.get('email'):
                existing.add(r['email'].lower())
        if len(data) < 1000:
            break
        page += 1
    
    print(f"  Found {len(existing)} existing emails")
    
    # Filter out duplicates
    new_records = []
    deduped = 0
    for r in records:
        email = r.get('email', '').lower()
        key = f"{email}|{r.get('domain','')}"
        if email in existing:
            deduped += 1
            continue
        existing.add(email)
        new_records.append(r)
    
    print(f"  After dedup: {len(new_records)} new records (skipped {deduped})")
    
    # Batch insert
    total = len(new_records)
    imported = 0
    for i in range(0, total, BATCH_SIZE):
        batch = new_records[i:i+BATCH_SIZE]
        for attempt in range(3):
            try:
                resp = requests.post(
                    f"{API}/quote_pool",
                    headers={**HEADERS, "Prefer": "return=minimal"},
                    json=batch,
                    timeout=30
                )
                if resp.status_code in (200, 201, 204):
                    imported += len(batch)
                    print(f"  Batch {i//BATCH_SIZE+1}: {imported}/{total}")
                    break
                else:
                    print(f"  Error batch {i//BATCH_SIZE+1}: {resp.status_code} {resp.text[:200]}")
                    if attempt < 2:
                        time.sleep(2)
            except Exception as e:
                print(f"  Exception batch {i//BATCH_SIZE+1}: {e}")
                if attempt < 2:
                    time.sleep(2)
        time.sleep(0.5)
    
    return imported, deduped

def main():
    print("=== TraffiClimb CSV → Quote Pool Import ===\n")
    
    rows = parse_csv()
    
    # Map rows to records, handling multi-domain entries
    records = []
    multi_domain_count = 0
    for row in rows:
        domain_raw = (row.get('Domain', '') or '').strip()
        domains = extract_domains(domain_raw)
        
        if not domains:
            print(f"  SKIP (no domain): {row.get('Name','?')[:30]}")
            continue
        
        if len(domains) > 1:
            multi_domain_count += 1
            # For multi-domain entries, create separate record for each domain
            # that gets its own entry
            base_rec = map_row(row)
            if base_rec:
                records.append(base_rec)
                # Add additional domain entries with same info but different domain
                for extra_domain in domains[1:]:
                    rec = dict(base_rec)
                    rec['domain'] = extra_domain
                    rec['email'] = rec['contact_email']  # keep same contact
                    records.append(rec)
        else:
            base_rec = map_row(row)
            if base_rec:
                records.append(base_rec)
    
    print(f"\nMapped {len(records)} total records ({multi_domain_count} multi-domain entries expanded)")
    
    if not records:
        print("No valid records to import!")
        return
    
    print(f"\nSample records:")
    for r in records[:3]:
        print(f"  {r['domain']:30s} | {r['supplier'][:20]:20s} | {r['site_category'][:15]:15s} | {r['country']:10s} | DA={r['traffic']} | {r['cooperation_type'][:20]:20s}")
    
    print(f"\nImporting {len(records)} records...")
    imported, skipped = import_records(records)
    
    print(f"\nDone! Imported: {imported}, Duplicates skipped: {skipped}")

if __name__ == '__main__':
    main()
