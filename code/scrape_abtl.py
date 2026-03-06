"""
scrape_abtl.py
==============
Scrapes all biographical records from the ÁBTL Archontológia database.
URL pattern: https://www.abtl.hu/ords/archontologia/f?p=108:5:::::P5_PRS_ID:{id}

Strategy: 
- The list page shows ~940 records with PRS_IDs as links
- Scrape the index pages to get all PRS_IDs
- Then fetch each individual record

Output:
- abtl_raw.json         — all raw records
- abtl_postings.csv     — city x year postings (one row per posting)
- abtl_officers.csv     — officer-level summary
"""

import requests, time, re, json, csv
from bs4 import BeautifulSoup
from pathlib import Path

BASE = "https://www.abtl.hu/ords/archontologia/f"
SESSION_PARAM = ""  # will be extracted from first page

HEADERS = {
    "User-Agent": "Mozilla/5.0 (research scraper; academic use)",
    "Accept-Language": "hu,en;q=0.9",
}

def get_session():
    """Get a session ID from the index page."""
    r = requests.get(f"{BASE}?p=108:4::::4::", headers=HEADERS, timeout=15)
    # extract session from URLs like f?p=108:4:4150828475788::...
    m = re.search(r'f\?p=108:4:(\d+)::', r.text)
    if m:
        return m.group(1)
    return ""

def get_all_prs_ids(session):
    """Scrape all PRS_IDs from the index (paginated, 15 per page)."""
    prs_ids = []
    page = 1
    min_row = 1
    
    while True:
        if min_row == 1:
            url = f"{BASE}?p=108:4:{session}::NO:RP,4:P4_SEARCH_INDEX:"
        else:
            url = f"{BASE}?p=108:4:{session}:pg_R_40092188966690279:NO&pg_min_row={min_row}&pg_max_rows=15&pg_rows_fetched=15"
        
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # find all links with P5_PRS_ID
        links = soup.find_all('a', href=re.compile(r'P5_PRS_ID:(\d+)'))
        if not links:
            break
            
        new_ids = []
        for link in links:
            m = re.search(r'P5_PRS_ID:(\d+)', link['href'])
            if m:
                pid = int(m.group(1))
                if pid not in prs_ids:
                    new_ids.append(pid)
                    prs_ids.append(pid)
        
        print(f"  Page {page}: found {len(new_ids)} new IDs (total: {len(prs_ids)})")
        
        if not new_ids:
            break
            
        min_row += 15
        page += 1
        time.sleep(0.5)
    
    return prs_ids

def parse_record(html, prs_id):
    """Parse a single officer record page."""
    soup = BeautifulSoup(html, 'html.parser')
    
    record = {'prs_id': prs_id, 'name': None, 'mother': None, 
              'birth_date': None, 'birth_place': None,
              'death_date': None, 'notes': None,
              'education': [], 'ranks': [], 'postings': [], 'sources': []}
    
    # name from title
    title = soup.find('title')
    if title:
        name = title.text.replace('ÁBTL Archontológia -', '').replace('életrajzi adatok', '').strip()
        record['name'] = name
    
    # parse tables
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 2:
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)
                
                if 'Anyja neve' in label:
                    record['mother'] = value
                elif 'Született' in label:
                    # format: 1942.09.18., Nagykamarás
                    parts = value.split(',', 1)
                    record['birth_date'] = parts[0].strip()
                    if len(parts) > 1:
                        record['birth_place'] = parts[1].strip()
                elif 'Meghalt' in label:
                    record['death_date'] = value
                elif 'Megjegyzés' in label:
                    record['notes'] = value
    
    # education - look for year patterns after school names
    # ranks and postings are in structured tables
    # Find the sections by header text
    all_text = soup.get_text()
    
    # Extract postings section - between "Foglalkozások, beosztások:" and "Források:"
    posting_match = re.search(
        r'Foglalkozások, beosztások:(.*?)(?:Források:|$)', 
        all_text, re.DOTALL
    )
    if posting_match:
        posting_text = posting_match.group(1)
        # Each posting: institution, years, role
        # Look for year patterns like 1964.06-1964.12 or 1964-1970
        lines = [l.strip() for l in posting_text.split('\n') if l.strip()]
        
        i = 0
        while i < len(lines):
            line = lines[i]
            # year pattern
            year_match = re.search(r'(\d{4}(?:\.\d{2})?)\s*[-–]\s*(\d{4}(?:\.\d{2})?|)', line)
            if year_match or re.match(r'\d{4}\.\d{2}\.\d{2}', line):
                # this line has years - it's part of a posting
                # institution is previous non-empty line
                institution = lines[i-1] if i > 0 else ''
                years = line
                role = lines[i+1] if i+1 < len(lines) else ''
                
                # extract city from institution name
                city = extract_city(institution)
                
                # extract year range
                y_start, y_end = parse_years(years)
                
                if institution and y_start:
                    record['postings'].append({
                        'institution': institution,
                        'city': city,
                        'year_start': y_start,
                        'year_end': y_end,
                        'role': role,
                        'raw_years': years,
                    })
            i += 1
    
    # rank section
    rank_match = re.search(
        r'Rendfokozatok:(.*?)(?:Foglalkozások|$)',
        all_text, re.DOTALL
    )
    if rank_match:
        rank_text = rank_match.group(1)
        lines = [l.strip() for l in rank_text.split('\n') if l.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            year_match = re.search(r'(\d{4})\s*[-–]\s*(\d{4})', line)
            if year_match:
                rank = lines[i-1] if i > 0 else ''
                branch = lines[i+1] if i+1 < len(lines) else ''
                record['ranks'].append({
                    'rank': rank,
                    'year_start': int(year_match.group(1)),
                    'year_end': int(year_match.group(2)),
                    'branch': branch,
                })
            i += 1
    
    return record

CITY_PATTERNS = [
    # explicit city mentions in institution names
    (r',\s*([A-ZÁÉÍÓÖŐÚÜŰ][a-záéíóöőúüű]+)$', 1),  # ends with city
]

KNOWN_CITIES = [
    'Budapest', 'Miskolc', 'Debrecen', 'Győr', 'Pécs', 'Szeged', 'Kecskemét',
    'Székesfehérvár', 'Szolnok', 'Veszprém', 'Kaposvár', 'Eger', 'Zalaegerszeg',
    'Szombathely', 'Nyíregyháza', 'Tatabánya', 'Sopron', 'Békéscsaba',
    'Salgótarján', 'Dunaújváros', 'Esztergom', 'Hódmezővásárhely',
    'Taszár', 'Pápa', 'Nagykanizsa', 'Szekszárd', 'Érd',
]

def extract_city(institution):
    """Extract city name from institution string."""
    if not institution:
        return None
    
    # check known cities
    for city in KNOWN_CITIES:
        if city in institution:
            return city
    
    # BM central = Budapest
    if any(x in institution for x in ['BM III', 'BM II', 'Belügyminisztérium', 
                                        'Legfőbb', 'Minisztertanács', 'MSZMP KB']):
        return 'Budapest'
    
    # extract after last comma
    m = re.search(r',\s*([A-ZÁÉÍÓÖŐÚÜŰ][a-záéíóöőúüű]+)\s*$', institution)
    if m:
        return m.group(1)
    
    return None

def parse_years(text):
    """Extract year_start, year_end from text like '1964.06-1964.12' or '1982-1989'."""
    m = re.search(r'(\d{4})(?:\.\d{2})?\s*[-–]\s*(\d{4})(?:\.\d{2})?', text)
    if m:
        return int(m.group(1)), int(m.group(2))
    m = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', text)
    if m:
        return int(m.group(1)), int(m.group(1))
    return None, None

def scrape_record(prs_id):
    url = f"{BASE}?p=108:5:::::P5_PRS_ID:{prs_id}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return parse_record(r.text, prs_id)
    except Exception as e:
        print(f"  Error {prs_id}: {e}")
    return None

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--ids_file', default=None, help='JSON file with PRS IDs (skip index scrape)')
    parser.add_argument('--limit', type=int, default=None, help='Limit records for testing')
    parser.add_argument('--output_dir', default='./abtl_data')
    args = parser.parse_args()
    
    out = Path(args.output_dir)
    out.mkdir(exist_ok=True)
    
    # Step 1: get all IDs
    ids_path = out / 'prs_ids.json'
    if args.ids_file:
        with open(args.ids_file) as f:
            prs_ids = json.load(f)
        print(f"Loaded {len(prs_ids)} IDs from {args.ids_file}")
    elif ids_path.exists():
        with open(ids_path) as f:
            prs_ids = json.load(f)
        print(f"Loaded {len(prs_ids)} cached IDs")
    else:
        print("Scraping index pages for PRS IDs...")
        session = get_session()
        print(f"Session: {session}")
        prs_ids = get_all_prs_ids(session)
        with open(ids_path, 'w') as f:
            json.dump(prs_ids, f)
        print(f"Saved {len(prs_ids)} IDs")
    
    if args.limit:
        prs_ids = prs_ids[:args.limit]
    
    # Step 2: scrape individual records
    records = []
    raw_path = out / 'abtl_raw.json'
    
    # load existing if resuming
    existing = {}
    if raw_path.exists():
        with open(raw_path) as f:
            for rec in json.load(f):
                existing[rec['prs_id']] = rec
        print(f"Resuming: {len(existing)} already scraped")
    
    for i, pid in enumerate(prs_ids):
        if pid in existing:
            records.append(existing[pid])
            continue
        
        print(f"  [{i+1}/{len(prs_ids)}] PRS_ID {pid}...")
        rec = scrape_record(pid)
        if rec:
            records.append(rec)
            existing[pid] = rec
        
        # save every 50
        if (i+1) % 50 == 0:
            with open(raw_path, 'w', encoding='utf-8') as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  Checkpoint saved ({len(records)} records)")
        
        time.sleep(0.3)  # polite delay
    
    with open(raw_path, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(records)} raw records")
    
    # Step 3: flatten to CSVs
    all_postings = []
    all_officers = []
    
    for rec in records:
        all_officers.append({
            'prs_id':      rec['prs_id'],
            'name':        rec.get('name'),
            'birth_date':  rec.get('birth_date'),
            'birth_place': rec.get('birth_place'),
            'death_date':  rec.get('death_date'),
            'n_postings':  len(rec.get('postings', [])),
            'n_ranks':     len(rec.get('ranks', [])),
        })
        
        for p in rec.get('postings', []):
            all_postings.append({
                'prs_id':      rec['prs_id'],
                'name':        rec.get('name'),
                'institution': p.get('institution'),
                'city':        p.get('city'),
                'year_start':  p.get('year_start'),
                'year_end':    p.get('year_end'),
                'role':        p.get('role'),
            })
    
    def write_csv(rows, path):
        if not rows: return
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader(); w.writerows(rows)
        print(f"Saved {len(rows)} rows → {path}")
    
    write_csv(all_officers, out / 'abtl_officers.csv')
    write_csv(all_postings, out / 'abtl_postings.csv')
    
    # quick city summary
    from collections import Counter
    cities_1985 = [p['city'] for p in all_postings 
                   if p.get('city') and p.get('year_start') and p.get('year_end')
                   and p['year_start'] <= 1989 and p['year_end'] >= 1985]
    print(f"\nCities with officers active 1985-1989: {len(set(cities_1985))}")
    print("Top cities:")
    for city, n in Counter(cities_1985).most_common(20):
        print(f"  {n:4d}  {city}")

