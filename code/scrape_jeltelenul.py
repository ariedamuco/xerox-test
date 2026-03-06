"""
scrape_jeltelenul.py
Scrapes case records from https://jeltelenul.hu

Run locally:
    pip install requests beautifulsoup4
    python scrape_jeltelenul.py

Output:
    jeltelenul_cases.csv     — one row per criminal case
    jeltelenul_victims.csv   — one row per victim (linked to case)
"""

import requests
import time
import csv
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://jeltelenul.hu"
DELAY    = 1.5   # seconds between requests — be polite

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (academic research; contact: your@email.com)'
})

def get_soup(url):
    r = session.get(url, timeout=15)
    r.raise_for_status()
    time.sleep(DELAY)
    return BeautifulSoup(r.text, 'html.parser')

# ── Step 1: collect all case URLs ────────────────────────────────
def get_all_case_urls():
    """
    jeltelenul.hu uses Drupal. Cases are at /node/* or slug URLs.
    Try the sitemap first, then fall back to crawling listing pages.
    """
    case_urls = set()

    # Try sitemap
    try:
        soup = get_soup(f"{BASE_URL}/sitemap.xml")
        locs = soup.find_all('loc')
        for loc in locs:
            url = loc.text.strip()
            # Case pages contain 'bunugye' or are under /esetek/
            if 'bunugye' in url or '/esetek/' in url or '/ugy/' in url:
                case_urls.add(url)
        print(f"Sitemap: found {len(case_urls)} case URLs")
    except Exception as e:
        print(f"Sitemap failed: {e}")

    # Try listing pages (Drupal views pagination)
    if len(case_urls) == 0:
        print("Trying listing pages...")
        page = 0
        while True:
            url = f"{BASE_URL}/esetek?page={page}" if page > 0 else f"{BASE_URL}/esetek"
            try:
                soup = get_soup(url)
                links = soup.find_all('a', href=True)
                new = set()
                for a in links:
                    href = a['href']
                    full = urljoin(BASE_URL, href)
                    if 'bunugye' in href or '/ugy/' in href or '/eset/' in href:
                        new.add(full)
                if not new:
                    break
                case_urls.update(new)
                print(f"  Page {page}: {len(new)} new, total {len(case_urls)}")
                page += 1
            except Exception as e:
                print(f"  Page {page} failed: {e}")
                break

    return list(case_urls)


# ── Step 2: parse a single case page ─────────────────────────────
def parse_case(url):
    """
    Extract structured fields from a case page.
    jeltelenul.hu uses labeled divs/spans — adjust field names if needed.
    """
    try:
        soup = get_soup(url)
    except Exception as e:
        print(f"  Failed {url}: {e}")
        return None, []

    case = {'url': url, 'slug': url.rstrip('/').split('/')[-1]}
    victims = []

    # Title / case name
    title = soup.find('h1') or soup.find('h2')
    case['title'] = title.get_text(strip=True) if title else ''

    # Look for labeled field rows — Drupal typically uses
    # <div class="field-label">Label</div><div class="field-items">Value</div>
    # or <span class="field-label">
    field_map = {
        'nyomozó szerv':       'investigating_organ',
        'ügyész':              'prosecutor',
        'bíróság':             'court',
        'bíró':                'judge',
        'ítélet':              'verdict',
        'büntetés':            'sentence',
        'kivégzés':            'execution',
        'kulcsszavak':         'keywords',
        'forrás':              'source',
        'iratszám':            'file_number',
        'helyszín':            'location',
        'település':           'settlement',
        'megye':               'county',
        'dátum':               'date',
        'tárgyalás':           'hearing_date',
    }

    # Generic field extraction
    labels = soup.find_all(class_=re.compile(r'field-label|label'))
    for label_el in labels:
        label_text = label_el.get_text(strip=True).rstrip(':').lower()
        for hu, en in field_map.items():
            if hu in label_text:
                # Value is usually the next sibling or parent's next element
                val_el = label_el.find_next_sibling()
                if val_el is None:
                    val_el = label_el.parent.find_next_sibling()
                if val_el:
                    case[en] = val_el.get_text(separator=' ', strip=True)
                break

    # Victims: usually listed in a table or repeated field group
    # Look for rows with names + birth dates
    victim_rows = soup.find_all(class_=re.compile(r'views-row|field-collection|victim|aldozat'))
    for row in victim_rows:
        name_el = row.find(class_=re.compile(r'name|nev'))
        date_el = row.find(class_=re.compile(r'date|datum|szulet'))
        place_el = row.find(class_=re.compile(r'place|hely|szulet.*hely'))
        if name_el:
            victims.append({
                'case_url':    url,
                'case_slug':   case['slug'],
                'name':        name_el.get_text(strip=True),
                'birth_date':  date_el.get_text(strip=True)  if date_el  else '',
                'birth_place': place_el.get_text(strip=True) if place_el else '',
            })

    # Fallback: grab all text mentioning settlement names
    # (useful if structure differs from expectations)
    full_text = soup.get_text(separator='\n')
    case['full_text'] = full_text[:3000]  # first 3000 chars for inspection

    return case, victims


# ── Step 3: run ───────────────────────────────────────────────────
if __name__ == '__main__':
    print("Step 1: collecting case URLs...")
    urls = get_all_case_urls()
    print(f"Total case URLs: {len(urls)}")

    if not urls:
        print("No URLs found — check site structure manually and adjust get_all_case_urls()")
        exit(1)

    print(f"\nStep 2: scraping {len(urls)} case pages...")
    all_cases   = []
    all_victims = []

    for i, url in enumerate(urls):
        print(f"  [{i+1}/{len(urls)}] {url}")
        case, victims = parse_case(url)
        if case:
            all_cases.append(case)
            all_victims.extend(victims)

    print(f"\nStep 3: saving...")

    # Cases
    if all_cases:
        case_fields = list(dict.fromkeys(
            k for c in all_cases for k in c.keys() if k != 'full_text'
        ))
        with open('jeltelenul_cases.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=case_fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(all_cases)
        print(f"Saved jeltelenul_cases.csv ({len(all_cases)} cases)")

    # Victims
    if all_victims:
        with open('jeltelenul_victims.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['case_url','case_slug','name','birth_date','birth_place'])
            w.writeheader()
            w.writerows(all_victims)
        print(f"Saved jeltelenul_victims.csv ({len(all_victims)} victims)")

    # Also save full_text for inspection
    with open('jeltelenul_raw.csv', 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=['url','slug','title','full_text'])
        w.writeheader()
        for c in all_cases:
            w.writerow({k: c.get(k,'') for k in ['url','slug','title','full_text']})
    print(f"Saved jeltelenul_raw.csv (for inspection)")

    print("\nDone.")
