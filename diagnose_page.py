"""
Run this on your machine to dump the actual HTML structure of a person page.
Usage: python diagnose_page.py https://jeltelenul.hu/csiky-lajos
"""
import sys
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Accept-Language": "hu-HU,hu;q=0.9",
}

url = sys.argv[1] if len(sys.argv) > 1 else "https://jeltelenul.hu/csiky-lajos"
r = requests.get(url, headers=HEADERS, timeout=20)
print(f"HTTP {r.status_code}  {url}\n")
soup = BeautifulSoup(r.text, "lxml")

print("=== <h1> ===")
for t in soup.find_all("h1"):
    print(" ", t.get_text(strip=True))

print("\n=== divs with 'field' in class ===")
for div in soup.find_all("div", class_=lambda c: c and "field" in c):
    print(f"  classes={div.get('class')}  text={div.get_text(' ', strip=True)[:120]!r}")

print("\n=== <table> rows ===")
for table in soup.find_all("table"):
    for row in table.find_all("tr"):
        cells = [c.get_text(" ", strip=True) for c in row.find_all(["th","td"])]
        if cells:
            print(" ", cells)

print("\n=== <dl> terms + descriptions ===")
for dl in soup.find_all("dl"):
    for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
        print(f"  {dt.get_text(strip=True)!r} → {dd.get_text(strip=True)!r}")

print("\n=== raw HTML snippet (first 6000 chars of <main>/<article>/<body>) ===")
main = soup.find("main") or soup.find("article") or soup.find("body")
print(main.prettify()[:6000] if main else "(nothing found)")
