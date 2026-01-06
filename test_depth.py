"""
Test with current SERP to count actual links and results in HTML
"""
import sys
sys.path.append('.')
from bs4 import BeautifulSoup
from src.gmb_core.crawler.serp_parser import GoogleSerpParser

# First run the scraper to capture fresh HTML
from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver

print("Capturing fresh SERP...")
driver = GeoCrawlerDriver(headless=True)
html, url = driver.scan_serp("best pizza nyc", location="United States", depth=20)

print(f"\nCaptured URL: {url}")
print(f"HTML size: {len(html):,} bytes")

# Save HTML for inspection
with open('test_depth_output.html', 'w', encoding='utf-8') as f:
    f.write(html)
print("Saved to test_depth_output.html")

# Parse and count
soup = BeautifulSoup(html, 'html.parser')

# Count all external links
main_container = soup.select_one('#combined_results') or soup.select_one('#rso') or soup.select_one('#search') or soup
all_links = main_container.select('a[href^="http"]')
external_links = [l for l in all_links if 'google.com' not in l.get('href', '') and 'google.co' not in l.get('href', '')]

print(f"\nAll HTTP links in main container: {len(all_links)}")
print(f"External (non-Google) links: {len(external_links)}")

# Parse with our parser
parser = GoogleSerpParser()
results = parser.parse_serp_results(html)
print(f"\nParser extracted: {len(results['organic_results'])} organic results")
print(f"Local pack: {len(results['local_pack'])} results")

# Show unique domains
domains = set()
for r in results['organic_results']:
    from urllib.parse import urlparse
    domains.add(urlparse(r['url']).netloc)
print(f"Unique domains: {len(domains)}")

# Check if there's a "More results" or pagination
next_page = soup.select_one('a#pnnext') or soup.select_one('a[aria-label="Next page"]')
print(f"\nPagination link found: {'Yes' if next_page else 'No'}")

# Print first 10 results
print("\nFirst 10 results:")
for r in results['organic_results'][:10]:
    print(f"  {r['rank']}. {r['title'][:50]}... - {r['displayed_url']}")
