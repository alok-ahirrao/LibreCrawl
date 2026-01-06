"""
Diagnose what selectors work on the captured SERP HTML
"""
import sys
sys.path.append('.')
from bs4 import BeautifulSoup

# Load the last SERP HTML - modify this to point to a saved file if needed
# For now, we'll capture the SERP again via the API endpoint

# Test selectors on actual HTML
def test_selectors(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    selectors_to_test = [
        # Current selectors
        'div.g',
        'div.g:not(.g-blk)',
        'div.MjjYud',
        'div.MjjYud > div',
        'div.MjjYud > div > div[data-hveid]',
        'div.tF2Cxc',
        'div.kvH3mc',
        'div.N54PNb',
        'div[data-sokoban-container]',
        
        # Try more selectors
        '#rso > div',
        '#search div.g',
        'div.yuRUbf',
        'div[data-hveid]',
        'div[jscontroller] a[href^="http"]',
        
        # New potential selectors
        'div.hlcw0c',  # Result container
        'div.Gx5Zad',  # Another result type
        'a[jsname][data-ved]',  # Links with tracking
    ]
    
    print("=" * 60)
    print(f"HTML Size: {len(html_content):,} bytes")
    print("=" * 60)
    
    for sel in selectors_to_test:
        try:
            items = soup.select(sel)
            if items:
                print(f"✓ {sel}: {len(items)} items")
            else:
                print(f"  {sel}: 0 items")
        except Exception as e:
            print(f"✗ {sel}: ERROR - {e}")
    
    # Also check for URLs - how many external links are there?
    print("\n" + "=" * 60)
    print("URL Analysis")
    print("=" * 60)
    
    all_links = soup.select('a[href^="http"]')
    external_links = [l for l in all_links if 'google.com' not in l.get('href', '')]
    unique_domains = set()
    for link in external_links:
        from urllib.parse import urlparse
        try:
            domain = urlparse(link.get('href', '')).netloc
            if domain:
                unique_domains.add(domain)
        except:
            pass
    
    print(f"Total links with http: {len(all_links)}")
    print(f"External (non-Google) links: {len(external_links)}")
    print(f"Unique external domains: {len(unique_domains)}")
    
    # Print first 20 domains
    print("\nFirst 20 unique domains:")
    for i, domain in enumerate(list(unique_domains)[:20], 1):
        print(f"  {i}. {domain}")


if __name__ == "__main__":
    # Read from saved HTML file
    try:
        with open('debug_serp_output.html', 'r', encoding='utf-8') as f:
            html = f.read()
        test_selectors(html)
    except FileNotFoundError:
        print("No debug_serp_output.html found. Run debug_serp.py first or save HTML manually.")
