"""
Capture Nashik HTML for analysis
"""
import sys
import os
import re
sys.path.append(os.getcwd())

from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
from bs4 import BeautifulSoup

def capture():
    driver = GeoCrawlerDriver(headless=True)
    html = driver.scan_grid_point("dentist", 19.9975, 73.7898)
    
    if not html:
        print("FAILED")
        return
    
    # Save full HTML
    with open("nashik_full.html", "w", encoding="utf-8") as f:
        f.write(html)
    
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.select('div[role="feed"] > div > div[jsaction]')
    
    # Save first non-sponsored item
    for item in items:
        link = item.select_one('a[href*="/maps/place/"]')
        if not link:
            continue
        sponsored = item.select_one('h1[aria-label="Sponsored"]')
        if not sponsored:
            with open("nashik_item.html", "w", encoding="utf-8") as f:
                f.write(item.prettify())
            
            # Print key elements
            name = link.get('aria-label', 'UNKNOWN')
            print(f"Item: {name}")
            
            rating_span = item.select_one('span[role="img"]')
            if rating_span:
                print(f"Rating aria-label: {rating_span.get('aria-label')}")
            
            # Check all spans
            print("All spans with numbers:")
            for span in item.select('span'):
                text = span.get_text(strip=True)
                if re.search(r'\d', text):
                    print(f"  '{text}'")
            break
            
    print("Saved to nashik_full.html and nashik_item.html")

if __name__ == "__main__":
    capture()
