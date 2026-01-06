import os
import sys
import json
from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
from src.gmb_core.crawler.serp_parser import GoogleSerpParser

# Force visible execution
os.environ['GMB_CRAWLER_HEADLESS'] = 'false'

def test_extended_local_scan():
    driver = GeoCrawlerDriver(headless=False)
    parser = GoogleSerpParser()
    
    keyword = "dentist"
    location = "New York, NY"
    
    print(f"--- Starting Test: Extended Local Pack Scan ---")
    print(f"Query: {keyword} in {location}")
    print("------------------------------------------------")
    
    try:
        # Run scan (depth 10 to trigger organic scan + local check)
        html, final_url = driver.scan_serp(keyword, location, depth=10)
        
        if not html:
            print("❌ Failed to get HTML")
            return
            
        print(f"\nScan complete. Processing results...")
        
        # Check if local finder HTML was appended
        if "<!-- LOCAL_FINDER_HTML_START -->" in html:
            print("✅ '<!-- LOCAL_FINDER_HTML_START -->' marker FOUND in output HTML.")
        else:
            print("❌ Marker NOT FOUND. 'More places' logic might not have triggered (or no local pack).")
            
        # Parse
        data = parser.parse_serp_results(html)
        
        local_results = data.get('local_pack', [])
        print(f"\n--- Parsed Local Results ({len(local_results)} items) ---")
        
        for i, item in enumerate(local_results[:5]): # Show first 5
            print(f"[{i+1}] {item.get('name')} | Rating: {item.get('rating')} | Site: {item.get('website')}")
            
        if len(local_results) > 3:
            print(f"\n✅ SUCCESS: Found {len(local_results)} local items (more than standard 3).")
            websites_count = sum(1 for item in local_results if item.get('website'))
            print(f"✅ Found {websites_count} items with Website URLs.")
        else:
            print("\n⚠️ WARNING: Found 3 or fewer items. Extended scan might have failed or query has no 'More places'.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_extended_local_scan()
