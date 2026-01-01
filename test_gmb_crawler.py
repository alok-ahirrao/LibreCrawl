import sys
import os
import time

# Ensure src is reachable
sys.path.append(os.getcwd())

try:
    from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
    from src.gmb_core.crawler.parsers import GoogleMapsParser
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def test_scan():
    print("="*50)
    print("Testing GMB 'God Mode' Geo-Crawler")
    print("="*50)
    
    # Initialize Driver (Headless=True for background, False to watch)
    try:
        driver = GeoCrawlerDriver(headless=True)
    except Exception as e:
        print(f"Failed to initialize Playwright: {e}")
        print("Run 'pip install playwright' and 'playwright install' if needed.")
        return

    # Target: Times Square, NYC
    # We expect to see NYC pizza places, not results from the User's actual location
    lat = 40.7580
    lng = -73.9855
    keyword = "pizza"
    
    print(f"📍 Spoofing Location: Times Square ({lat}, {lng})")
    print(f"🔍 Searching for: '{keyword}'")
    
    start_time = time.time()
    html = driver.scan_grid_point(keyword, lat, lng)
    duration = time.time() - start_time
    
    if html:
        print(f"✅ HTML Captured in {duration:.2f}s")
        print(f"📄 Content Length: {len(html)} bytes")
        
        print("🧩 Parsing results...")
        parser = GoogleMapsParser()
        results = parser.parse_list_results(html)
        
        print(f"✅ Found {len(results)} listings")
        
        if len(results) > 0:
            print("\nTop 3 Results (Verification):")
            for i, res in enumerate(results[:3]):
                print(f"  #{res.get('rank')} {res.get('name')} ({res.get('rating')}⭐, {res.get('reviews')} reviews)")
            
            # Simple heuristic check
            first_name = results[0].get('name', '').lower()
            if 'pizza' in first_name or results[0].get('rating', 0) > 0:
                 print("\n✅ Verification Successful: Data looks valid.")
            else:
                 print("\n⚠️ Verification Warning: Top result might be irrelevant.")
        else:
            print("❌ No results parsed. Check selectors or bot detection.")
            
    else:
        print("❌ Failed to capture HTML.")

if __name__ == "__main__":
    test_scan()
