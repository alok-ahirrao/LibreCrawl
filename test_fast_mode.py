
import sys
import os
import time
from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver

def test_fast_scan():
    print("🚀 Starting Fast Mode Benchmark...")
    driver = GeoCrawlerDriver(headless=True)
    
    start_time = time.time()
    
    # Test a Known Location
    keyword = "coffee shop"
    lat = 40.7580
    lng = -73.9855
    
    print(f"📍 Scanning {keyword} at {lat}, {lng} with fast_mode=True...")
    
    try:
        html = driver.scan_grid_point(keyword, lat, lng, fast_mode=True)
        
        duration = time.time() - start_time
        print(f"⏱️ Time taken: {duration:.2f} seconds")
        
        if html and len(html) > 1000:
            print(f"✅ Success! Captured {len(html)} bytes.")
        else:
            print("❌ Failed to capture valid HTML.")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    test_fast_scan()
