import sys
import os
import time

# Ensure src is reachable
sys.path.append(os.getcwd())

try:
    from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
    from src.gmb_core.crawler.serp_parser import GoogleSerpParser
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def test_serp_scan():
    print("=" * 50)
    print("Testing Google SERP Checker")
    print("=" * 50)
    
    # Initialize Driver
    try:
        driver = GeoCrawlerDriver(headless=True)
    except Exception as e:
        print(f"Failed to initialize Playwright: {e}")
        print("Run 'pip install playwright' and 'playwright install' if needed.")
        return
    
    # Test query
    keyword = "best running shoes"
    location = "United States"
    target_domain = "nike.com"
    
    print(f"🔍 Keyword: '{keyword}'")
    print(f"📍 Location: {location}")
    print(f"🎯 Target Domain: {target_domain}")
    
    start_time = time.time()
    html, final_url = driver.scan_serp(keyword, location)
    duration = time.time() - start_time
    
    if html:
        print(f"✅ HTML Captured in {duration:.2f}s")
        print(f"📄 Content Length: {len(html)} bytes")
        print(f"🔗 Final URL: {final_url}")
        
        print("\n🧩 Parsing results...")
        parser = GoogleSerpParser()
        results = parser.parse_serp_results(html, target_domain=target_domain)
        
        print(f"✅ Found {len(results['organic_results'])} organic results")
        print(f"📦 Local Pack: {len(results['local_pack'])} items")
        
        # Show top 5 results
        if results['organic_results']:
            print("\n📊 Top 5 Organic Results:")
            for res in results['organic_results'][:5]:
                print(f"  #{res['rank']}: {res['title'][:50]}...")
                print(f"      URL: {res['url'][:60]}...")
        
        # Show target ranking
        if results['target_rank']:
            print(f"\n🎯 Target Domain Ranking: #{results['target_rank']}")
            print(f"   URL: {results['target_url']}")
        else:
            print(f"\n⚠️ Target domain '{target_domain}' not found in results")
        
        # Show SERP features
        active_features = [k for k, v in results['serp_features'].items() if v]
        if active_features:
            print(f"\n🔹 SERP Features Detected: {', '.join(active_features)}")
        
        print("\n✅ Test Complete!")
        
    else:
        print("❌ Failed to capture HTML.")

if __name__ == "__main__":
    test_serp_scan()
