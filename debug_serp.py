import sys
import os
import time

sys.path.append(os.getcwd())

try:
    from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
    from src.gmb_core.crawler.serp_parser import GoogleSerpParser
except ImportError as e:
    print(f"Import Error: {e}")
    sys.exit(1)

def debug_serp():
    print("=" * 50)
    print("DEBUG: Google SERP - Saving HTML for inspection")
    print("=" * 50)
    
    # Run in VISIBLE mode to see what's happening
    driver = GeoCrawlerDriver(headless=False)  # <-- Non-headless to debug
    
    keyword = "pizza"
    location = "United States"
    
    print(f"🔍 Keyword: '{keyword}'")
    print(f"📍 Location: {location}")
    
    html, final_url = driver.scan_serp(keyword, location)
    
    if html:
        print(f"✅ Captured {len(html)} bytes")
        print(f"🔗 Final URL: {final_url}")
        
        # Save HTML for inspection
        with open("debug_serp_output.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("📄 HTML saved to: debug_serp_output.html")
        
        # Check for common blocking indicators
        blocking_indicators = [
            "unusual traffic",
            "captcha",
            "sorry",
            "blocked",
            "consent",
            "Before you continue"
        ]
        
        html_lower = html.lower()
        for indicator in blocking_indicators:
            if indicator.lower() in html_lower:
                print(f"⚠️ BLOCKING DETECTED: Found '{indicator}' in HTML")
        
        # Try parsing
        parser = GoogleSerpParser()
        results = parser.parse_serp_results(html)
        print(f"\n📊 Parsed {len(results['organic_results'])} organic results")
        
        if results['organic_results']:
            for r in results['organic_results'][:3]:
                print(f"  #{r['rank']}: {r['title'][:50]}...")
        
    else:
        print("❌ No HTML captured")

if __name__ == "__main__":
    debug_serp()
