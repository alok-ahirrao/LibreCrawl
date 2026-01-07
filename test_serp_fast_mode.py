"""
Test script for SERP Fast Mode
Tests the new requests-based fast mode vs Playwright-based browser mode
"""
import sys
import os
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
from src.gmb_core.crawler.serp_parser import GoogleSerpParser


def test_fast_mode():
    """Test the fast mode SERP scanner"""
    print("=" * 60)
    print("SERP Fast Mode Test")
    print("=" * 60)
    
    driver = GeoCrawlerDriver(headless=True)
    parser = GoogleSerpParser()
    
    # Test 1: Simple country-level search (should work in fast mode)
    print("\n[Test 1] Simple search - 'best running shoes' in United States")
    print("-" * 40)
    
    start_time = time.time()
    html, final_url, success = driver.scan_serp_fast(
        keyword="best running shoes",
        location="United States",
        device="desktop",
        depth=10,
        language="en"
    )
    fast_time = time.time() - start_time
    
    if success and html:
        print(f"✓ Fast mode succeeded in {fast_time:.2f}s")
        results = parser.parse_serp_results(html)
        print(f"  - Organic results: {len(results['organic_results'])}")
        print(f"  - Local pack: {len(results['local_pack'])}")
        print(f"  - SERP features detected: {[k for k, v in results['serp_features'].items() if v]}")
        
        # Show top 3 results
        print("\n  Top 3 organic results:")
        for i, r in enumerate(results['organic_results'][:3], 1):
            print(f"    {i}. {r.get('title', 'No title')[:50]}...")
    else:
        print(f"✗ Fast mode failed in {fast_time:.2f}s")
    
    # Test 2: Different location
    print("\n[Test 2] UK search - 'best laptops' in United Kingdom")
    print("-" * 40)
    
    start_time = time.time()
    html, final_url, success = driver.scan_serp_fast(
        keyword="best laptops",
        location="United Kingdom",
        device="desktop",
        depth=10,
        language="en"
    )
    uk_time = time.time() - start_time
    
    if success and html:
        print(f"✓ UK search succeeded in {uk_time:.2f}s")
        results = parser.parse_serp_results(html)
        print(f"  - Organic results: {len(results['organic_results'])}")
    else:
        print(f"✗ UK search failed in {uk_time:.2f}s")
    
    # Test 3: Mobile device
    print("\n[Test 3] Mobile device - 'weather today' in India")
    print("-" * 40)
    
    start_time = time.time()
    html, final_url, success = driver.scan_serp_fast(
        keyword="weather today",
        location="India",
        device="mobile",
        depth=10,
        language="en"
    )
    mobile_time = time.time() - start_time
    
    if success and html:
        print(f"✓ Mobile search succeeded in {mobile_time:.2f}s")
        results = parser.parse_serp_results(html)
        print(f"  - Organic results: {len(results['organic_results'])}")
    else:
        print(f"✗ Mobile search failed in {mobile_time:.2f}s")
    
    print("\n" + "=" * 60)
    print("Test Complete")
    print("=" * 60)


if __name__ == "__main__":
    test_fast_mode()
