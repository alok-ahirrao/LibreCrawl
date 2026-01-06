"""
SERP Warmup Script
Run this ONCE in visible mode to:
1. Accept Google's cookie consent
2. Solve any CAPTCHA manually
3. Build browser trust by browsing a bit

After running this, the persistent profile will have cookies that help avoid detection.
"""
import sys
import os
import time

sys.path.append(os.getcwd())

from playwright.sync_api import sync_playwright

def warmup_browser():
    print("=" * 60)
    print("🔥 SERP Browser Warmup - Run Once to Build Trust")
    print("=" * 60)
    print("\nThis will open a VISIBLE browser window.")
    print("Please interact with it if prompted (consent, CAPTCHA).")
    print("After ~60 seconds, the profile will be saved.\n")
    
    profile_dir = os.path.join(os.path.dirname(__file__), 'browser_profile')
    os.makedirs(profile_dir, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch_persistent_context(
            user_data_dir=profile_dir,
            headless=False,  # VISIBLE
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
            ],
            viewport={'width': 1366, 'height': 768},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        )
        
        page = browser.pages[0] if browser.pages else browser.new_page()
        
        print("📍 Step 1: Opening Google homepage...")
        page.goto('https://www.google.com/', wait_until='networkidle')
        print("   ✓ Loaded. If you see a consent dialog, please click 'Accept all'.")
        time.sleep(10)
        
        print("\n📍 Step 2: Performing a test search...")
        try:
            search_box = page.locator('textarea[name="q"], input[name="q"]').first
            if search_box.is_visible():
                search_box.click()
                time.sleep(0.5)
                search_box.type("weather today", delay=100)
                time.sleep(1)
                page.keyboard.press('Enter')
                print("   ✓ Search submitted. Wait for results...")
                time.sleep(10)
        except Exception as e:
            print(f"   ⚠️ Search box not found: {e}")
        
        print("\n📍 Step 3: If you see a CAPTCHA, please solve it now...")
        print("   Waiting 30 seconds for manual interaction...")
        time.sleep(30)
        
        print("\n📍 Step 4: Scrolling to simulate browsing...")
        for i in range(3):
            page.mouse.wheel(0, 300)
            time.sleep(2)
        page.keyboard.press('Home')
        time.sleep(5)
        
        print("\n✅ Warmup complete! Profile saved to:", profile_dir)
        print("   You can now close this window or let it close automatically.")
        
        time.sleep(5)
        browser.close()
    
    print("\n" + "=" * 60)
    print("🎉 Done! Now try running the SERP checker again.")
    print("=" * 60)

if __name__ == "__main__":
    warmup_browser()
