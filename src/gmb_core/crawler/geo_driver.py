"""
Geo-targeted Crawler Driver
Playwright-based crawler with geolocation spoofing and stealth capabilities.
"""
import time
import random
from math import cos, radians
from playwright.sync_api import sync_playwright
from ..config import config


class GeoCrawlerDriver:
    """
    Advanced Playwright Driver with Geolocation Spoofing capabilities.
    """
    
    # Timezone mapping for common regions (lat-based approximation)
    TIMEZONE_MAP = [
        (-180, -157.5, 'Pacific/Midway'),
        (-157.5, -127.5, 'America/Anchorage'),
        (-127.5, -112.5, 'America/Los_Angeles'),
        (-112.5, -97.5, 'America/Denver'),
        (-97.5, -82.5, 'America/Chicago'),
        (-82.5, -67.5, 'America/New_York'),
        (-67.5, -37.5, 'America/Sao_Paulo'),
        (-37.5, -7.5, 'Atlantic/Azores'),
        (-7.5, 7.5, 'Europe/London'),
        (7.5, 22.5, 'Europe/Paris'),
        (22.5, 37.5, 'Europe/Athens'),
        (37.5, 52.5, 'Europe/Moscow'),
        (52.5, 67.5, 'Asia/Karachi'),
        (67.5, 82.5, 'Asia/Dhaka'),
        (82.5, 97.5, 'Asia/Bangkok'),
        (97.5, 112.5, 'Asia/Singapore'),
        (112.5, 127.5, 'Asia/Tokyo'),
        (127.5, 142.5, 'Asia/Tokyo'),
        (142.5, 172.5, 'Pacific/Guam'),
        (172.5, 180, 'Pacific/Auckland'),
    ]
    
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    ]
    
    VIEWPORTS = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1536, 'height': 864},
        {'width': 1440, 'height': 900},
        {'width': 1280, 'height': 720},
    ]
    
    def __init__(self, headless: bool = True, proxy_url: str = None):
        self.headless = headless
        self.proxy_url = proxy_url or (config.PROXY_URL if config.PROXY_ENABLED else None)
        self.timeout = config.CRAWLER_TIMEOUT
    
    def _get_timezone_for_coords(self, lng: float) -> str:
        """Get approximate timezone based on longitude."""
        for min_lng, max_lng, tz in self.TIMEZONE_MAP:
            if min_lng <= lng < max_lng:
                return tz
        return 'UTC'
    
    def _get_random_user_agent(self) -> str:
        """Get a random user agent."""
        return random.choice(self.USER_AGENTS)
    
    def _get_random_viewport(self) -> dict:
        """Get a random viewport size."""
        return random.choice(self.VIEWPORTS)
    
    def _create_stealth_context(self, browser, lat: float, lng: float):
        """Create a browser context with stealth and geolocation settings."""
        timezone = self._get_timezone_for_coords(lng)
        viewport = self._get_random_viewport()
        user_agent = self._get_random_user_agent()
        
        context_options = {
            'geolocation': {'latitude': lat, 'longitude': lng, 'accuracy': 100},
            'permissions': ['geolocation'],
            'viewport': viewport,
            'locale': 'en-US',
            'timezone_id': timezone,
            'user_agent': user_agent,
        }
        
        # Add proxy if configured
        if self.proxy_url:
            context_options['proxy'] = {'server': self.proxy_url}
        
        context = browser.new_context(**context_options)
        
        # Add stealth scripts
        context.add_init_script("""
            // Override webdriver detection
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            
            // Override plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Override platform
            Object.defineProperty(navigator, 'platform', {
                get: () => 'Win32'
            });
            
            // Hide automation indicators
            window.chrome = { runtime: {} };
            
            // Override permissions query
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                Promise.resolve({ state: Notification.permission }) :
                originalQuery(parameters)
            );
        """)
        
        return context
    
    def scan_grid_point(self, keyword: str, lat: float, lng: float) -> str:
        """
        Perform a search for 'keyword' at the precise 'lat, lng'.
        Returns the raw HTML of the results page for parsing.
        """
        with sync_playwright() as p:
            # Launch with stealth args
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-accelerated-2d-canvas',
                '--disable-gpu'
            ]
            
            browser = p.chromium.launch(
                headless=self.headless,
                args=launch_args
            )
            
            context = self._create_stealth_context(browser, lat, lng)
            page = context.new_page()
            
            try:
                # Navigate to Google Maps with keyword
                url = f'https://www.google.com/maps/search/{keyword}/@{lat},{lng},15z'
                page.goto(url, wait_until='domcontentloaded', timeout=self.timeout)
                
                # Reduced delay for speed
                time.sleep(random.uniform(1.0, 2.0))
                
                # Handle consent popup if present
                try:
                    consent_button = page.locator('button:has-text("Accept all")')
                    if consent_button.is_visible(timeout=1500):
                        consent_button.click()
                        time.sleep(0.5)
                except:
                    pass
                
                # Scroll the results panel to load more listings
                sidebar_selector = 'div[role="feed"]'
                
                try:
                    page.wait_for_selector(sidebar_selector, timeout=5000)
                    
                    # Reduced scrolling - just 1-2 times
                    for _ in range(random.randint(1, 2)):
                        page.eval_on_selector(
                            sidebar_selector, 
                            '(el) => el.scrollTop += 600 + Math.random() * 400'
                        )
                        time.sleep(random.uniform(0.3, 0.7))
                        
                except Exception as scroll_error:
                    print(f"Scroll warning: {scroll_error}")
                
                # Capture content
                content = page.content()
                
                return content
            
            except Exception as e:
                print(f"Error during crawl at {lat},{lng}: {e}")
                return None
                
            finally:
                browser.close()
    
    def scan_place_details(self, place_url: str) -> tuple:
        """
        Scrape details from a specific place page.
        
        Args:
            place_url: Full Google Maps place URL
            
        Returns:
            Tuple of (HTML content, final_url) - final_url is the redirected URL with coordinates
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            context = browser.new_context(
                user_agent=self._get_random_user_agent(),
                viewport=self._get_random_viewport()
            )
            
            if self.proxy_url:
                context = browser.new_context(
                    proxy={'server': self.proxy_url},
                    user_agent=self._get_random_user_agent()
                )
            
            page = context.new_page()
            
            try:
                page.goto(place_url, wait_until='domcontentloaded', timeout=self.timeout)
                time.sleep(random.uniform(2.0, 3.0))
                
                # Capture the final URL after any redirects (contains coordinates)
                final_url = page.url
                print(f"[ScanPlaceDetails] Final URL after redirect: {final_url}")
                
                # Click "About" tab if exists to load more details
                try:
                    about_tab = page.locator('button[aria-label*="About"]')
                    if about_tab.is_visible(timeout=2000):
                        about_tab.click()
                        time.sleep(1)
                except:
                    pass
                
                return page.content(), final_url
                
            except Exception as e:
                print(f"Error scraping place details: {e}")
                return None, None
                
            finally:
                browser.close()
    
    def verify_location_spoof(self, lat: float, lng: float) -> bool:
        """Debug method to verify geolocation spoofing works."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=self.headless)
            context = self._create_stealth_context(browser, lat, lng)
            page = context.new_page()
            
            try:
                page.goto('https://browserleaks.com/geo', timeout=self.timeout)
                time.sleep(5)
                page.screenshot(path=f"verification_spoof_{lat}_{lng}.png")
                return True
            except:
                return False
            finally:
                browser.close()
    
    def search_business(self, query: str, location: str = None) -> str:
        """
        Search for a business by name on Google Maps.
        
        Args:
            query: Business name to search for
            location: Optional location context (city, area)
            
        Returns:
            HTML content of the search results page
        """
        search_query = query
        if location:
            search_query = f"{query} {location}"
        
        # Default location for search (can be overridden)
        default_lat = 40.7580  # Times Square, NYC as default
        default_lng = -73.9855
        
        with sync_playwright() as p:
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
            
            browser = p.chromium.launch(
                headless=self.headless,
                args=launch_args
            )
            
            context = self._create_stealth_context(browser, default_lat, default_lng)
            page = context.new_page()
            
            try:
                # Navigate to Google Maps with business search
                url = f'https://www.google.com/maps/search/{search_query}'
                print(f"[BusinessSearch] Navigating to: {url}")
                page.goto(url, wait_until='domcontentloaded', timeout=self.timeout)
                
                # Wait for results to load (optimized for speed)
                time.sleep(random.uniform(1.0, 1.5))
                
                # Handle consent popup if present
                try:
                    consent_button = page.locator('button:has-text("Accept all")')
                    if consent_button.is_visible(timeout=1500):
                        consent_button.click()
                        time.sleep(0.5)
                except:
                    pass
                
                # Wait for results panel
                try:
                    page.wait_for_selector('div[role="feed"]', timeout=5000)
                except:
                    # Try waiting for single business result
                    page.wait_for_selector('h1', timeout=3000)
                
                # Small delay for final rendering
                time.sleep(random.uniform(0.3, 0.7))
                
                content = page.content()
                final_url = page.url
                print(f"[BusinessSearch] Captured {len(content)} bytes of HTML from {final_url}")
                return content, final_url
                
            except Exception as e:
                print(f"[BusinessSearch] Error searching for '{query}': {e}")
                return None, None
                
            finally:
                browser.close()
