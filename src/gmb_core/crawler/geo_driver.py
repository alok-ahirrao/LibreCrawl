import time
import random
import numpy as np
from math import cos, sin, radians, sqrt
from playwright.sync_api import sync_playwright
from ..config import config
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
        # Windows Chrome 130+ (High Trust)
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        # Windows Edge (High Trust)
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
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
        # Persistent profile for cookie retention
        import os
        self.profile_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'browser_profile')
        os.makedirs(self.profile_dir, exist_ok=True)
    
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
    
    def _generate_uule(self, location_name: str) -> str:
        """Generate Google UULE parameter for precise location targeting."""
        import base64
        
        # Canonicalize (simplified)
        location_name = location_name.strip()
        
        # Calculate length encoding
        original_len = len(location_name)
        secret_list = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        if original_len >= len(secret_list):
            return "" # Fallback or error
            
        key = secret_list[original_len]
        
        # Base64 encode
        b64 = base64.b64encode(location_name.encode()).decode().replace('+', '-').replace('/', '_').replace('=', '')
        
        return f"w+CAIQICI{key}{b64}"

    def resolve_location_to_coords(self, location_name: str) -> tuple:
        """
        Resolve a location string (e.g. 'Boston, MA') to (lat, lng) using Google Maps.
        Returns (None, None) if resolution fails.
        """
        from urllib.parse import quote_plus
        import re
        
        print(f"[Geo] Resolving coordinates for '{location_name}'...")
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--disable-blink-features=AutomationControlled']
            )
            # Use a fresh context with no specific location to let Google find the best match
            context = browser.new_context(
                user_agent=self._get_random_user_agent(),
                viewport=self._get_random_viewport()
            )
            # Block media for speed
            context.route("**/*", lambda route: route.abort() 
                if route.request.resource_type in ["image", "media", "font"] 
                else route.continue_())
                
            page = context.new_page()
            try:
                # Search for the location
                url = f"https://www.google.com/maps/search/{quote_plus(location_name)}"
                page.goto(url, wait_until='domcontentloaded', timeout=15000)
                
                # Wait for URL to update with coordinates
                # Pattern: /@lat,lng,zoom
                target_url = page.url
                # Wait up to 5s for redirect/update
                for _ in range(10):
                    if '@' in target_url:
                        break
                    time.sleep(0.5)
                    target_url = page.url
                    
                match = re.search(r'@([-0-9.]+),([-0-9.]+)', target_url)
                if match:
                    lat = float(match.group(1))
                    lng = float(match.group(2))
                    print(f"[Geo] Resolved '{location_name}' -> ({lat}, {lng})")
                    return lat, lng
                else:
                    print(f"[Geo] Could not extract coordinates from URL: {target_url}")
                    return None, None
            except Exception as e:
                print(f"[Geo] Resolution error: {e}")
                return None, None
            finally:
                browser.close()

    def scan_grid_point(self, keyword: str, lat: float, lng: float, fast_mode: bool = False) -> str:
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
            
            # FAST MODE: Block heavy resources
            if fast_mode:
                context.route("**/*", lambda route: route.abort() 
                    if route.request.resource_type in ["image", "media", "font", "stylesheet"] 
                    else route.continue_())
                    
            page = context.new_page()
            
            try:
                # Navigate to Google Maps with keyword
                url = f'https://www.google.com/maps/search/{keyword}/@{lat},{lng},15z'
                page.goto(url, wait_until='networkidle', timeout=self.timeout)
                
                # Wait for page to stabilize
                if fast_mode:
                    time.sleep(random.uniform(0.5, 1.0))
                else:
                    time.sleep(random.uniform(1.5, 2.5))
                
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
                    
                     
                    # Enhanced scrolling - scroll more times and with longer delays
                    # This ensures all content (including review counts) is loaded
                    num_scrolls = random.randint(1, 2) if fast_mode else random.randint(3, 4)
                    
                    for i in range(num_scrolls):
                        page.eval_on_selector(
                            sidebar_selector, 
                            '(el) => el.scrollTop += 500 + Math.random() * 300'
                        )
                        # Longer delay to allow content to load
                        if fast_mode:
                            time.sleep(random.uniform(0.2, 0.4))
                        else:
                            time.sleep(random.uniform(0.5, 1.0))
                    
                    # Scroll back to top to ensure first items are fully rendered
                    page.eval_on_selector(
                        sidebar_selector,
                        '(el) => el.scrollTop = 0'
                    )
                    time.sleep(random.uniform(0.5, 0.8))
                    
                    # Hover over listing items to trigger full content loading
                    # This forces Google Maps to load detailed info including review counts
                    # FAST MODE: Skip hovers if possible, or do fewer
                    if not fast_mode:
                        try:
                            listing_items = page.locator('div[role="article"]').all()
                            for i, item in enumerate(listing_items[:10]):  # First 10 items
                                try:
                                    item.hover(timeout=500)
                                    time.sleep(random.uniform(0.1, 0.2))
                                except:
                                    pass
                        except:
                            pass
                    
                    # Wait a bit more for content to finish loading after hovers
                    time.sleep(random.uniform(0.3, 0.5))
                    
                    # Wait for feed items to be populated with content
                    try:
                        # Wait for at least one item with a rating span
                        page.wait_for_selector('span[role="img"][aria-label]', timeout=3000)
                    except:
                        pass  # Continue even if not found
                        
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
    
    
    def scan_place_details(self, place_url: str, lat: float = None, lng: float = None) -> tuple:
        """
        Scrape details from a specific place page.
        
        Args:
            place_url: Full Google Maps place URL
            lat: Optional latitude for context
            lng: Optional longitude for context
            
        Returns:
            Tuple of (HTML content, final_url)
        """
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=['--disable-blink-features=AutomationControlled']
            )
            
            # Use specific coordinates if provided, otherwise default to a varied location or keep it simple
            if lat is not None and lng is not None:
                context = self._create_stealth_context(browser, lat, lng)
            else:
                 # If no coords, create context with random viewport/UA but no specific geolocation override (or default to US)
                context = browser.new_context(
                    user_agent=self._get_random_user_agent(),
                    viewport=self._get_random_viewport()
                )
            
            if self.proxy_url:
                # Re-create context with proxy if needed (this overrides the above unless we merge options)
                # Ideally _create_stealth_context handles proxy. 
                # If we fell back to new_context, we need to add proxy here.
                # But let's check if _create_stealth_context handles proxy? Yes it does.
                
                if lat is None or lng is None:
                     context = browser.new_context(
                        proxy={'server': self.proxy_url},
                        user_agent=self._get_random_user_agent(),
                        viewport=self._get_random_viewport()
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
    
    def search_business(self, query: str, location: str = None, lat: float = None, lng: float = None) -> str:
        """
        Search for a business by name on Google Maps.
        
        Args:
            query: Business name to search for
            location: Optional location context (city, area)
            lat: Optional latitude for context
            lng: Optional longitude for context
            
        Returns:
            HTML content of the search results page
        """
        search_query = query
        if location:
            search_query = f"{query} {location}"
        
        # Use provided lat/lng or fallback
        # If no lat/lng, we can try to guess or just use a generic US location if location string implies US
        # But crucially, we shouldn't force NYC if the user asked for London unless we have London coords.
        # For now, if no lat/lng, we use the default NYC *only if* no location string is provided, 
        # OR we just rely on Google finding it by string.
        
        target_lat = lat if lat is not None else 40.7580
        target_lng = lng if lng is not None else -73.9855
        
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
            
            context = self._create_stealth_context(browser, target_lat, target_lng)
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
    
    def scan_serp(self, keyword: str, location: str = "United States", 
                  device: str = "desktop", depth: int = 10, language: str = "en",
                  lat: float = None, lng: float = None) -> tuple:
        """
        Perform a Google Search and capture the SERP HTML.
        
        Args:
            keyword: Search query
            location: Location name (country or city)
            device: 'desktop' or 'mobile'
            depth: Number of results to request (10, 20, 50, 100)
            language: Language code (e.g., 'en', 'es')
            lat: Optional latitude for context
            lng: Optional longitude for context
            
        Returns:
            Tuple of (HTML content, final_url)
        """
        import time
        import random
        from urllib.parse import quote_plus
        
        # Map location to Google's gl parameter (country code)
        LOCATION_MAP = {
            'united states': 'us',
            'usa': 'us',
            'united kingdom': 'gb',
            'uk': 'gb',
            'canada': 'ca',
            'australia': 'au',
            'germany': 'de',
            'france': 'fr',
            'india': 'in',
            'brazil': 'br',
            'mexico': 'mx',
            'spain': 'es',
            'italy': 'it',
            'netherlands': 'nl',
            'japan': 'jp',
        }
        
        # Handle GL code logic - Do NOT default to 'us' if it's a custom location
        cleaned_loc = location.lower().strip()
        gl_code = LOCATION_MAP.get(cleaned_loc)
        
        # If not in map but location provided, default to 'us' ONLY if strictly needed? 
        # No, for custom locations (e.g. Nashik), we rely on UULE and should NOT send gl=us
        # So we leave gl_code as None if not in map
        if not gl_code and not cleaned_loc:
             gl_code = 'us'

        # [NEW] Automatic Coordinate Resolution for Local Search
        # If we have a specific location (no gl_code match) and no coordinates, try to find them
        # [FIX] Use API-based geocoding instead of browser to avoid opening extra browser windows
        if location.strip() and not gl_code and (lat is None or lng is None):
            print(f"[SerpScan] Location '{location}' needs coordinates. Resolving via API...")
            try:
                from ..geoip import geocode_location
                geo_result = geocode_location(location)
                if geo_result and geo_result.get('lat') and geo_result.get('lng'):
                    lat = geo_result['lat']
                    lng = geo_result['lng']
                    
                    # Use country_code from geocoding result for gl parameter
                    country_code = geo_result.get('country_code', '').lower()
                    if country_code and country_code in ['us', 'gb', 'ca', 'au', 'de', 'fr', 'in', 'br', 'mx', 'es', 'it', 'nl', 'jp']:
                        gl_code = country_code
                    
                    print(f"[SerpScan] Resolved coordinates: {lat}, {lng} -> GL: {gl_code}")
                else:
                    print(f"[SerpScan] Could not resolve coordinates for '{location}'")
            except Exception as e:
                print(f"[SerpScan] Geocoding error: {e}")
                # Fallback to browser-based resolution if API fails
                r_lat, r_lng = self.resolve_location_to_coords(location)
                if r_lat is not None and r_lng is not None:
                    lat = r_lat
                    lng = r_lng
                    
                    # Infer GL code from coordinates
                    if not gl_code:
                        if -125 <= lng <= -65: gl_code = 'us'
                        elif -10 <= lng <= 3 and 49 <= lat <= 60: gl_code = 'gb'
                        elif 68 <= lng <= 97 and 8 <= lat <= 37: gl_code = 'in'
                        elif 112 <= lng <= 154 and -44 <= lat <= -10: gl_code = 'au'
                        elif -141 <= lng <= -52 and 41 <= lat <= 83: gl_code = 'ca'
                        
                    print(f"[SerpScan] Fallback resolved: {lat}, {lng} -> GL: {gl_code}")



        uule = ""
        # Generate UULE if it's a custom location (not a country code match)
        # BUT skip UULE if we have precise coordinates to avoid conflict?
        # Actually Google sometimes prefers UULE over Geo, so let's keep it but prioritize Geo in context
        if hasattr(self, '_generate_uule') and location.strip() and not gl_code:
             # Only use UULE if we didn't resolve coordinates? 
             # No, UULE is good backup. But if we have lat/lng, maybe we don't strictly need it?
             # Let's keep it for now but log it.
            uule = self._generate_uule(location)
            print(f"[SerpScan] Generated UULE for '{location}': {uule}")
        
        # [FIX] If we have lat/lng but no gl_code yet, infer gl_code from coordinates
        # This is essential for product/shopping searches which rely on gl parameter
        if lat is not None and lng is not None and not gl_code:
            print(f"[SerpScan] Inferring GL code from coordinates: {lat}, {lng}")
            if -125 <= lng <= -65:  # US
                gl_code = 'us'
            elif -10 <= lng <= 3 and 49 <= lat <= 60:  # UK
                gl_code = 'gb'
            elif 68 <= lng <= 97 and 8 <= lat <= 37:  # India
                gl_code = 'in'
            elif 112 <= lng <= 154 and -44 <= lat <= -10:  # Australia
                gl_code = 'au'
            elif -141 <= lng <= -52 and 41 <= lat <= 83:  # Canada
                gl_code = 'ca'
            elif 5 <= lng <= 15 and 47 <= lat <= 55:  # Germany
                gl_code = 'de'
            elif -5 <= lng <= 10 and 42 <= lat <= 51:  # France
                gl_code = 'fr'
            elif 6 <= lng <= 18 and 36 <= lat <= 47:  # Italy
                gl_code = 'it'
            elif -9 <= lng <= 4 and 36 <= lat <= 44:  # Spain
                gl_code = 'es'
            elif 129 <= lng <= 146 and 30 <= lat <= 46:  # Japan
                gl_code = 'jp'
            elif -73 <= lng <= -34 and -34 <= lat <= 5:  # Brazil
                gl_code = 'br'
            elif -118 <= lng <= -86 and 14 <= lat <= 33:  # Mexico
                gl_code = 'mx'
            elif 3 <= lng <= 7 and 50 <= lat <= 54:  # Netherlands
                gl_code = 'nl'
            
            if gl_code:
                print(f"[SerpScan] Inferred GL code from coordinates: {gl_code}")
        
        # Device viewport settings
        if device.lower() == 'mobile':
            viewport = {'width': 375, 'height': 812}
            user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        else:
            viewport = self._get_random_viewport()
            user_agent = self._get_random_user_agent()
        
        with sync_playwright() as p:
            # ... (launch args omitted for brevity in replace block if unchanged, but I need to include context to replace correctly)
            # Actually I should target the lines before context creation
            
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--disable-gpu',
                '--no-first-run',
                '--no-default-browser-check',
            ]
            
            # Prepare context options
            context_args = {
                'user_data_dir': self.profile_dir,
                'headless': self.headless,
                'args': launch_args,
                'viewport': viewport,
                'user_agent': user_agent,
                'java_script_enabled': True,
                'bypass_csp': True,
                'ignore_https_errors': True,
            }
            
            # Helper to get timezone (simple approximation or default)
            tz_id = "America/New_York"
            if lat and lng:
                if 60 < lng < 100: tz_id = "Asia/Kolkata" # India
                elif -10 < lng < 3: tz_id = "Europe/London" # UK
                elif -125 < lng < -65: # US
                     if lng < -115: tz_id = "America/Los_Angeles"
                     elif lng < -105: tz_id = "America/Denver"
                     elif lng < -95: tz_id = "America/Chicago"
                     else: tz_id = "America/New_York"
                # Add more if needed, or implement proper lookup
            
            # Add Geolocation if available
            if lat is not None and lng is not None:
                context_args['geolocation'] = {'latitude': lat, 'longitude': lng, 'accuracy': 100}
                context_args['permissions'] = ['geolocation']
                context_args['timezone_id'] = tz_id
                
            # Add Locale
            locale_suffix = gl_code.upper() if gl_code else 'US'
            context_args['locale'] = f'{language}-{locale_suffix}'
            
            # Add Proxy
            if self.proxy_url:
                context_args['proxy'] = {'server': self.proxy_url}
            
            # Launch persistent context
            context = p.chromium.launch_persistent_context(**context_args)

            # Enhanced stealth scripts
            context.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                    configurable: true
                });
                
                // Realistic plugins array
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        const plugins = [
                            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer' },
                            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
                            { name: 'Native Client', filename: 'internal-nacl-plugin' }
                        ];
                        plugins.item = (i) => plugins[i];
                        plugins.namedItem = (name) => plugins.find(p => p.name === name);
                        plugins.refresh = () => {};
                        return plugins;
                    }
                });
                
                // Languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en', 'es']
                });
                
                // Platform matching user agent
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32'
                });
                
                // Hardware concurrency (realistic value)
                Object.defineProperty(navigator, 'hardwareConcurrency', {
                    get: () => 8
                });
                
                // Device memory (realistic value)
                Object.defineProperty(navigator, 'deviceMemory', {
                    get: () => 8
                });
                
                // Chrome object
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() { return {}; },
                    app: {}
                };
                
                // Permissions API
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
                );
                
                // WebGL vendor/renderer
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) return 'Intel Inc.';
                    if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                    return getParameter.apply(this, arguments);
                };
            """)
            
            page = context.new_page()
            
            try:
                # Map location to country-specific Google domain
                GOOGLE_DOMAINS = {
                    'us': 'google.com',
                    'gb': 'google.co.uk',
                    'ca': 'google.ca',
                    'au': 'google.com.au',
                    'de': 'google.de',
                    'fr': 'google.fr',
                    'in': 'google.co.in',
                    'br': 'google.com.br',
                    'mx': 'google.com.mx',
                    'es': 'google.es',
                    'it': 'google.it',
                    'nl': 'google.nl',
                    'jp': 'google.co.jp',
                }
                
                # Default to google.com if no gl_code
                google_domain = GOOGLE_DOMAINS.get(gl_code, 'google.com')
                
                # Warmup skipped for speed
                # proceeding to direct navigation optimization
                
                # (Simulation removed)
                
                # DIRECT NAVIGATION (Optimized)
                # Skip warm-up and simulated typing for speed
                
                search_url = f'https://www.{google_domain}/search?q={quote_plus(keyword)}&hl={language}&num={depth}'
                
                if gl_code:
                    search_url += f'&gl={gl_code}'
                
                if uule:
                    search_url += f'&uule={uule}'
                    
                print(f"[SerpScan] fast-nav -> {search_url}")
                page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
                
                # Wait for results
                print(f"[SerpScan] Waiting for search results (depth={depth})...")
                time.sleep(random.uniform(3.0, 5.0))
                
                # Check for CAPTCHA and wait for manual solving if detected
                def check_and_wait_for_captcha(pg, max_wait=120):
                    """Check for CAPTCHA and wait for user to solve it."""
                    try:
                        html_preview = pg.content()[:5000].lower()
                    except Exception as e:
                        print(f"[SerpScan] Warning: Could not check for CAPTCHA (page navigating?): {e}")
                        time.sleep(2)
                        return True

                    captcha_indicators = ['unusual traffic', 'captcha', 'recaptcha', 'verify you', 'not a robot']
                    
                    if any(indicator in html_preview for indicator in captcha_indicators):
                        print("[SerpScan] ⚠️ CAPTCHA detected! Please solve it in the browser window...")
                        print(f"[SerpScan] Waiting up to {max_wait} seconds for CAPTCHA to be solved...")
                        
                        # Wait for CAPTCHA to be solved (check every 3 seconds)
                        for wait_count in range(max_wait // 3):
                            time.sleep(3)
                            try:
                                current_html = pg.content()[:5000].lower()
                            except Exception:
                                continue # Ignore errors during check (navigation/reload)

                            # Check if CAPTCHA is gone (search results appeared)
                            if 'search' in current_html and not any(ind in current_html for ind in captcha_indicators):
                                print("[SerpScan] ✓ CAPTCHA solved! Continuing...")
                                time.sleep(2)  # Wait a bit more for page to fully load
                                return True
                            if wait_count % 5 == 0:
                                print(f"[SerpScan] Still waiting for CAPTCHA... ({(wait_count + 1) * 3}s)")
                        
                        print("[SerpScan] ⚠️ Timeout waiting for CAPTCHA. Results may be incomplete.")
                        return False
                    return True
                
                # Check for CAPTCHA on initial page load
                check_and_wait_for_captcha(page)
                
                # Collect HTML from multiple pages if depth > 10
                all_html_parts = []
                
                if depth > 10:
                    print(f"[SerpScan] Collecting results across multiple pages for depth={depth}...")
                    
                    # Calculate how many pages we might need (10 results per page)
                    pages_needed = max(1, depth // 10)
                    
                    for page_num in range(pages_needed):
                        # Capture current page's HTML
                        current_html = page.content()
                        all_html_parts.append(current_html)
                        print(f"[SerpScan] Captured page {page_num + 1} ({len(current_html)} bytes)")
                        
                        if page_num >= pages_needed - 1:
                            break
                        
                        # Scroll to bottom to find "More results" or "Next"
                        for _ in range(3):
                            page.keyboard.press('End')
                            time.sleep(random.uniform(0.3, 0.5))
                        
                        time.sleep(random.uniform(1.0, 2.0))
                        
                        # Try to click "More results" button
                        more_results_selectors = [
                            'a:has-text("More results")',
                            '#pnnext',  # Traditional next page
                            'a[aria-label="More results"]',
                            'a[aria-label="Next page"]',
                            'span:has-text("Next") >> xpath=ancestor::a',
                        ]
                        
                        clicked = False
                        for selector in more_results_selectors:
                            try:
                                more_btn = page.locator(selector).first
                                if more_btn.is_visible(timeout=2000):
                                    more_btn.click()
                                    print(f"[SerpScan] Moving to page {page_num + 2}...")
                                    clicked = True
                                    time.sleep(random.uniform(2.5, 4.0))
                                    break
                            except:
                                continue
                        
                        if not clicked:
                            print("[SerpScan] No more pages available")
                            break
                    
                    # Combine all HTML parts - wrap in container for parser
                    # Combine all HTML parts - Merge into the first page (Base)
                    # [FIX] Use Page 1 as base to preserve Local Pack and other features
                    print(f"[SerpScan] Merging {len(all_html_parts)} pages into single SERP...")
                    from bs4 import BeautifulSoup
                    
                    # Parse Page 1 as the base
                    base_soup = BeautifulSoup(all_html_parts[0], 'html.parser')
                    base_rso = base_soup.select_one('#rso') or base_soup.select_one('#search')
                    
                    if base_rso:
                        for i, html_part in enumerate(all_html_parts[1:]):
                            try:
                                part_soup = BeautifulSoup(html_part, 'html.parser')
                                part_rso = part_soup.select_one('#rso') or part_soup.select_one('#search')
                                
                                if part_rso:
                                    # Create a separator comment for clarity (optional, but good for debugging)
                                    # We just append children of the new RSO to the base RSO
                                    
                                    # Append all children of next page's RSO to base RSO
                                    # We convert to list to avoid modification issues during iteration
                                    for child in list(part_rso.children):
                                        base_rso.append(child)
                            except Exception as e:
                                print(f"[SerpScan] Error merging page {i+2}: {e}")
                                
                        content = str(base_soup)
                        print(f"[SerpScan] Merged content size: {len(content)} bytes")
                    else:
                         # Fallback if Page 1 structure is weird - just concat strings (raw)
                         print("[SerpScan] Could not find #rso in Page 1, falling back to concatenation")
                         content = "".join(all_html_parts)
                    
                    final_url = page.url
                else:
                    # Standard behavior for depth <= 10
                    for _ in range(3):
                        page.mouse.wheel(0, random.randint(300, 500))
                        time.sleep(random.uniform(0.5, 1.0))
                    
                    content = page.content()
                    final_url = page.url
                    print(f"[SerpScan] Captured {len(content)} bytes of HTML")
                
                # --- Extended Local Pack Scan ---
                # --- Strict Heading-Based Navigation ---
                # Check for specific headings to determine crawl path
                try:
                    from bs4 import BeautifulSoup
                    soup_check = BeautifulSoup(content, 'html.parser')
                    
                    headings = {
                        'PLACES': ['Places', 'Businesses'],
                        # Broader Hotel matching
                        'HOTELS': ['Hotels'], 
                        'SHOPPING': ['Popular products']
                    }
                    
                    detected_intent = 'ORGANIC'
                    detected_heading_text = ''
                    
                    # Scan headers (h1-h6, div[role="heading"], specific classes)
                    # We look for exact text matches or "Starts with" for Hotels
                    
                    # Create a set of candidate header texts
                    candidate_texts = set()
                    for h in soup_check.find_all(['h1', 'h2', 'h3', 'div', 'span']):
                        text = h.get_text(strip=True)
                        if text and len(text) < 50: # Optimization
                            candidate_texts.add(text)
                            
                    # Check against rules
                    for intent, keywords in headings.items():
                        for k in keywords:
                            for text in candidate_texts:
                                # Flexible Hotel Match: "Hotels" or "Hotels | ..." or "Hotels in ..."
                                if intent == 'HOTELS' and 'Hotels' in text:
                                    # Ensure it's a heading-like text, not just a random span
                                    detected_intent = 'HOTELS'
                                    detected_heading_text = text
                                    break
                                elif text == k:
                                    detected_intent = 'PLACES' # 'Businesses' maps to PLACES logic
                                    detected_heading_text = text
                                    break
                                elif 'Popular products' in text:
                                    detected_intent = 'SHOPPING'
                                    detected_heading_text = text
                                    break
                            if detected_intent != 'ORGANIC':
                                break
                        if detected_intent != 'ORGANIC':
                            break
                    
                    # FALLBACK: Check for "View all hotels" link if heading missed
                    if detected_intent == 'ORGANIC':
                        fallback_hotel_link = soup_check.find(lambda tag: tag.name == 'a' and ('View all hotels' in tag.get_text() or 'More hotels' in tag.get_text()) and 'google' not in tag.get('href', ''))
                        if fallback_hotel_link:
                            print(f"[SerpScan] Fallback: Detected Hotel intent via Link '{fallback_hotel_link.get_text()}'")
                            detected_intent = 'HOTELS'
                            detected_heading_text = "Fallback Link"

                    print(f"[SerpScan] Detected Heading: '{detected_heading_text}' -> Intent: {detected_intent}")
                    
                    if detected_heading_text:
                        content += f"\n<!-- DETECTED_HEADING: {detected_heading_text} -->\n"

                    if detected_intent == 'PLACES':
                        # Action: Select "More places" / "More businesses"
                        # Look for link containing "More places" or "View all" specifically near the header or in local pack
                        local_link = soup_check.find(lambda tag: tag.name == 'a' and ('More places' in tag.get_text() or 'More businesses' in tag.get_text() or 'View all' in tag.get_text()) and 'google' not in tag.get('href', ''))
                        
                        if local_link and local_link.get('href'):
                            local_url = local_link.get('href')
                            if local_url.startswith('/'):
                                local_url = f"https://www.{google_domain}{local_url}"
                            
                            # [FIX] Append UULE/GL to Local URL to preserve location
                            if uule and 'uule=' not in local_url:
                                local_url += f"&uule={uule}"
                            if gl_code and 'gl=' not in local_url:
                                local_url += f"&gl={gl_code}"

                                
                            # VALIDATION: Ensure it looks like a map/search URL (not video)
                            if '/maps' in local_url or 'tbs=lrf' in local_url or '/search?' in local_url:
                                if '/vid' not in local_url:
                                    print(f"[SerpScan] Found VALID 'More places' URL: {local_url}")
                                    print("[SerpScan] Navigating to Local Finder...")
                                    page.goto(local_url, wait_until='domcontentloaded', timeout=20000)
                                else:
                                    print(f"[SerpScan] SKIPPING: URL looks like video/image: {local_url}")
                            else:
                                print(f"[SerpScan] SKIPPING: URL does not look like Local Finder: {local_url}")

                    elif detected_intent == 'HOTELS':
                        print(f"[SerpScan] Hotel intent detected via '{detected_heading_text}'. Looking for 'See more' link...")
                        # Look for "View all", "More hotels", or similar
                        hotel_link = soup_check.find(lambda tag: tag.name == 'a' and ('View all' in tag.get_text() or 'More hotels' in tag.get_text() or 'See more' in tag.get_text()) and 'google' not in tag.get('href', ''))
                        
                        if hotel_link and hotel_link.get('href'):
                            hotel_url = hotel_link.get('href')
                            if hotel_url.startswith('/'):
                                hotel_url = f"https://www.{google_domain}{hotel_url}"
                            
                            # [FIX] Append UULE/GL to Hotel URL
                            if uule and 'uule=' not in hotel_url:
                                hotel_url += f"&uule={uule}"
                            if gl_code and 'gl=' not in hotel_url:
                                hotel_url += f"&gl={gl_code}"

                            
                            print(f"[SerpScan] Found Hotel View URL: {hotel_url}")
                            print("[SerpScan] Navigating to Hotel Finder...")
                            page.goto(hotel_url, wait_until='domcontentloaded', timeout=20000)
                            
                            # Hotel finder specific wait - it often takes longer to load the grid
                            try:
                                # Wait for hotel cards (often identifiable by specific classes or just a list)
                                # Hotel cards usually have prices, so waiting for currency symbol might be good, or just wait for the main feed
                                page.wait_for_selector('div[role="feed"], div[jsname*=""]', timeout=10000) 
                                time.sleep(random.uniform(2.0, 4.0)) # Extra wait for Hotel UI
                                
                                # Scroll logic specifically for Hotels
                                print("[SerpScan] Scrolling Hotel List...")
                                for _ in range(4):
                                    page.mouse.wheel(0, random.randint(500, 800))
                                    time.sleep(random.uniform(0.8, 1.5))
                                    
                            except Exception as e:
                                print(f"[SerpScan] Hotel wait/scroll warning: {e}")
                                time.sleep(2)
                        else:
                            print("[SerpScan] Could not find a 'View all' link for Hotels.")

                    elif detected_intent == 'SHOPPING':
                        print("[SerpScan] Shopping intent detected. Looking for 'Shopping' tab...")
                        # 1. Try to find the "Shopping" tab in the top navigation bar
                        # Tabs are usually <a> tags with text "Shopping"
                        shopping_tab = soup_check.find(lambda tag: tag.name == 'a' and 'Shopping' in tag.get_text())
                        
                        if shopping_tab and shopping_tab.get('href'):
                             shopping_url = shopping_tab.get('href')
                             if shopping_url.startswith('/'):
                                 shopping_url = f"https://www.{google_domain}{shopping_url}"
                             
                             # [FIX] Append UULE/GL to Shopping URL
                             if uule and 'uule=' not in shopping_url:
                                 shopping_url += f"&uule={uule}"
                             if gl_code and 'gl=' not in shopping_url:
                                 shopping_url += f"&gl={gl_code}"

                             
                             print(f"[SerpScan] Found Shopping Tab URL: {shopping_url}")
                             print("[SerpScan] Navigating to Shopping Tab...")
                             page.goto(shopping_url, wait_until='domcontentloaded', timeout=20000)
                             
                             # Wait for product grid
                             try:
                                 # Common selector for shopping grid
                                 # Updated with more robust selectors including individual items
                                 page.wait_for_selector('div.sh-pr__product-results-grid, div.sh-dgr__grid-result, div[data-docid], div.i0X6df, div.KZmu8e', timeout=10000)
                                 time.sleep(2)
                                 
                                 print("[SerpScan] Scrolling Shopping List...")
                                 for _ in range(3):
                                     page.mouse.wheel(0, 800)
                                     time.sleep(1)
                                     
                             except Exception as e:
                                 print(f"[SerpScan] Shopping grid wait warning: {e}")
                                 time.sleep(1)
                        else:
                             print("[SerpScan] Could not find 'Shopping' tab link.")

                    elif detected_intent == 'ORGANIC':
                        # Fallback / Default
                        pass

                except Exception as e:
                    print(f"[SerpScan] Error during navigation check: {e}")
                    pass
                
                # --- Post-Navigation Scraping ---
                
                # Check if we are now on a different URL than the initial SERP
                detected_move = page.url != final_url
                
                if detected_move:
                    print(f"[SerpScan] Navigation occurred. Current URL: {page.url}")
                    
                    if detected_intent == 'HOTELS':
                         try:
                            # Hotel specific scraping
                            # Hotels often load lazily. Ensure we capture enough.
                            hotel_html = page.content()
                            print(f"[SerpScan] Captured Hotel List ({len(hotel_html)} bytes)")
                            
                            content += f"\n<!-- HOTELS_HTML_START -->\n{hotel_html}\n<!-- HOTELS_HTML_END -->"
                         except Exception as e:
                             print(f"[SerpScan] Error scraping Hotel content: {e}")

                    elif detected_intent == 'SHOPPING':
                        try:
                            shopping_html = page.content()
                            print(f"[SerpScan] Captured Shopping List ({len(shopping_html)} bytes)")
                            content += f"\n<!-- SHOPPING_HTML_START -->\n{shopping_html}\n<!-- SHOPPING_HTML_END -->"
                        except Exception as e:
                            print(f"[SerpScan] Error scraping Shopping content: {e}")

                    else:
                        # Assume Local Finder (PLACES)
                        try:
                            # Wait for list to load
                            try:
                                page.wait_for_selector('div[role="article"]', timeout=5000)
                            except:
                                time.sleep(3)
                                
                            # Scroll to load more results (Local Finder is infinite scroll)
                            # Ensure we are hovering the feed to avoid map zoom
                            try:
                                page.hover('div[role="feed"]', timeout=2000)
                            except:
                                # Fallback: move mouse to left side
                                try:
                                    page.mouse.move(200, 400) 
                                except:
                                    pass

                            for _ in range(3):
                                page.mouse.wheel(0, 1000)
                                time.sleep(1)
                            
                            local_finder_html = page.content()
                            print(f"[SerpScan] Captured Local Finder ({len(local_finder_html)} bytes)")
                            
                            # Append to content with marker
                            content += f"\n<!-- LOCAL_FINDER_HTML_START -->\n{local_finder_html}\n<!-- LOCAL_FINDER_HTML_END -->"
                            
                        except Exception as e:
                            print(f"[SerpScan] Extended Local Pack Error: {e}")

                
                return content, final_url
                
            except Exception as e:
                print(f"[SerpScan] Error: {e}")
                import traceback
                traceback.print_exc()
                return None, None
                
            finally:
                try:
                    context.close()
                except:
                    pass

