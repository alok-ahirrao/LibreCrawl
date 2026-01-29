import time
import random
import threading
import atexit
import contextlib
import numpy as np
from math import cos, sin, radians, sqrt
from playwright.sync_api import sync_playwright
try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    stealth_sync = None
from ..config import config
from ..logger import log
from .stealth_config import (
    STEALTH_LAUNCH_ARGS,
    STEALTH_INIT_SCRIPT,
    DESKTOP_USER_AGENTS,
    DESKTOP_VIEWPORTS,
    MOBILE_USER_AGENTS,
    MOBILE_VIEWPORTS,
    random_delay,
    human_like_mouse_move,
    human_like_scroll,
    apply_stealth_to_context,
    setup_request_interception,
    get_random_profile,
)


class PlaywrightManager:
    """
    Singleton manager that maintains THREAD-LOCAL Playwright instances.
    This fixes 'greenlet' errors by ensuring each thread uses its own Playwright loop.
    """
    _instance = None
    _lock = threading.Lock()
    _semaphore = threading.Semaphore(16) # Balanced limit: 16 browsers (Faster than 8, safer than 32)
    _local = threading.local()
    
    @classmethod
    def get_instance(cls):
        """Get the singleton manager instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize Manager."""
        log.debug("[PlaywrightManager] Initialized Thread-Local Manager")
        
    def _get_local_playwright(self):
        """Get or create thread-local Playwright instance."""
        if not hasattr(self._local, 'playwright'):
            self._local.playwright = sync_playwright().start()
            log.debug(f"[PlaywrightManager] Thread {threading.get_ident()} initialized Playwright")
        return self._local.playwright

    @contextlib.contextmanager
    def managed_browser(self, headless: bool = True, proxy_url: str = None, launch_args: list = None):
        """
        Yields a THREAD-LOCAL browser instance.
        Uses a Semaphore to limit total concurrent browsers across the system.
        """
        # Acquire slot
        self._semaphore.acquire()
        try:
            p = self._get_local_playwright()
            
            # Default launch args - use comprehensive stealth args
            if launch_args is None:
                launch_args = STEALTH_LAUNCH_ARGS
            
            # Launch options
            launch_opts = {
                'headless': headless,
                'args': launch_args
            }
            if proxy_url:
                launch_opts['proxy'] = {'server': proxy_url}
            
            # Check for existing browser in this thread
            if not hasattr(self._local, 'browser') or self._local.browser is None:
                self._local.browser = p.chromium.launch(**launch_opts)
            else:
                # Optional: Check if connected, restart if valid?
                try:
                    if not self._local.browser.is_connected():
                         self._local.browser = p.chromium.launch(**launch_opts)
                except:
                    self._local.browser = p.chromium.launch(**launch_opts)
            
            yield self._local.browser
            
        finally:
            self._semaphore.release()
            
            # We DONT close the browser here to allow reuse within the thread!
            # Waitress threads are reused. This enables persistent browsers per thread.
            # But we must be careful about context cleanup (already handled by scan functions creating new_context)

    # Deprecated methods
    def get_browser(self, *args, **kwargs):
        raise RuntimeError("Use managed_browser()")
    
    def close_browser(self):
        """Close current thread's browser if tracked (not applicable for new-instance-per-call)."""
        pass
    
    def shutdown(self):
        """Cannot safely shutdown thread-local instances from main thread."""
        pass


# Global manager - lazily initialized
_playwright_manager = None
_playwright_lock = threading.Lock()


def get_playwright_manager():
    """Get or create the global PlaywrightManager instance."""
    global _playwright_manager
    if _playwright_manager is None:
        with _playwright_lock:
            if _playwright_manager is None:
                _playwright_manager = PlaywrightManager.get_instance()
    return _playwright_manager


# Register cleanup on exit
def _cleanup_playwright():
    global _playwright_manager
    if _playwright_manager:
        _playwright_manager.shutdown()
        _playwright_manager = None


atexit.register(_cleanup_playwright)


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
        """
        Create a browser context with comprehensive stealth and geolocation settings.
        Uses playwright_stealth if available, plus custom anti-detection scripts.
        """
        timezone = self._get_timezone_for_coords(lng)
        viewport = random.choice(DESKTOP_VIEWPORTS)
        user_agent = random.choice(DESKTOP_USER_AGENTS)
        
        context_options = {
            'geolocation': {'latitude': lat, 'longitude': lng, 'accuracy': 50 + random.randint(0, 100)},
            'permissions': ['geolocation'],
            'viewport': viewport,
            'locale': 'en-US',
            'timezone_id': timezone,
            'user_agent': user_agent,
            # Additional stealth options
            'color_scheme': 'light',
            'reduced_motion': 'no-preference',
            'has_touch': False,
            'java_script_enabled': True,
            'bypass_csp': True,
            'ignore_https_errors': True,
        }
        
        # Add proxy if configured
        if self.proxy_url:
            context_options['proxy'] = {'server': self.proxy_url}
        
        context = browser.new_context(**context_options)
        
        # Apply playwright_stealth if available (comprehensive anti-detection)
        if STEALTH_AVAILABLE:
            page_for_stealth = context.new_page()
            try:
                stealth_sync(page_for_stealth)
                log.debug("[Stealth] playwright_stealth applied successfully")
            except Exception as e:
                log.debug(f"[Stealth] playwright_stealth failed: {e}")
            page_for_stealth.close()
        
        # Add comprehensive stealth init script from stealth_config
        context.add_init_script(STEALTH_INIT_SCRIPT)
        log.debug("[Stealth] Comprehensive anti-detection script injected")
        
        # Set up request interception to block trackers
        try:
            setup_request_interception(context)
            log.debug("[Stealth] Tracker blocking enabled")
        except Exception as e:
            log.debug(f"[Stealth] Tracker blocking failed: {e}")
        
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
        
        log.debug(f"[Geo] Resolving coordinates for '{location_name}'...")
        log.debug(f"[Geo] Resolving coordinates for '{location_name}'...")
        
        # [FIX] Use PlaywrightManager singleton
        browser = None
        browser_ctx = None
        try:
            pm = get_playwright_manager()
            browser_ctx = pm.managed_browser(
                headless=self.headless,
                launch_args=['--disable-blink-features=AutomationControlled']
            )
            browser = browser_ctx.__enter__()
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
                    log.debug(f"[Geo] Resolved '{location_name}' -> ({lat}, {lng})")
                    return lat, lng
                else:
                    log.debug(f"[Geo] Could not extract coordinates from URL: {target_url}")
                    return None, None
            except Exception as e:
                log.debug(f"[Geo] Resolution error: {e}")
                return None, None
            finally:
                pass
                
        except:
            pass
        finally:
            if browser_ctx:
                try:
                    browser_ctx.__exit__(None, None, None)
                except:
                    pass

    def scan_grid_point(self, keyword: str, lat: float, lng: float, fast_mode: bool = False, target_name: str = None) -> str:
        """
        Perform a search for 'keyword' at the precise 'lat, lng'.
        Returns the raw HTML of the results page for parsing.
        
        Args:
            keyword: Search term
            lat: Latitude
            lng: Longitude
            fast_mode: If True, blocks images/fonts for speed
            target_name: Optional business name to look for. If found, stops scrolling early.
        """
        browser = None
        browser_ctx = None
        context = None
        page = None
        
        try:
            # [FIX] Use dummy block to maintain indentation of existing code (16 spaces)
            if True:
                # [FIX] Use PlaywrightManager singleton
                pm = get_playwright_manager()
                
                # Launch with stealth args
                launch_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-accelerated-2d-canvas',
                    '--disable-gpu'
                ]
                
                browser_ctx = pm.managed_browser(
                    headless=self.headless,
                    launch_args=launch_args
                )
                browser = browser_ctx.__enter__()
            
            context = self._create_stealth_context(browser, lat, lng)
            
            # FAST MODE: Block heavy resources
            if fast_mode:
                context.route("**/*", lambda route: route.abort() 
                    if route.request.resource_type in ["image", "media", "font", "stylesheet"] 
                    else route.continue_())
                    
            page = context.new_page()
            
            if True: # Fixed indentation wrapper
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
                    
                     
                    # Enhanced scrolling logic to ensure we load enough results (depth)
                    # We aim to load at least 60 results (rank #60) to catch lower rankings
                    target_count = 20 if fast_mode else 60
                    # Increase max scrolls to ensure we reach depth
                    max_scrolls = 6 if fast_mode else 16
                    
                    listing_selector = 'div[role="article"]'
                    current_count = 0
                    
                    # Target name normalization for check
                    target_name_lower = target_name.lower().strip() if target_name else None
                    
                    for i in range(max_scrolls):
                        # --- EARLY EXIT OPTIMIZATION ---
                        # If target_name is provided, check if it's already in the loaded content
                        if target_name_lower:
                            try:
                                # Quick check using page text content (fastest) - scope to feed
                                feed_text = page.locator(sidebar_selector).text_content()
                                if feed_text:
                                    feed_text_lower = feed_text.lower()
                                    # Simple validation: Check if target (e.g. "Bar") is distinct from "Barber"
                                    # We check if it is surrounded by non-alphanumeric chars or boundaries
                                    # Using a simple heuristic for speed without heavy regex overhead in the loop
                                    if target_name_lower in feed_text_lower:
                                        # Double check basic boundaries to avoid simple substrings like "Bar" in "Barber"
                                        # This is a lightweight robust check
                                        import re
                                        if re.search(rf"\b{re.escape(target_name_lower)}\b", feed_text_lower):
                                            log.debug(f"[Geo] EARLY EXIT: Found target '{target_name}' at scroll {i+1}")
                                            break
                            except:
                                pass
                        
                        # Scroll down
                        page.eval_on_selector(
                            sidebar_selector, 
                            '(el) => el.scrollTop += 2000'  # Larger scroll step
                        )
                        
                        # Wait for loading
                        if fast_mode:
                            time.sleep(random.uniform(0.5, 0.8))
                        else:
                            time.sleep(random.uniform(0.8, 1.2))
                            
                        # Check count
                        try:
                            items = page.locator(listing_selector).all()
                            current_count = len(items)
                            
                            # If we have reached target count AND we are not searching for a specific target 
                            # (or we are searching but haven't found it yet - in which case we continue to max_scrolls)
                            # Actually, if we haven't found it, we WANT to keep scrolling up to max_scrolls/target_count.
                            # So the condition is: stop if enough items loaded. 
                            # But wait: if we are looking for a target that is #50, and target_count=60, we keep going.
                            if current_count >= target_count:
                                break
                        except:
                            pass
                    
                    # Scroll back to top to ensure first items are fully rendered/visible
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
                            listing_items = page.locator(listing_selector).all()
                            # Hover over a few items spread out to trigger lazy loading
                            indices_to_hover = [0, 4, 9, 19, 29, 39, 49]
                            for idx in indices_to_hover:
                                if idx < len(listing_items):
                                    try:
                                        listing_items[idx].hover(timeout=500)
                                        time.sleep(0.1)
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
                    log.debug(f"Scroll warning: {scroll_error}")
                
                # Capture content
                content = page.content()
                
                return content
            
        except Exception as e:
            log.error(f"Error during crawl at {lat},{lng}: {e}")
            return None
            
        finally:
            # Explicitly close page and context to prevent leaks
            # managed_browser reuses the browser, so we MUST cleanup these headers
            if page:
                try: page.close()
                except: pass
            
            if context:
                try: context.close()
                except: pass
                
            if browser_ctx:
                try:
                    browser_ctx.__exit__(None, None, None)
                except:
                    pass
            elif browser:
                # Fallback if managed_browser wasn't used
                try: browser.close()
                except: pass
    
    
    def _resolve_short_url(self, url: str) -> str:
        """Resolve short URLs like maps.app.goo.gl to their full destination."""
        if 'maps.app.goo.gl' not in url and 'goo.gl/maps' not in url:
            return url
            
        try:
            import requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            # Use GET request (HEAD doesn't always follow redirects properly)
            response = requests.get(url, allow_redirects=True, timeout=15, headers=headers)
            resolved_url = response.url
            
            log.debug(f"[URLResolve] Short URL '{url}' resolved to: {resolved_url}")
            
            # Validate the resolved URL - should contain /maps/place/
            if '/maps/place/' in resolved_url or 'google.com/maps' in resolved_url:
                return resolved_url
            else:
                log.debug(f"[URLResolve] ⚠️ Resolved URL doesn't look like a valid place URL, using original")
                return url
        except Exception as e:
            log.debug(f"[URLResolve] Error resolving short URL: {e}")
            return url

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
        # Resolve short URLs first to avoid partial loading or list views
        place_url = self._resolve_short_url(place_url)
        log.debug(f"[ScanPlaceDetails] Resolved URL: {place_url}")

        browser = None
        browser_ctx = None
        context = None
        page = None
        try:
            # [FIX] Use dummy block to maintain indentation of existing code (16 spaces)
            if True:
                # [FIX] Use PlaywrightManager singleton
                pm = get_playwright_manager()
                
                browser_ctx = pm.managed_browser(
                    headless=self.headless,
                    launch_args=['--disable-blink-features=AutomationControlled']
                )
                browser = browser_ctx.__enter__()
            
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
            
            if True: # wrapper to maintain indentation
                # Use networkidle to wait for JS content to load
                page.goto(place_url, wait_until='networkidle', timeout=self.timeout)
                
                # Check if we landed on a search results list instead of a specific place
                # This happens with short URLs like maps.app.goo.gl/... that resolve to a search query
                try:
                    # Wait briefly to see if it's a list view
                    page.wait_for_selector('div[role="feed"]', timeout=4000)
                    log.debug("[ScanPlaceDetails] Detected search results list - clicking first result...")
                    
                    # Find first result
                    first_result = page.locator('div[role="feed"] > div > div[jsaction] a[href*="/maps/place/"]').first
                    if first_result.is_visible():
                        # Click it and wait for details to load
                        first_result.click()
                        time.sleep(random.uniform(4.5, 5.5))
                except:
                    # Not a list view, or timed out waiting for it - assume it's the direct page or will load
                    pass

                # ========== ENHANCED WAITING FOR CONTENT ==========
                
                # Wait for the main business name (h1) to appear first
                try:
                    page.wait_for_selector('h1', timeout=10000)
                except:
                    log.debug("[ScanPlaceDetails] ⚠️ H1 title not found after 10s")

                # Click tabs to load more data
                try:
                    # 1. Click "Reviews" tab - handle "Reviews" or "Reviews (123)"
                    try:
                        reviews_tab = page.locator('button[role="tab"][aria-label*="Reviews"], div[role="tab"][aria-label*="Reviews"]').first
                        if reviews_tab.count() > 0:
                             log.debug("[ScanPlaceDetails] Found Reviews tab, clicking...")
                             reviews_tab.click(force=True)
                             time.sleep(2.0)
                             
                             # Wait for at least one review to appear
                             try:
                                 page.wait_for_selector('div[data-review-id], div[class*="review"]', timeout=5000)
                                 log.debug("[ScanPlaceDetails] Review content loaded.")
                             except:
                                 log.debug("[ScanPlaceDetails] Timeout waiting for review content.")
                        else:
                             log.debug("[ScanPlaceDetails] Reviews tab NOT found.")
                    except Exception as e:
                        log.debug(f"[ScanPlaceDetails] Error clicking Reviews tab: {e}")
                        
                    # 2. Click "About" tab
                    try:
                        about_tab = page.locator('button[role="tab"][aria-label*="About"], div[role="tab"][aria-label*="About"]').first
                        if about_tab.count() > 0:
                             about_tab.click(force=True)
                             time.sleep(1.0)
                    except: pass
                        
                    # 3. Go back to Overview 
                    try:
                        overview_tab = page.locator('button[role="tab"][aria-label*="Overview"], div[role="tab"][aria-label*="Overview"]').first
                        if overview_tab.count() > 0:
                             overview_tab.click(force=True)
                             time.sleep(0.5)
                    except: pass
                        
                except Exception as e:
                    log.debug(f"[ScanPlaceDetails] Tab interaction warning: {e}")
                        
                except Exception as e:
                    log.debug(f"[ScanPlaceDetails] Tab interaction warning: {e}")

                # Scroll to load dynamic content (Q&A, Competitors, Reviews)
                try:
                    # Target the main scrollable container
                    page.evaluate('''() => {
                        const main = document.querySelector('div[role="main"]');
                        if (main) {
                            main.scrollTo(0, 500);
                            setTimeout(() => main.scrollTo(0, 1500), 500);
                            setTimeout(() => main.scrollTo(0, 3000), 1000);
                            setTimeout(() => main.scrollTo(0, main.scrollHeight), 1500);
                        }
                            setTimeout(() => main.scrollTo(0, main.scrollHeight), 1500);
                        }
                    }''')
                    log.debug("[ScanPlaceDetails] Scrolling to load dynamic content...")
                    time.sleep(3.0)
                    
                    # Keyboard scroll to trigger interaction observers
                    try:
                        page.click('div[role="main"]', timeout=2000)
                        for _ in range(3):
                            page.keyboard.press('End')
                            time.sleep(1.5)
                    except:
                        pass
                    
                except Exception as e:
                     log.debug(f"[ScanPlaceDetails] Scroll warning: {e}")
                
                # Debug screenshot to check what we see
                try:
                    page.screenshot(path="crawl_debug_view.png")
                    log.debug("[ScanPlaceDetails] Saved debug screenshot to crawl_debug_view.png")
                except:
                    pass

                # Capture the final URL after any redirects
                final_url = page.url
                log.debug(f"[ScanPlaceDetails] Final URL after redirect: {final_url}")
                
                # Get page content
                html_content = page.content()
                
                # Quick validation
                if html_content and len(html_content) < 15000: # Increased threshold again
                    log.debug("[ScanPlaceDetails] ⚠️ HTML content seems too short/incomplete, waiting longer...")
                    time.sleep(3)
                    html_content = page.content()
                
                return html_content, final_url
                
        except Exception as e:
            log.error(f"Error scraping place details: {e}")
            return None, None
            
        finally:
            if page:
                try: page.close()
                except: pass
            if context:
                try: context.close()
                except: pass

            # Force close browser to prevent lingering process
            if browser and browser.is_connected():
                try: browser.close()
                except: pass

            if browser_ctx:
                try:
                    browser_ctx.__exit__(None, None, None)
                except:
                    pass
            elif browser:
                try: browser.close()
                except: pass
    
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
        
        # Determine if we should use geo-spoofing or let Google figure it out
        # If user provides a location STRING but no coordinates, skip geo-spoofing
        # to let Google naturally find businesses in that location
        use_geolocation = (lat is not None and lng is not None)
        target_lat = lat if lat is not None else 40.7580
        target_lng = lng if lng is not None else -73.9855
        
        browser = None
        browser_ctx = None
        context = None
        page = None
        try:
            # [FIX] Use dummy block to maintain indentation of existing code (16 spaces)
            if True:
                # [FIX] Use PlaywrightManager singleton
                pm = get_playwright_manager()
                
                launch_args = [
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                ]
                
                browser_ctx = pm.managed_browser(
                    headless=self.headless,
                    launch_args=launch_args
                )
                browser = browser_ctx.__enter__()
            
            # If we have specific coordinates, use stealth context with geo-spoofing
            # Otherwise, use a simple context and let Google handle location from search query
            if use_geolocation:
                context = self._create_stealth_context(browser, target_lat, target_lng)
            else:
                # Simple context without geo-spoofing - lets Google use search query for location
                context = browser.new_context(
                    user_agent=self._get_random_user_agent(),
                    viewport=self._get_random_viewport()
                )
            page = context.new_page()
            
            if True: # wrapper to maintain indentation
                # Navigate to Google Maps with business search
                url = f'https://www.google.com/maps/search/{search_query}'
                log.debug(f"[BusinessSearch] Navigating to: {url}")
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
                log.debug(f"[BusinessSearch] Captured {len(content)} bytes of HTML from {final_url}")
                return content, final_url
                
        except Exception as e:
            log.debug(f"[BusinessSearch] Error searching for '{query}': {e}")
            while True:  # Small hack to ensure we break out of retry loop if we had one, or just return
                 return None, None
                 
        finally:
            if page:
                try:
                    page.close()
                except:
                    pass
            if context:
                try:
                    context.close()
                except:
                    pass

            # Force close browser to prevent lingering process
            if browser and browser.is_connected():
                try: browser.close()
                except: pass

            if browser_ctx:
                try:
                    browser_ctx.__exit__(None, None, None)
                except:
                    pass
            elif browser:
                try: browser.close()
                except: pass
    
    def scan_serp_fast(self, keyword: str, location: str = "United States", 
                       device: str = "desktop", depth: int = 10, language: str = "en") -> tuple:
        """
        Fast SERP scan using requests (no browser overhead).
        Best for country-level searches without precise geolocation needs.
        
        This method supports three modes:
        1. ScraperAPI - Uses ScraperAPI.com to handle JS rendering (recommended)
        2. SerpAPI - Uses SerpAPI.com for structured SERP data
        3. Direct requests - Falls back to direct HTTP (may be blocked by Google)
        
        Configure via environment variables:
        - SERP_API_PROVIDER: 'scraperapi', 'serpapi', or 'none'
        - SERP_API_KEY: Your API key
        
        Args:
            keyword: Search query
            location: Location name (country or city)
            device: 'desktop' or 'mobile'
            depth: Number of results to request (10, 20, 50, 100)
            language: Language code (e.g., 'en', 'es')
            
        Returns:
            Tuple of (HTML content, final_url, success_flag)
        """
        import requests
        from urllib.parse import quote_plus
        from ..config import config
        
        log.debug(f"[SerpFast] Starting fast SERP scan for '{keyword}' in '{location}'")
        
        # Check if a SERP API provider is configured
        api_provider = config.SERP_API_PROVIDER
        api_key = config.SERP_API_KEY
        
        if api_provider != 'none' and api_key:
            log.debug(f"[SerpFast] Using {api_provider.upper()} for fast mode")
            return self._scan_serp_via_api(keyword, location, device, depth, language, api_provider, api_key)
        
        # Fall back to direct requests (may be blocked)
        log.debug(f"[SerpFast] No SERP API configured, attempting direct request...")
        
        
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
            'singapore': 'sg',
            'new zealand': 'nz',
            'ireland': 'ie',
            'south africa': 'za',
            'uae': 'ae',
            'united arab emirates': 'ae',
        }
        
        # Google domain mapping
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
            'sg': 'google.com.sg',
            'nz': 'google.co.nz',
            'ie': 'google.ie',
            'za': 'google.co.za',
            'ae': 'google.ae',
        }
        
        # Determine GL code
        cleaned_loc = location.lower().strip()
        gl_code = LOCATION_MAP.get(cleaned_loc, 'us')
        google_domain = GOOGLE_DOMAINS.get(gl_code, 'google.com')
        
        # Generate UULE for more specific locations (cities)
        uule = ""
        if cleaned_loc not in LOCATION_MAP:
            # It's a city or specific location, generate UULE
            uule = self._generate_uule(location)
            log.debug(f"[SerpFast] Generated UULE for '{location}'")
        
        # Device-specific user agents
        if device.lower() == 'mobile':
            user_agents = [
                'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1',
            ]
        else:
            user_agents = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            ]
        
        # Select random user agent
        import random
        user_agent = random.choice(user_agents)
        
        # Build headers to mimic real browser
        headers = {
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': f'{language}-{gl_code.upper()},{language};q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?1' if device.lower() == 'mobile' else '?0',
            'sec-ch-ua-platform': '"Android"' if device.lower() == 'mobile' else '"Windows"',
        }
        
        # Build search URL
        search_url = f'https://www.{google_domain}/search?q={quote_plus(keyword)}&hl={language}&num={depth}'
        
        if gl_code:
            search_url += f'&gl={gl_code}'
        
        if uule:
            search_url += f'&uule={uule}'
        
        log.debug(f"[SerpFast] Fetching: {search_url}")
        
        # Configure session with proxy if available
        session = requests.Session()
        session.headers.update(headers)
        
        proxies = None
        if self.proxy_url:
            proxies = {
                'http': self.proxy_url,
                'https': self.proxy_url
            }
            log.debug(f"[SerpFast] Using proxy: {self.proxy_url[:30]}...")
        
        # Add cookies that indicate a real browser session
        # This can help bypass some JavaScript requirement checks
        session.cookies.set('CONSENT', 'YES+', domain='.google.com')
        session.cookies.set('SOCS', 'CAISHAgBEhJnd3NfMjAyNDAxMDktMF9SQzEaAmVuIAEaBgiA_K6tBg', domain='.google.com')
        
        try:
            # Make the request
            response = session.get(
                search_url,
                proxies=proxies,
                timeout=15,
                allow_redirects=True
            )
            
            log.debug(f"[SerpFast] Response: {response.status_code}, {len(response.text)} bytes")
            
            # Check for success
            if response.status_code != 200:
                log.debug(f"[SerpFast] Non-200 status code: {response.status_code}")
                return None, None, False
            
            html_content = response.text
            final_url = response.url
            
            # Check for CAPTCHA or blocks
            captcha_indicators = ['unusual traffic', 'captcha', 'recaptcha', 'verify you', 'not a robot', 'detected unusual traffic']
            html_lower = html_content[:10000].lower()
            
            if any(indicator in html_lower for indicator in captcha_indicators):
                log.debug("[SerpFast] ⚠️ CAPTCHA/Block detected - falling back to browser mode")
                return None, None, False
            
            # Check for JavaScript-required page (Google's JS gate)
            js_required_indicators = ['please click', 'enable javascript', 'javascript is required', 'enablejs']
            if any(indicator in html_lower for indicator in js_required_indicators):
                log.debug("[SerpFast] ⚠️ JavaScript required page - falling back to browser mode")
                return None, None, False
            
            # Check for valid search results - look for actual result containers
            has_results = any([
                'id="rso"' in html_content,
                "id='rso'" in html_content,
                'class="g"' in html_content,
                'data-hveid=' in html_content,
                'data-sokoban' in html_content,  # Modern Google result attribute
                '<cite' in html_content,  # URLs in results
            ])
            
            if not has_results:
                log.debug("[SerpFast] ⚠️ No search results found in response - may be blocked")
                return None, None, False
            
            log.debug(f"[SerpFast] ✓ Successfully fetched SERP ({len(html_content)} bytes)")
            return html_content, final_url, True
            
        except requests.exceptions.Timeout:
            log.debug("[SerpFast] ⚠️ Request timed out")
            return None, None, False
        except requests.exceptions.ConnectionError as e:
            log.debug(f"[SerpFast] ⚠️ Connection error: {e}")
            return None, None, False
        except Exception as e:
            log.debug(f"[SerpFast] ⚠️ Error: {e}")
            return None, None, False
    
    def _scan_serp_via_api(self, keyword: str, location: str, device: str, 
                            depth: int, language: str, provider: str, api_key: str) -> tuple:
        """
        Fetch SERP using a third-party API service.
        
        Supported providers:
        - scraperapi: ScraperAPI.com - Renders JS and returns HTML
        - serpapi: SerpAPI.com - Returns structured JSON (converted to HTML-like format)
        """
        import requests
        from urllib.parse import quote_plus
        
        # Map location to country code for APIs
        LOCATION_MAP = {
            'united states': 'us', 'usa': 'us',
            'united kingdom': 'gb', 'uk': 'gb',
            'canada': 'ca', 'australia': 'au', 'germany': 'de',
            'france': 'fr', 'india': 'in', 'brazil': 'br',
            'mexico': 'mx', 'spain': 'es', 'italy': 'it',
            'netherlands': 'nl', 'japan': 'jp',
        }
        
        cleaned_loc = location.lower().strip()
        country_code = LOCATION_MAP.get(cleaned_loc, 'us')
        
        try:
            if provider == 'scraperapi':
                # ScraperAPI - Returns rendered HTML
                # https://www.scraperapi.com/documentation/
                target_url = f"https://www.google.com/search?q={quote_plus(keyword)}&hl={language}&gl={country_code}&num={depth}"
                
                api_url = f"http://api.scraperapi.com?api_key={api_key}&url={quote_plus(target_url)}&render=true"
                
                if device.lower() == 'mobile':
                    api_url += "&device_type=mobile"
                
                log.debug(f"[SerpAPI] Fetching via ScraperAPI...")
                response = requests.get(api_url, timeout=60)
                
                if response.status_code == 200:
                    html_content = response.text
                    log.debug(f"[SerpAPI] ✓ ScraperAPI returned {len(html_content)} bytes")
                    return html_content, target_url, True
                else:
                    log.debug(f"[SerpAPI] ⚠️ ScraperAPI returned status {response.status_code}")
                    return None, None, False
                    
            elif provider == 'serpapi':
                # SerpAPI - Returns structured JSON
                # https://serpapi.com/search-api
                api_url = "https://serpapi.com/search"
                params = {
                    'api_key': api_key,
                    'q': keyword,
                    'hl': language,
                    'gl': country_code,
                    'num': depth,
                    'engine': 'google',
                    'output': 'html'  # Get HTML instead of JSON for compatibility
                }
                
                if device.lower() == 'mobile':
                    params['device'] = 'mobile'
                
                log.debug(f"[SerpAPI] Fetching via SerpAPI...")
                response = requests.get(api_url, params=params, timeout=60)
                
                if response.status_code == 200:
                    html_content = response.text
                    log.debug(f"[SerpAPI] ✓ SerpAPI returned {len(html_content)} bytes")
                    return html_content, f"https://www.google.com/search?q={quote_plus(keyword)}", True
                else:
                    log.debug(f"[SerpAPI] ⚠️ SerpAPI returned status {response.status_code}: {response.text[:200]}")
                    return None, None, False
            
            else:
                log.debug(f"[SerpAPI] ⚠️ Unknown provider: {provider}")
                return None, None, False
                
        except requests.exceptions.Timeout:
            log.debug(f"[SerpAPI] ⚠️ API request timed out")
            return None, None, False
        except Exception as e:
            log.debug(f"[SerpAPI] ⚠️ Error: {e}")
            return None, None, False
    
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
            log.debug(f"[SerpScan] Location '{location}' needs coordinates. Resolving via API...")
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
                    
                    log.debug(f"[SerpScan] Resolved coordinates: {lat}, {lng} -> GL: {gl_code}")
                else:
                    log.debug(f"[SerpScan] Could not resolve coordinates for '{location}'")
            except Exception as e:
                log.debug(f"[SerpScan] Geocoding error: {e}")
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
                        
                    log.debug(f"[SerpScan] Fallback resolved: {lat}, {lng} -> GL: {gl_code}")



        uule = ""
        # Generate UULE if it's a custom location (not a country code match)
        # BUT skip UULE if we have precise coordinates to avoid conflict?
        # Actually Google sometimes prefers UULE over Geo, so let's keep it but prioritize Geo in context
        if hasattr(self, '_generate_uule') and location.strip() and not gl_code:
             # Only use UULE if we didn't resolve coordinates? 
             # No, UULE is good backup. But if we have lat/lng, maybe we don't strictly need it?
             # Let's keep it for now but log it.
            uule = self._generate_uule(location)
            log.debug(f"[SerpScan] Generated UULE for '{location}': {uule}")
        
        # [FIX] If we have lat/lng but no gl_code yet, infer gl_code from coordinates
        # This is essential for product/shopping searches which rely on gl parameter
        if lat is not None and lng is not None and not gl_code:
            log.debug(f"[SerpScan] Inferring GL code from coordinates: {lat}, {lng}")
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
                log.debug(f"[SerpScan] Inferred GL code from coordinates: {gl_code}")
        
        # Device viewport settings
        if device.lower() == 'mobile':
            viewport = {'width': 375, 'height': 812}
            user_agent = 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1'
        else:
            viewport = self._get_random_viewport()
            user_agent = self._get_random_user_agent()
        
        # [FIX] Global try-except to prevent Playwright crashes from killing the server
        browser = None
        context = None
        browser_ctx = None
        try:
            # [FIX] Use PlaywrightManager singleton to avoid event loop corruption
            pm = get_playwright_manager()
            
            # [OPTIMIZED] Enhanced launch args for better stealth and performance
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-infobars',
                '--disable-gpu',
                '--no-first-run',
                '--no-default-browser-check',
                '--disable-extensions',
                '--disable-background-networking',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-component-update',
                '--disable-hang-monitor',
                '--disable-ipc-flooding-protection',
                '--disable-popup-blocking',
                '--disable-prompt-on-repost',
                '--disable-renderer-backgrounding',
                '--disable-sync',
                '--metrics-recording-only',
                '--password-store=basic',
                '--use-mock-keychain',
                '--disable-features=TranslateUI',
                '--disable-features=BlinkGenPropertyTrees',
            ]
            
            # Acquire lock and browser via context manager manually
            browser_ctx = pm.managed_browser(
                headless=self.headless,
                proxy_url=self.proxy_url,
                launch_args=launch_args
            )
            browser = browser_ctx.__enter__()
            
            # Prepare context options (excluding user_data_dir for fresh context)
            context_options = {
                'viewport': viewport,
                'user_agent': user_agent,
                'java_script_enabled': True,
                'bypass_csp': True,
                'ignore_https_errors': True,
                # [NEW] Additional stealth settings
                'color_scheme': 'light',
                'reduced_motion': 'no-preference',
                'has_touch': device.lower() == 'mobile',
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
                # [FIX] Use higher accuracy (10m) for better local pack results
                context_options['geolocation'] = {'latitude': lat, 'longitude': lng, 'accuracy': 10}
                context_options['permissions'] = ['geolocation']
                context_options['timezone_id'] = tz_id
                log.debug(f"[SerpScan] 📍 Geolocation set: ({lat}, {lng}) with 10m accuracy, TZ: {tz_id}")
                
            # Add Locale
            locale_suffix = gl_code.upper() if gl_code else 'US'
            context_options['locale'] = f'{language}-{locale_suffix}'
            
            # Add Proxy
            if self.proxy_url:
                context_options['proxy'] = {'server': self.proxy_url}
            
            # Create context from the browser we got from PlaywrightManager
            context = browser.new_context(**context_options)

            # [OPTIMIZED] Enhanced stealth scripts for human-like behavior
            context.add_init_script("""
                // === CORE WEBDRIVER HIDING ===
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                    configurable: true
                });
                
                // Delete automation indicators
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
                delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
                
                // === PLUGINS (Realistic) ===
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        const plugins = [
                            { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                            { name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: 'Portable Document Format', length: 1 },
                            { name: 'Native Client', filename: 'internal-nacl-plugin', description: '', length: 2 },
                            { name: 'Chromium PDF Viewer', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 }
                        ];
                        plugins.item = (i) => plugins[i];
                        plugins.namedItem = (name) => plugins.find(p => p.name === name);
                        plugins.refresh = () => {};
                        Object.setPrototypeOf(plugins, PluginArray.prototype);
                        return plugins;
                    }
                });
                
                // === LANGUAGES ===
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
                
                // === PLATFORM ===
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32'
                });
                
                // === HARDWARE VALUES (Randomized realistic) ===
                const cores = [4, 6, 8, 12, 16][Math.floor(Math.random() * 5)];
                const memory = [4, 8, 16, 32][Math.floor(Math.random() * 4)];
                Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => cores });
                Object.defineProperty(navigator, 'deviceMemory', { get: () => memory });
                
                // === CHROME OBJECT ===
                window.chrome = {
                    runtime: {
                        connect: () => ({}),
                        sendMessage: () => {},
                        onMessage: { addListener: () => {} }
                    },
                    loadTimes: function() {
                        return {
                            requestTime: Date.now() / 1000 - Math.random() * 10,
                            startLoadTime: Date.now() / 1000 - Math.random() * 5,
                            firstPaintTime: Date.now() / 1000 - Math.random() * 3,
                            finishDocumentLoadTime: Date.now() / 1000 - Math.random() * 2,
                            finishLoadTime: Date.now() / 1000 - Math.random(),
                            navigationType: 'Other'
                        };
                    },
                    csi: function() { return { pageT: Date.now(), startE: Date.now() - Math.random() * 1000 }; },
                    app: { isInstalled: false, getDetails: () => null, getIsInstalled: () => false, installState: () => 'not_installed' }
                };
                
                // === PERMISSIONS API ===
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => {
                    if (parameters.name === 'notifications') {
                        return Promise.resolve({ state: 'default', onchange: null });
                    }
                    return originalQuery.call(window.navigator.permissions, parameters);
                };
                
                // === WEBGL FINGERPRINT ===
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(parameter) {
                    if (parameter === 37445) return 'Intel Inc.';
                    if (parameter === 37446) return 'Intel Iris OpenGL Engine';
                    if (parameter === 7937) return 'WebKit WebGL';
                    return getParameter.apply(this, arguments);
                };
                
                // === CANVAS FINGERPRINT PROTECTION ===
                const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function(type) {
                    if (this.width === 16 && this.height === 16) {
                        // Likely fingerprinting, add noise
                        const context = this.getContext('2d');
                        if (context) {
                            const imageData = context.getImageData(0, 0, this.width, this.height);
                            for (let i = 0; i < imageData.data.length; i += 4) {
                                imageData.data[i] ^= (Math.random() * 2) | 0;
                            }
                            context.putImageData(imageData, 0, 0);
                        }
                    }
                    return originalToDataURL.apply(this, arguments);
                };
                
                // === WEB AUDIO FINGERPRINT ===
                const originalGetChannelData = AudioBuffer.prototype.getChannelData;
                AudioBuffer.prototype.getChannelData = function(channel) {
                    const array = originalGetChannelData.call(this, channel);
                    for (let i = 0; i < array.length; i += 100) {
                        array[i] += (Math.random() * 0.0001);
                    }
                    return array;
                };
                
                // === CONNECTION INFO ===
                Object.defineProperty(navigator, 'connection', {
                    get: () => ({
                        effectiveType: '4g',
                        rtt: 50,
                        downlink: 10,
                        saveData: false
                    })
                });
                
                // === BATTERY API (Deprecated but checked) ===
                if ('getBattery' in navigator) {
                    navigator.getBattery = () => Promise.resolve({
                        charging: true,
                        chargingTime: 0,
                        dischargingTime: Infinity,
                        level: 0.99
                    });
                }
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
                
                # [CRITICAL] WARM-UP VISIT - Real users come from Google homepage
                # Bots that directly hit search URLs get flagged immediately
                log.debug(f"[SerpScan] Starting warm-up visit to Google homepage...")
                try:
                    # First visit Google homepage to establish session cookies
                    page.goto(f'https://www.{google_domain}/', wait_until='domcontentloaded', timeout=15000)
                    random_delay(1000, 2500)  # Wait like a human looking at the page
                    
                    # Accept cookies/consent if shown (important for EU regions)
                    try:
                        consent_btns = page.locator('button:has-text("Accept"), button:has-text("I agree"), button:has-text("Accept all")')
                        if consent_btns.count() > 0:
                            consent_btns.first.click()
                            random_delay(500, 1000)
                            log.debug("[SerpScan] Accepted consent dialog")
                    except:
                        pass
                    
                    # Simulate human interaction - move mouse, maybe scroll
                    try:
                        human_like_mouse_move(page, random.randint(300, 600), random.randint(200, 400))
                        random_delay(300, 700)
                    except:
                        pass
                    
                except Exception as warmup_err:
                    log.debug(f"[SerpScan] Warmup warning: {warmup_err}")
                
                # Now navigate to search via the actual search box or direct URL
                search_url = f'https://www.{google_domain}/search?q={quote_plus(keyword)}&hl={language}&num={depth}'
                
                if gl_code:
                    search_url += f'&gl={gl_code}'
                
                if uule:
                    search_url += f'&uule={uule}'
                    
                log.debug(f"[SerpScan] Navigating to search -> {search_url}")
                
                # [STEALTH] Use longer timeout and networkidle for more realistic loading
                try:
                    page.goto(search_url, wait_until='networkidle', timeout=25000)
                    
                    # [NEW] CAPTCHA DETECTION & MANUAL SOLVE WAIT
                    # Check if we hit the "Unusual traffic" or "I'm not a robot" page
                    page_content = page.content().lower()
                    if "unusual traffic" in page_content or "recaptcha" in page_content or "i'm not a robot" in page_content:
                        log.warning("[CAPTCHA] ⚠️ GOOGLE BLOCK DETECTED!")
                        log.warning("[CAPTCHA] The browser is visible - PLEASE SOLVE THE CAPTCHA MANUALLY NOW.")
                        log.warning("[CAPTCHA] The script is paused waiting for search results to appear...")
                        
                        # Wait for user to solve it (up to 5 minutes)
                        # We check for the search results container (#rso) every 2 seconds
                        try:
                            for i in range(150): # 300 seconds
                                time.sleep(2)
                                if page.locator('#rso, #search, .g').count() > 0:
                                    log.info("[CAPTCHA] ✓ access restored! Resuming...")
                                    break
                                if i % 15 == 0:
                                    log.debug(f"[CAPTCHA] Waiting for manual solution... ({i*2}s)")
                        except:
                            pass
                            
                except Exception as nav_err:
                    log.debug(f"[SerpScan] Navigation warning: {nav_err}")
                
                # [OPTIMIZED] Smart wait - check for content before fixed delay
                log.debug(f"[SerpScan] Waiting for search results (depth={depth})...")
                try:
                    # Wait for search results container (max 3s, usually much faster)
                    page.wait_for_selector('#rso, #search, .g', timeout=3000)
                except:
                    pass  # Continue anyway
                
                # [STEALTH] Longer wait for page to fully render - short waits are bot signals
                random_delay(1500, 3000)
                
                # [ENHANCED] Simulate human-like mouse movement and scroll to avoid detection
                try:
                    # Natural Bezier curve mouse movement to a random position
                    human_like_mouse_move(page, random.randint(150, 500), random.randint(200, 400))
                    random_delay(500, 1000)
                    
                    # Simulate casual viewport scroll (like a human reading results)
                    human_like_scroll(page, 'down', random.randint(200, 400))
                    random_delay(800, 1500)
                    human_like_scroll(page, 'up', random.randint(100, 200))
                    random_delay(300, 600)
                except Exception as e:
                    log.debug(f"[Stealth] Human simulation skipped: {e}")
                
                # [ENHANCED] Check for CAPTCHA and handle automatically or wait for manual solving
                def solve_captcha_via_api(site_key: str, page_url: str) -> str:
                    """Attempt to solve reCAPTCHA using 2captcha or anticaptcha API."""
                    from ..config import config
                    import requests
                    
                    if config.CAPTCHA_PROVIDER == 'none' or not config.CAPTCHA_API_KEY:
                        return None
                    
                    api_key = config.CAPTCHA_API_KEY
                    
                    try:
                        if config.CAPTCHA_PROVIDER == '2captcha':
                            # Submit CAPTCHA to 2captcha
                            log.debug(f"[CAPTCHA] Submitting to 2captcha...")
                            submit_url = "http://2captcha.com/in.php"
                            submit_data = {
                                'key': api_key,
                                'method': 'userrecaptcha',
                                'googlekey': site_key,
                                'pageurl': page_url,
                                'json': 1
                            }
                            resp = requests.post(submit_url, data=submit_data, timeout=30)
                            result = resp.json()
                            
                            if result.get('status') != 1:
                                log.debug(f"[CAPTCHA] 2captcha error: {result.get('error_text', 'Unknown error')}")
                                return None
                            
                            captcha_id = result.get('request')
                            log.debug(f"[CAPTCHA] 2captcha submitted, ID: {captcha_id}")
                            
                            # Poll for result
                            for attempt in range(60):  # Max 3 minutes
                                time.sleep(3)
                                check_url = f"http://2captcha.com/res.php?key={api_key}&action=get&id={captcha_id}&json=1"
                                check_resp = requests.get(check_url, timeout=30)
                                check_result = check_resp.json()
                                
                                if check_result.get('status') == 1:
                                    token = check_result.get('request')
                                    log.debug(f"[CAPTCHA] ✓ 2captcha solved!")
                                    return token
                                elif 'CAPCHA_NOT_READY' not in check_result.get('request', ''):
                                    log.debug(f"[CAPTCHA] 2captcha error: {check_result.get('request')}")
                                    return None
                                    
                                if attempt % 10 == 0:
                                    log.debug(f"[CAPTCHA] Waiting for 2captcha... ({attempt * 3}s)")
                        
                        elif config.CAPTCHA_PROVIDER == 'anticaptcha':
                            # Submit CAPTCHA to antiCaptcha
                            log.debug(f"[CAPTCHA] Submitting to antiCaptcha...")
                            submit_url = "https://api.anti-captcha.com/createTask"
                            submit_data = {
                                "clientKey": api_key,
                                "task": {
                                    "type": "RecaptchaV2TaskProxyless",
                                    "websiteURL": page_url,
                                    "websiteKey": site_key
                                }
                            }
                            resp = requests.post(submit_url, json=submit_data, timeout=30)
                            result = resp.json()
                            
                            if result.get('errorId') != 0:
                                log.debug(f"[CAPTCHA] antiCaptcha error: {result.get('errorDescription', 'Unknown error')}")
                                return None
                            
                            task_id = result.get('taskId')
                            log.debug(f"[CAPTCHA] antiCaptcha submitted, ID: {task_id}")
                            
                            # Poll for result
                            for attempt in range(60):
                                time.sleep(3)
                                check_url = "https://api.anti-captcha.com/getTaskResult"
                                check_data = {"clientKey": api_key, "taskId": task_id}
                                check_resp = requests.post(check_url, json=check_data, timeout=30)
                                check_result = check_resp.json()
                                
                                if check_result.get('status') == 'ready':
                                    token = check_result.get('solution', {}).get('gRecaptchaResponse')
                                    log.debug(f"[CAPTCHA] ✓ antiCaptcha solved!")
                                    return token
                                elif check_result.get('status') == 'processing':
                                    if attempt % 10 == 0:
                                        log.debug(f"[CAPTCHA] Waiting for antiCaptcha... ({attempt * 3}s)")
                                else:
                                    log.debug(f"[CAPTCHA] antiCaptcha error: {check_result.get('errorDescription')}")
                                    return None
                        
                    except Exception as e:
                        log.debug(f"[CAPTCHA] API error: {e}")
                    
                    return None
                
                def check_and_wait_for_captcha(pg, max_wait=120):
                    """Check for CAPTCHA and attempt automatic or manual solving."""
                    try:
                        html_preview = pg.content()[:10000].lower()
                    except Exception as e:
                        log.debug(f"[SerpScan] Warning: Could not check for CAPTCHA (page navigating?): {e}")
                        time.sleep(2)
                        return True

                    captcha_indicators = ['unusual traffic', 'captcha', 'recaptcha', 'verify you', 'not a robot']
                    
                    if any(indicator in html_preview for indicator in captcha_indicators):
                        log.debug("[SerpScan] ⚠️ CAPTCHA detected!")
                        
                        # Try to extract reCAPTCHA site key for automatic solving
                        site_key = None
                        try:
                            import re
                            site_key_match = re.search(r'data-sitekey=["\']([^"\']+)["\']', pg.content())
                            if site_key_match:
                                site_key = site_key_match.group(1)
                                log.debug(f"[CAPTCHA] Found site key: {site_key[:20]}...")
                        except:
                            pass
                        
                        # Attempt automatic solving if configured
                        from ..config import config
                        if site_key and config.CAPTCHA_PROVIDER != 'none' and config.CAPTCHA_API_KEY:
                            token = solve_captcha_via_api(site_key, pg.url)
                            if token:
                                # Inject token and submit
                                try:
                                    pg.evaluate(f"""
                                        document.getElementById('g-recaptcha-response').innerHTML = '{token}';
                                        document.getElementById('g-recaptcha-response').style.display = 'block';
                                        // Try to submit the form
                                        const form = document.querySelector('form');
                                        if (form) form.submit();
                                    """)
                                    time.sleep(3)
                                    
                                    # Check if solved
                                    new_html = pg.content()[:5000].lower()
                                    if not any(ind in new_html for ind in captcha_indicators):
                                        log.debug("[CAPTCHA] ✓ Automatic CAPTCHA solving succeeded!")
                                        return True
                                except Exception as e:
                                    log.debug(f"[CAPTCHA] Token injection error: {e}")
                        
                        # Fall back to manual solving
                        log.debug(f"[SerpScan] Waiting up to {max_wait} seconds for manual CAPTCHA solve...")
                        
                        for wait_count in range(max_wait // 2):
                            time.sleep(2)  # Check more frequently
                            try:
                                current_html = pg.content()[:5000].lower()
                            except Exception:
                                continue

                            if 'search' in current_html and not any(ind in current_html for ind in captcha_indicators):
                                log.debug("[SerpScan] ✓ CAPTCHA solved! Continuing...")
                                time.sleep(1)
                                return True
                            if wait_count % 10 == 0:
                                log.debug(f"[SerpScan] Still waiting for CAPTCHA... ({wait_count * 2}s)")
                        
                        log.debug("[SerpScan] ⚠️ Timeout waiting for CAPTCHA. Results may be incomplete.")
                        return False
                    return True
                
                # Check for CAPTCHA on initial page load
                check_and_wait_for_captcha(page)
                
                # Collect HTML from multiple pages if depth > 10
                all_html_parts = []
                
                if depth > 10:
                    log.debug(f"[SerpScan] Collecting results across multiple pages for depth={depth}...")
                    
                    # Calculate how many pages we might need (10 results per page)
                    pages_needed = max(1, depth // 10)
                    
                    for page_num in range(pages_needed):
                        # Capture current page's HTML
                        current_html = page.content()
                        all_html_parts.append(current_html)
                        log.debug(f"[SerpScan] Captured page {page_num + 1} ({len(current_html)} bytes)")
                        
                        if page_num >= pages_needed - 1:
                            break
                        
                        # [OPTIMIZED] Faster scroll to bottom
                        page.keyboard.press('End')
                        time.sleep(random.uniform(0.2, 0.4))
                        
                        # Try to click "More results" button
                        more_results_selectors = [
                            '#pnnext',  # Traditional next page (most common)
                            'a:has-text("More results")',
                            'a[aria-label="More results"]',
                            'a[aria-label="Next page"]',
                            'span:has-text("Next") >> xpath=ancestor::a',
                        ]
                        
                        clicked = False
                        for selector in more_results_selectors:
                            try:
                                more_btn = page.locator(selector).first
                                if more_btn.is_visible(timeout=1500):
                                    more_btn.click()
                                    log.debug(f"[SerpScan] Moving to page {page_num + 2}...")
                                    clicked = True
                                    # [OPTIMIZED] Wait for next page to load
                                    try:
                                        page.wait_for_selector('#rso, #search', timeout=5000)
                                    except:
                                        time.sleep(1)
                                    time.sleep(random.uniform(0.3, 0.6))
                                    break
                            except:
                                continue
                        
                        if not clicked:
                            log.debug("[SerpScan] No more pages available")
                            break
                    
                    # Combine all HTML parts - wrap in container for parser
                    # Combine all HTML parts - Merge into the first page (Base)
                    # [FIX] Use Page 1 as base to preserve Local Pack and other features
                    log.debug(f"[SerpScan] Merging {len(all_html_parts)} pages into single SERP...")
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
                                log.debug(f"[SerpScan] Error merging page {i+2}: {e}")
                                
                        content = str(base_soup)
                        log.debug(f"[SerpScan] Merged content size: {len(content)} bytes")
                    else:
                         # Fallback if Page 1 structure is weird - just concat strings (raw)
                         log.debug("[SerpScan] Could not find #rso in Page 1, falling back to concatenation")
                         content = "".join(all_html_parts)
                    
                    final_url = page.url
                else:
                    # Standard behavior for depth <= 10
                    for _ in range(3):
                        page.mouse.wheel(0, random.randint(300, 500))
                        time.sleep(random.uniform(0.5, 1.0))
                    
                    content = page.content()
                    final_url = page.url
                    log.debug(f"[SerpScan] Captured {len(content)} bytes of HTML")

                # --- AI MODE TAB DETECTION (Labs Feature) ---
                # Check if "AI Mode" tab exists and capture it
                ai_mode_html = ""
                try:
                    # Look for the tab visually similar to "All", "Maps"
                    ai_mode_selectors = [
                        'a:has-text("AI Mode")', 
                        'div[role="tab"]:has-text("AI Mode")', 
                        'span:has-text("AI Mode")',
                        '[data-hveid] a:text("AI Mode")',  # Google-specific
                        '.q.qs:has-text("AI Mode")',  # Tab class
                    ]
                    
                    ai_mode_tab = None
                    for sel in ai_mode_selectors:
                        try:
                            tab = page.locator(sel).first
                            if tab.is_visible(timeout=1000):
                                ai_mode_tab = tab
                                break
                        except: continue
                    
                    if ai_mode_tab:
                        log.debug("[SerpScan] Found 'AI Mode' tab! Navigating to AI view...")
                        try:
                            ai_mode_tab.click(timeout=5000)
                        except Exception as e:
                             log.debug(f"[SerpScan] Failed to click AI Mode tab: {e}")
                             raise e # Re-raise to be caught by outer try/except and skip AI mode logic 

                        
                        # Wait for the AI interface to load
                        try:
                            page.wait_for_load_state('networkidle', timeout=10000)
                            time.sleep(3.0)  # Extra wait for AI content generation
                        except: pass
                        
                        # ===== CRITICAL: Find and click "Show all" in right sidebar =====
                        # The sidebar shows "X sites" header with sources, and has a "Show all" button
                        show_all_clicked = False
                        
                        # Multiple selector strategies for the Show all button
                        show_all_selectors = [
                            # Text-based selectors
                            'span:has-text("Show all")',
                            'button:has-text("Show all")',
                            'div:has-text("Show all"):not(:has(div))', # Leaf div with text
                            'a:has-text("Show all")',
                            
                            # Container-based (sidebar panel)
                            '[class*="sidebar"] [role="button"]:has-text("Show all")',
                            '[class*="panel"] span:has-text("Show all")',
                            
                            # Google-specific patterns
                            'g-inner-card span:has-text("Show all")',
                            '[data-attrid] span:has-text("Show all")',
                            '[jscontroller] [jsaction*="click"]:has-text("Show all")',
                        ]
                        
                        for sel in show_all_selectors:
                            try:
                                show_all_btn = page.locator(sel).first
                                if show_all_btn.is_visible(timeout=2000):
                                    log.debug(f"[SerpScan] Found 'Show all' button via: {sel}")
                                    
                                    # Scroll it into view
                                    show_all_btn.scroll_into_view_if_needed()
                                    time.sleep(0.5)
                                    
                                    # Click it
                                    show_all_btn.click()
                                    show_all_clicked = True
                                    log.debug("[SerpScan] Clicked 'Show all' - waiting for full list to load...")
                                    
                                    # Wait for expanded list to fully render
                                    time.sleep(3.0)
                                    
                                    # Try to wait for the list container to populate
                                    try:
                                        page.wait_for_selector('[class*="list"] a[href]', timeout=3000)
                                    except: pass
                                    
                                    break
                            except Exception as e:
                                continue
                        
                        if not show_all_clicked:
                            log.debug("[SerpScan] Could not find 'Show all' button - capturing visible citations only")
                            # Try scrolling the sidebar to load more
                            try:
                                sidebar = page.locator('[class*="sidebar"], [class*="panel"]').first
                                if sidebar.is_visible(timeout=2000):
                                    # Scroll within sidebar to trigger lazy loading
                                    for _ in range(3):
                                        sidebar.evaluate('el => el.scrollTop += 500')
                                        time.sleep(0.5)
                            except: pass
                        
                        # Capture the full AI Mode content including all expanded citations
                        ai_mode_html = page.content()
                        log.debug(f"[SerpScan] Captured AI Mode content ({len(ai_mode_html)} bytes)")
                        
                except Exception as e:
                    log.debug(f"[SerpScan] AI Mode check/capture failed: {e}")
                    
                if ai_mode_html:
                    content += f"\n<!-- AI_MODE_HTML_START -->\n{ai_mode_html}\n<!-- AI_MODE_HTML_END -->"
                
                # --- AI OVERVIEW EXPANSION ---
                # Try to expand and capture full AI Overview content
                ai_overview_html = ""
                try:
                    # [FIX] "Wake up" the page to trigger dynamic content loading
                    # Small scroll often triggers lazy-loaded AI components
                    page.mouse.wheel(0, 300)
                    time.sleep(1.0)
                    page.mouse.wheel(0, -300)
                    time.sleep(0.5)
                    
                    from bs4 import BeautifulSoup
                    content_check = page.content()
                    soup_ai = BeautifulSoup(content_check, 'html.parser')
                    
                    # Detect AI Overview presence
                    ai_container = soup_ai.select_one('div[data-attrid="ai_overview"]')
                    ai_header = soup_ai.find(lambda tag: tag.name in ['h1', 'h2', 'span', 'div'] and tag.get_text(strip=True) == "AI Overview")
                    
                    if ai_container or ai_header:
                        log.debug("[SerpScan] AI Overview detected! Attempting expansion...")
                        
                        # Try to click "Show more" or expand button
                        expand_clicked = False
                        expand_selectors = [
                            'div[data-attrid="ai_overview"] button',           # Generic button in AI overview
                            'div[data-attrid="ai_overview"] [role="button"]', # Role button
                            'div.GenerativeAI button',                         # Button in GenerativeAI container
                            'button:has-text("Show more")',                    # Show more button
                            '[aria-label*="Show more"]',                       # Aria label
                            'div[jsname] button[data-ved]',                    # Data-ved buttons
                        ]
                        
                        for selector in expand_selectors:
                            try:
                                expand_btn = page.locator(selector).first
                                if expand_btn.is_visible(timeout=1000):
                                    log.debug(f"[SerpScan] Found AI expand button via: {selector}")
                                    # Hover first to simulate human interest
                                    expand_btn.hover()
                                    time.sleep(random.uniform(0.5, 1.0)) 
                                    expand_btn.click(timeout=2000)
                                    expand_clicked = True
                                    
                                    # [FIX] Wait longer for generation/expansion (AI can be slow)
                                    log.debug("[SerpScan] Waiting for AI content to generate...")
                                    time.sleep(4.0) 
                                    break
                            except:
                                continue
                        
                        if expand_clicked:
                            log.debug("[SerpScan] AI Overview expanded successfully")
                        else:
                            log.debug("[SerpScan] AI Overview may already be fully expanded")
                        
                        # Re-capture content after expansion
                        # Wait for network idle to ensure streaming content finishes
                        try:
                            page.wait_for_load_state('networkidle', timeout=3000)
                        except: pass
                        
                        ai_content_now = page.content()
                        soup_ai_expanded = BeautifulSoup(ai_content_now, 'html.parser')
                        
                        # Extract AI Overview section HTML
                        ai_section = soup_ai_expanded.select_one('div[data-attrid="ai_overview"]')
                        if not ai_section:
                            # Try alternative containers
                            ai_header_exp = soup_ai_expanded.find(lambda tag: tag.name in ['h1', 'h2', 'span', 'div'] and "AI Overview" == tag.get_text(strip=True))
                            if ai_header_exp:
                                ai_section = ai_header_exp.find_parent('div', class_=lambda c: c and ('M8OgIe' in c or 'Generative' in c))
                                if not ai_section:
                                    ai_section = ai_header_exp.parent.parent.parent
                        
                        if ai_section:
                            ai_overview_html = str(ai_section)
                            log.debug(f"[SerpScan] Captured AI Overview ({len(ai_overview_html)} bytes)")
                            
                            # Update main content with expanded version
                            content = ai_content_now
                    else:
                        log.debug("[SerpScan] No AI Overview detected on this SERP")
                        
                except Exception as ai_err:
                    log.debug(f"[SerpScan] AI Overview expansion error: {ai_err}")

                # Append AI Overview HTML with markers if captured
                if ai_overview_html:
                    content += f"\n<!-- AI_OVERVIEW_HTML_START -->\n{ai_overview_html}\n<!-- AI_OVERVIEW_HTML_END -->"
                
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
                            log.debug(f"[SerpScan] Fallback: Detected Hotel intent via Link '{fallback_hotel_link.get_text()}'")
                            detected_intent = 'HOTELS'
                            detected_heading_text = "Fallback Link"

                    log.debug(f"[SerpScan] Detected Heading: '{detected_heading_text}' -> Intent: {detected_intent}")
                    
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
                            
                            # [FIX] Do NOT append UULE if we are using coordinates/blue dot
                            # Appending broad UULE overrides the precise geolocation context
                            # Only append GL if strictly needed (usually link has it)
                            if gl_code and 'gl=' not in local_url:
                                local_url += f"&gl={gl_code}"

                                
                            # VALIDATION: Ensure it looks like a map/search URL (not video)
                            if '/maps' in local_url or 'tbs=lrf' in local_url or '/search?' in local_url:
                                if '/vid' not in local_url:
                                    log.debug(f"[SerpScan] Found VALID 'More places' URL: {local_url}")
                                    log.debug("[SerpScan] Navigating to Local Finder...")
                                    try:
                                        page.goto(local_url, wait_until='domcontentloaded', timeout=20000)
                                    except Exception as nav_err:
                                        log.debug(f"[SerpScan] Local Finder navigation warning (proceeding anyway): {nav_err}")
                                    
                                    # [FIX] Wait for lazy-loaded content (Phone, Timings)
                                    try:
                                        # 1. Wait for connection to stabilize
                                        page.wait_for_load_state('networkidle', timeout=5000)
                                    except: pass
                                    
                                    time.sleep(2.0)
                                    
                                    # 2. Trigger lazy load by scrolling
                                    try:
                                        if not page.is_closed():
                                            log.debug("[SerpScan] Triggering lazy load via scroll...")
                                            # Use safe scroll wrapper
                                            try:
                                                page.mouse.wheel(0, 500)
                                                time.sleep(0.5)
                                                page.mouse.wheel(0, 1000)
                                                time.sleep(1.0)
                                                page.mouse.wheel(0, -1500) # Scroll back up for top results
                                                time.sleep(1.0)
                                            except Exception as scroll_err:
                                                log.debug(f"[SerpScan] Scroll error (ignoring): {scroll_err}")
                                    except Exception as e:
                                        log.debug(f"[SerpScan] Scroll/Wait warning: {e}")

                                else:
                                    log.debug(f"[SerpScan] SKIPPING: URL looks like video/image: {local_url}")
                            else:
                                log.debug(f"[SerpScan] SKIPPING: URL does not look like Local Finder: {local_url}")

                    elif detected_intent == 'HOTELS':
                        log.debug(f"[SerpScan] Hotel intent detected via '{detected_heading_text}'. Looking for 'See more' link...")
                        # Look for "View all", "More hotels", or similar
                        hotel_link = soup_check.find(lambda tag: tag.name == 'a' and ('View all' in tag.get_text() or 'More hotels' in tag.get_text() or 'See more' in tag.get_text()) and 'google' not in tag.get('href', ''))
                        
                        if hotel_link and hotel_link.get('href'):
                            hotel_url = hotel_link.get('href')
                            if hotel_url.startswith('/'):
                                hotel_url = f"https://www.{google_domain}{hotel_url}"
                            
                            # [FIX] Do NOT append UULE to preserve precise geolocation
                            if gl_code and 'gl=' not in hotel_url:
                                hotel_url += f"&gl={gl_code}"

                            
                            log.debug(f"[SerpScan] Found Hotel View URL: {hotel_url}")
                            log.debug("[SerpScan] Navigating to Hotel Finder...")
                            page.goto(hotel_url, wait_until='domcontentloaded', timeout=20000)
                            
                            # Hotel finder specific wait - it often takes longer to load the grid
                            try:
                                # Wait for hotel cards (often identifiable by specific classes or just a list)
                                # Hotel cards usually have prices, so waiting for currency symbol might be good, or just wait for the main feed
                                page.wait_for_selector('div[role="feed"], div[jsname*=""]', timeout=10000) 
                                time.sleep(random.uniform(2.0, 4.0)) # Extra wait for Hotel UI
                                
                                # Scroll logic specifically for Hotels
                                log.debug("[SerpScan] Scrolling Hotel List...")
                                for _ in range(4):
                                    page.mouse.wheel(0, random.randint(500, 800))
                                    time.sleep(random.uniform(0.8, 1.5))
                                    
                            except Exception as e:
                                log.debug(f"[SerpScan] Hotel wait/scroll warning: {e}")
                                time.sleep(2)
                        else:
                            log.debug("[SerpScan] Could not find a 'View all' link for Hotels.")

                    elif detected_intent == 'SHOPPING':
                        log.debug("[SerpScan] Shopping intent detected. Looking for 'Shopping' tab...")
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

                             
                             log.debug(f"[SerpScan] Found Shopping Tab URL: {shopping_url}")
                             log.debug("[SerpScan] Navigating to Shopping Tab...")
                             page.goto(shopping_url, wait_until='domcontentloaded', timeout=20000)
                             
                             # Wait for product grid
                             try:
                                 # Common selector for shopping grid
                                 # Updated with more robust selectors including individual items
                                 page.wait_for_selector('div.sh-pr__product-results-grid, div.sh-dgr__grid-result, div[data-docid], div.i0X6df, div.KZmu8e', timeout=10000)
                                 time.sleep(2)
                                 
                                 log.debug("[SerpScan] Scrolling Shopping List...")
                                 for _ in range(3):
                                     page.mouse.wheel(0, 800)
                                     time.sleep(1)
                                     
                             except Exception as e:
                                 log.debug(f"[SerpScan] Shopping grid wait warning: {e}")
                                 time.sleep(1)
                        else:
                             log.debug("[SerpScan] Could not find 'Shopping' tab link.")

                    elif detected_intent == 'ORGANIC':
                        # Fallback / Default
                        pass

                except Exception as e:
                    log.debug(f"[SerpScan] Error during navigation check: {e}")
                    pass
                
                # --- Post-Navigation Scraping ---
                
                # Check if we are now on a different URL than the initial SERP
                detected_move = page.url != final_url
                
                if detected_move:
                    log.debug(f"[SerpScan] Navigation occurred. Current URL: {page.url}")
                    
                    if detected_intent == 'HOTELS':
                         try:
                            # Hotel specific scraping
                            # Hotels often load lazily. Ensure we capture enough.
                            hotel_html = page.content()
                            log.debug(f"[SerpScan] Captured Hotel List ({len(hotel_html)} bytes)")
                            
                            content += f"\n<!-- HOTELS_HTML_START -->\n{hotel_html}\n<!-- HOTELS_HTML_END -->"
                         except Exception as e:
                             log.debug(f"[SerpScan] Error scraping Hotel content: {e}")

                    elif detected_intent == 'SHOPPING':
                        try:
                            shopping_html = page.content()
                            log.debug(f"[SerpScan] Captured Shopping List ({len(shopping_html)} bytes)")
                            content += f"\n<!-- SHOPPING_HTML_START -->\n{shopping_html}\n<!-- SHOPPING_HTML_END -->"
                        except Exception as e:
                            log.debug(f"[SerpScan] Error scraping Shopping content: {e}")

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
                            log.debug(f"[SerpScan] Captured Local Finder ({len(local_finder_html)} bytes)")
                            
                            # Append to content with marker
                            content += f"\n<!-- LOCAL_FINDER_HTML_START -->\n{local_finder_html}\n<!-- LOCAL_FINDER_HTML_END -->"
                            
                        except Exception as e:
                            log.debug(f"[SerpScan] Extended Local Pack Error: {e}")

                
                return content, final_url
                
            except Exception as e:
                log.debug(f"[SerpScan] Error: {e}")
                import traceback
                traceback.print_exc()
                return None, None
                
            finally:
                try:
                    context.close()
                except:
                    pass
                try:
                    if browser_ctx:
                        browser_ctx.__exit__(None, None, None)
                    elif browser:
                        browser.close()
                except:
                    pass
        
        except Exception as global_err:
            # [FIX] Catch Playwright/greenlet crashes that would otherwise kill the server
            log.error(f"[SerpScan] Fatal Playwright error: {global_err}")
            import traceback
            traceback.print_exc()
            return None, None

