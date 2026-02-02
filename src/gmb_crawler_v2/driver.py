"""
GMB Crawler V2 - Playwright Driver

Standalone browser driver for Google Maps scraping.
Completely independent from gmb_core geo_driver.
"""

import time
import random
import threading
import atexit
import re
from typing import Optional, Tuple, Dict, Any
from contextlib import contextmanager

# Playwright imports
try:
    from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from playwright_stealth import stealth_sync
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False

import logging

logger = logging.getLogger(__name__)


# ==================== Browser Pool Manager ====================

class BrowserManager:
    """
    Thread-safe browser instance manager.
    """
    _instance = None
    _lock = threading.Lock()
    
    # Browser limits
    MAX_CONCURRENT_BROWSERS = 3
    _semaphore = threading.Semaphore(MAX_CONCURRENT_BROWSERS)
    
    @classmethod
    def get_instance(cls):
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self._local = threading.local()
    
    @contextmanager
    def managed_browser(self, headless: bool = True, proxy_url: str = None):
        """
        Context manager for thread-safe browser usage.
        
        Args:
            headless: Run in headless mode
            proxy_url: Optional proxy URL
            
        Yields:
            Browser instance
        """
        self._semaphore.acquire()
        pw = None
        browser = None
        
        try:
            pw = sync_playwright().start()
            
            launch_args = [
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
            
            launch_kwargs = {
                'headless': headless,
                'args': launch_args,
            }
            
            if proxy_url:
                launch_kwargs['proxy'] = {'server': proxy_url}
            
            browser = pw.chromium.launch(**launch_kwargs)
            yield browser
            
        finally:
            if browser:
                try:
                    browser.close()
                except Exception:
                    pass
            if pw:
                try:
                    pw.stop()
                except Exception:
                    pass
            self._semaphore.release()


# Global manager instance
_browser_manager = None


def get_browser_manager() -> BrowserManager:
    """Get global browser manager."""
    global _browser_manager
    if _browser_manager is None:
        _browser_manager = BrowserManager.get_instance()
    return _browser_manager


# ==================== Main Driver Class ====================

class GMBDriverV2:
    """
    Playwright-based driver for Google Maps data extraction.
    
    Features:
    - Stealth mode with anti-detection
    - Geolocation spoofing
    - Smart scrolling for lazy content
    - CAPTCHA detection
    """
    
    # User agents
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    ]
    
    # Viewport sizes
    VIEWPORTS = [
        {'width': 1920, 'height': 1080},
        {'width': 1536, 'height': 864},
        {'width': 1440, 'height': 900},
    ]
    
    # Timezone mapping by longitude
    TIMEZONE_MAP = [
        (-180, -157.5, 'Pacific/Midway'),
        (-157.5, -127.5, 'America/Anchorage'),
        (-127.5, -112.5, 'America/Los_Angeles'),
        (-112.5, -97.5, 'America/Denver'),
        (-97.5, -82.5, 'America/Chicago'),
        (-82.5, -67.5, 'America/New_York'),
        (-67.5, -52.5, 'America/Halifax'),
        (-52.5, -37.5, 'America/Sao_Paulo'),
        (-37.5, -22.5, 'Atlantic/Azores'),
        (-22.5, -7.5, 'Atlantic/Reykjavik'),
        (-7.5, 7.5, 'Europe/London'),
        (7.5, 22.5, 'Europe/Paris'),
        (22.5, 37.5, 'Europe/Athens'),
        (37.5, 52.5, 'Europe/Moscow'),
        (52.5, 67.5, 'Asia/Karachi'),
        (67.5, 82.5, 'Asia/Dhaka'),
        (82.5, 97.5, 'Asia/Bangkok'),
        (97.5, 112.5, 'Asia/Shanghai'),
        (112.5, 127.5, 'Asia/Tokyo'),
        (127.5, 142.5, 'Australia/Sydney'),
        (142.5, 157.5, 'Pacific/Noumea'),
        (157.5, 180, 'Pacific/Auckland'),
    ]
    
    def __init__(self, headless: bool = True, proxy_url: str = None):
        """
        Initialize driver.
        
        Args:
            headless: Run browser in headless mode
            proxy_url: Optional proxy URL
        """
        self.headless = headless
        self.proxy_url = proxy_url
        self.manager = get_browser_manager()
    
    def _get_timezone(self, lng: float) -> str:
        """Get timezone for longitude."""
        for min_lng, max_lng, tz in self.TIMEZONE_MAP:
            if min_lng <= lng < max_lng:
                return tz
        return 'UTC'
    
    def _get_random_user_agent(self) -> str:
        """Get random user agent."""
        return random.choice(self.USER_AGENTS)
    
    def _get_random_viewport(self) -> Dict[str, int]:
        """Get random viewport size."""
        return random.choice(self.VIEWPORTS)
    
    def _create_context(
        self, 
        browser: 'Browser', 
        lat: float = None, 
        lng: float = None
    ) -> 'BrowserContext':
        """
        Create browser context with stealth and geolocation.
        
        Args:
            browser: Browser instance
            lat: Latitude for geolocation
            lng: Longitude for geolocation
            
        Returns:
            Configured browser context
        """
        context_options = {
            'user_agent': self._get_random_user_agent(),
            'viewport': self._get_random_viewport(),
            'locale': 'en-US',
            'java_script_enabled': True,
        }
        
        # Add geolocation if provided
        if lat is not None and lng is not None:
            context_options['geolocation'] = {'latitude': lat, 'longitude': lng}
            context_options['permissions'] = ['geolocation']
            context_options['timezone_id'] = self._get_timezone(lng)
        
        context = browser.new_context(**context_options)
        
        # Apply stealth if available
        # Note: stealth_sync applies to pages, not contexts
        
        return context
    
    def _apply_stealth(self, page: 'Page') -> None:
        """Apply stealth modifications to page."""
        if STEALTH_AVAILABLE:
            try:
                stealth_sync(page)
            except Exception as e:
                logger.warning(f"Failed to apply stealth: {e}")
        
        # Additional anti-detection scripts
        try:
            page.add_init_script("""
                // Override webdriver detection
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Override plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5]
                });
                
                // Override languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            """)
        except Exception as e:
            logger.warning(f"Failed to add init script: {e}")
    
    def _random_delay(self, min_ms: int = 500, max_ms: int = 1500) -> None:
        """Add random delay to appear human."""
        delay = random.randint(min_ms, max_ms) / 1000
        time.sleep(delay)
    
    def _scroll_page(self, page: 'Page', scroll_count: int = 3) -> None:
        """
        Scroll page to load lazy content.
        Targets both window and specific GMB scrollable containers.
        """
        # 1. Try to find scrollable container (GMB specific)
        # div.m6QErb is the standard container for GMB sidebar/content
        scrollable_selector = 'div.m6QErb[aria-label], div[role="main"]'
        
        try:
            # Check if likely a scrollable element exists
            handle = page.query_selector(scrollable_selector)
            
            if handle:
                logger.info("Found scrollable GMB container, scrolling it...")
                for i in range(scroll_count):
                    # Scroll the element
                    page.evaluate(f'''
                        const el = document.querySelector('{scrollable_selector}');
                        if (el) {{
                            el.scrollTop = el.scrollHeight;
                        }}
                    ''')
                    self._random_delay(500, 1000)
            else:
                # Fallback to window scroll
                logger.info("No specific scroll container found, scrolling window...")
                for i in range(scroll_count):
                    page.evaluate('window.scrollBy(0, window.innerHeight)')
                    self._random_delay(300, 800)
                    
        except Exception as e:
            logger.warning(f"Error during scrolling: {e}")
            # Fallback
            for i in range(scroll_count):
                page.evaluate('window.scrollBy(0, window.innerHeight)')
                self._random_delay(300, 800)
        
        # Scroll back to top to ensure top elements are visible for parsing
        try:
            page.evaluate('''
                const el = document.querySelector('div.m6QErb[aria-label], div[role="main"]');
                if (el) el.scrollTop = 0;
                window.scrollTo(0, 0);
            ''')
        except Exception:
            pass
            
        self._random_delay(200, 500)
    
    def _check_captcha(self, page: 'Page') -> bool:
        """Check if page has CAPTCHA."""
        captcha_indicators = [
            'iframe[src*="recaptcha"]',
            'div.g-recaptcha',
            '#captcha',
            'form[action*="sorry"]',
        ]
        
        for selector in captcha_indicators:
            if page.query_selector(selector):
                logger.warning("CAPTCHA detected")
                return True
        
        # Check page content
        content = page.content()
        if 'unusual traffic' in content.lower() or 'not a robot' in content.lower():
            return True
        
        return False
    
    def _wait_for_content(self, page: 'Page', timeout: int = 30000) -> bool:
        """Wait for main content to load."""
        try:
            # First, wait for network to be mostly idle
            try:
                page.wait_for_load_state('networkidle', timeout=15000)
            except Exception:
                pass  # Continue even if timeout
            
            # Wait for Google Maps specific elements with multiple fallbacks
            # These are the most reliable selectors for Google Maps business pages
            selectors = [
                'div[role="main"]',  # Main content container (most reliable)
                'button[data-item-id="address"]',  # Address button
                'button[data-item-id*="phone"]',  # Phone button
                'h1',  # Any h1 (business name)
                'div.m6QErb',  # Scrollable content area
                'div[jslog]',  # Google Maps specific element
            ]
            
            for selector in selectors:
                try:
                    page.wait_for_selector(selector, timeout=5000, state='attached')
                    logger.debug(f"Found selector: {selector}")
                    # Found at least one element, give extra time for more content
                    time.sleep(2)
                    return True
                except Exception:
                    continue
            
            # Last resort: just wait a bit more for any dynamic content
            logger.warning("No specific selectors found, waiting for dynamic content...")
            time.sleep(5)
            return True
            
        except Exception as e:
            logger.warning(f"Timeout waiting for content: {e}")
            time.sleep(3)
            return False
    
    def fetch_page(
        self, 
        url: str, 
        lat: float = None, 
        lng: float = None,
        scroll: bool = True,
        wait_for_content: bool = True
    ) -> Tuple[str, str, bool]:
        """
        Fetch a Google Maps page.
        
        Args:
            url: URL to fetch
            lat: Optional latitude for geo context
            lng: Optional longitude for geo context
            scroll: Whether to scroll for lazy content
            wait_for_content: Whether to wait for content to load
            
        Returns:
            Tuple of (HTML content, final URL, success)
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright not available")
            return "", url, False
        
        with self.manager.managed_browser(self.headless, self.proxy_url) as browser:
            context = self._create_context(browser, lat, lng)
            page = context.new_page()
            
            try:
                self._apply_stealth(page)
                
                # Navigate to page - use 'domcontentloaded' which is faster than 'load' but safe enough for SPA start
                logger.info(f"Navigating to: {url}")
                try:
                    page.goto(url, wait_until='domcontentloaded', timeout=60000)
                except Exception:
                    page.goto(url, wait_until='commit', timeout=60000)
                
                # Give page time to start rendering JavaScript
                time.sleep(3)
                
                # Try to wait for network idle to ensure initial XHRs
                try:
                    page.wait_for_load_state('networkidle', timeout=10000)
                except Exception:
                    pass
                
                # Check for CAPTCHA
                if self._check_captcha(page):
                    logger.error("CAPTCHA encountered")
                    return page.content(), page.url, False
                
                # Wait for content
                if wait_for_content:
                    self._wait_for_content(page)
                
                # Scroll to load lazy content
                if scroll:
                    self._scroll_page(page)
                    # Extra time after scrolling for content to load
                    time.sleep(2)
                
                # Get final content
                html_content = page.content()
                final_url = page.url
                
                return html_content, final_url, True
                
            except Exception as e:
                logger.error(f"Error fetching page: {e}")
                try:
                    return page.content(), page.url, False
                except Exception:
                    return "", url, False
            
            finally:
                try:
                    context.close()
                except Exception:
                    pass
    
    def resolve_short_url(self, url: str) -> Optional[str]:
        """
        Resolve short URL (maps.app.goo.gl) to full URL.
        
        Args:
            url: Short Google Maps URL
            
        Returns:
            Full resolved URL
        """
        if 'maps.app.goo.gl' not in url and 'goo.gl/maps' not in url:
            return url  # Not a short URL
        
        with self.manager.managed_browser(True, self.proxy_url) as browser:
            context = browser.new_context()
            page = context.new_page()
            
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=30000)
                time.sleep(2)
                
                final_url = page.url
                logger.info(f"Resolved short URL to: {final_url}")
                return final_url
                
            except Exception as e:
                logger.error(f"Error resolving short URL: {e}")
                return url
            
            finally:
                context.close()
    
    def extract_place_details(
        self, 
        url: str,
        lat: float = None,
        lng: float = None
    ) -> Tuple[str, str, bool]:
        """
        Extract details from a Google Maps place page.
        
        Args:
            url: Google Maps place URL
            lat: Optional latitude for geo context
            lng: Optional longitude for geo context
            
        Returns:
            Tuple of (HTML content, final URL, success)
        """
        # Resolve short URLs first
        if 'goo.gl' in url:
            url = self.resolve_short_url(url) or url
        
        return self.fetch_page(
            url, 
            lat=lat, 
            lng=lng, 
            scroll=True, 
            wait_for_content=True
        )
    
    def search_business(
        self, 
        query: str, 
        location: str = None,
        lat: float = None,
        lng: float = None
    ) -> Tuple[str, str, bool]:
        """
        Search for a business on Google Maps.
        
        Args:
            query: Business name/search query
            location: Optional location context (city, area)
            lat: Optional latitude
            lng: Optional longitude
            
        Returns:
            Tuple of (HTML content, final URL, success)
        """
        # Build search URL
        search_query = query
        if location:
            search_query = f"{query} {location}"
        
        from urllib.parse import quote
        url = f"https://www.google.com/maps/search/{quote(search_query)}"
        
        return self.fetch_page(
            url, 
            lat=lat, 
            lng=lng, 
            scroll=True,
            wait_for_content=True
        )
