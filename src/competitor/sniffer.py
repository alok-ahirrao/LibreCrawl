"""
Competitor Sniffer
Extracts competitor profiles from Google Maps Local Pack and deep scrapes details.
"""
import time
import random
import json
from typing import List, Dict, Optional
from datetime import datetime

from ..gmb_core.crawler.geo_driver import GeoCrawlerDriver
from ..gmb_core.crawler.parsers import GoogleMapsParser, LocalPackParser
from ..gmb_core.models import get_db
from ..gmb_core.config import config


class CompetitorSniffer:
    """
    Extracts competitor data from Google Maps.
    Uses existing GeoCrawlerDriver for browser automation and parsers for data extraction.
    """
    
    def __init__(self, delay_seconds: float = 10.0, headless: bool = True):
        """
        Initialize the sniffer.
        
        Args:
            delay_seconds: Delay between requests to avoid rate limiting
            headless: Run browser in headless mode
        """
        self.delay_seconds = delay_seconds
        self.headless = headless
        self.maps_parser = GoogleMapsParser()
        self.local_pack_parser = LocalPackParser()
    
    def _get_driver(self) -> GeoCrawlerDriver:
        """Create a new driver instance (stateless)."""
        return GeoCrawlerDriver(headless=self.headless)
    
    def close(self):
        """No-op: GeoCrawlerDriver manages browser lifecycle internally."""
        pass
    
    def extract_local_pack_competitors(
        self, 
        keyword: str, 
        lat: float, 
        lng: float,
        max_results: int = 10
    ) -> List[Dict]:
        """
        Extract competitors from Google Maps search results.
        
        Args:
            keyword: Search keyword (e.g., "dentist", "pizza")
            lat: Latitude for geo-targeted search
            lng: Longitude for geo-targeted search
            max_results: Maximum number of competitors to extract
            
        Returns:
            List of competitor dicts with place_id, name, rank, rating, review_count
        """
        driver = self._get_driver()
        competitors = []
        
        try:
            # Use existing scan method to get HTML
            html = driver.scan_grid_point(keyword, lat, lng)
            
            if not html:
                print(f"[Sniffer] Failed to get HTML for '{keyword}' at ({lat}, {lng})")
                return competitors
            
            # Parse the list results
            results = self.maps_parser.parse_list_results(html)
            
            for i, result in enumerate(results[:max_results]):
                competitors.append({
                    'place_id': result.get('place_id'),
                    'name': result.get('name'),
                    'rank': result.get('rank', i + 1),
                    'rating': result.get('rating'),
                    'review_count': result.get('reviews', 0),
                    'category': result.get('category'),
                    'address': result.get('address'),
                    'extracted_at': datetime.utcnow().isoformat()
                })
            
            print(f"[Sniffer] Extracted {len(competitors)} competitors for '{keyword}'")
            
        except Exception as e:
            print(f"[Sniffer] Error extracting competitors: {e}")
        
        return competitors
    
    def scrape_competitor_profile(self, place_id: str, lat: float = None, lng: float = None) -> Optional[Dict]:
        """
        Deep scrape a competitor's full profile from Google Maps.
        
        Args:
            place_id: Google Maps Place ID
            lat: Optional latitude for context
            lng: Optional longitude for context
            
        Returns:
            Dict with full profile details or None if failed
        """
        if not place_id:
            return None
        
        driver = self._get_driver()
        
        try:
            # Use the driver's scan_place_details method
            url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
            html, final_url = driver.scan_place_details(url, lat, lng)
            
            if not html:
                print(f"[Sniffer] Failed to get HTML for place_id: {place_id}")
                return None
            
            # Parse place details using existing parser
            details = self.maps_parser.parse_place_details(html)
            
            # Add place_id and timestamp
            details['place_id'] = place_id
            details['scraped_at'] = datetime.utcnow().isoformat()
            
            print(f"[Sniffer] Scraped profile for place_id: {place_id}")
            
            # Rate limiting delay
            self._polite_delay()
            
            return details
            
        except Exception as e:
            print(f"[Sniffer] Error scraping profile {place_id}: {e}")
            return None
    
    def _extract_extended_data(self, page) -> Dict:
        """
        Extract additional data not covered by standard parser.
        
        Args:
            page: Playwright page object
            
        Returns:
            Dict with extended data fields
        """
        extended = {
            'post_count': 0,
            'q_and_a_count': 0,
            'services': [],
            'hours': None
        }
        
        try:
            # Try to get posts count from "Updates" tab
            posts_tab = page.query_selector('button[aria-label*="Updates"]')
            if posts_tab:
                label = posts_tab.get_attribute('aria-label') or ''
                import re
                match = re.search(r'(\d+)', label)
                if match:
                    extended['post_count'] = int(match.group(1))
            
            # Try to get Q&A count
            qa_section = page.query_selector('button[aria-label*="question"]')
            if qa_section:
                label = qa_section.get_attribute('aria-label') or ''
                match = re.search(r'(\d+)', label)
                if match:
                    extended['q_and_a_count'] = int(match.group(1))
            
            # Try to get business hours
            hours_button = page.query_selector('button[data-item-id="oh"]')
            if hours_button:
                hours_button.click()
                time.sleep(1)
                # Extract hours from expanded section
                hours_table = page.query_selector('table[class*="hours"]')
                if hours_table:
                    extended['hours'] = hours_table.inner_text()
            
            # Try to get services
            services_section = page.query_selector_all('div[class*="services"] span')
            extended['services'] = [s.inner_text() for s in services_section[:10]]
            
        except Exception as e:
            print(f"[Sniffer] Warning: Could not extract extended data: {e}")
        
        return extended
    
    def _polite_delay(self):
        """Add randomized delay to avoid rate limiting."""
        jitter = random.uniform(-2, 2)
        delay = max(5, self.delay_seconds + jitter)
        time.sleep(delay)
    
    def batch_scrape_competitors(
        self, 
        competitors: List[Dict],
        callback=None
    ) -> List[Dict]:
        """
        Scrape full profiles for a batch of competitors.
        
        Args:
            competitors: List of competitor dicts with place_id
            callback: Optional callback(index, total, competitor) for progress
            
        Returns:
            List of fully scraped competitor profiles
        """
        profiles = []
        total = len(competitors)
        
        for i, competitor in enumerate(competitors):
            place_id = competitor.get('place_id')
            
            if callback:
                callback(i, total, competitor)
            
            if not place_id:
                print(f"[Sniffer] Skipping competitor without place_id: {competitor.get('name')}")
                continue
            
            # Check cache first
            cached = self._get_cached_profile(place_id)
            if cached:
                profiles.append(cached)
                print(f"[Sniffer] Using cached profile for {competitor.get('name')}")
                continue
            
            # Scrape fresh
            profile = self.scrape_competitor_profile(place_id)
            if profile:
                # Merge with basic competitor data
                profile['rank'] = competitor.get('rank')
                profiles.append(profile)
                
                # Save to cache
                self._save_competitor_profile(profile)
        
        return profiles
    
    def _get_cached_profile(self, place_id: str, max_age_days: int = 7) -> Optional[Dict]:
        """Get cached competitor profile if not stale."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM gmb_competitors 
                    WHERE place_id = ? 
                    AND last_scraped_at > datetime('now', '-' || ? || ' days')
                ''', (place_id, max_age_days))
                
                row = cursor.fetchone()
                if row:
                    return dict(row)
        except Exception as e:
            print(f"[Sniffer] Cache lookup error: {e}")
        
        return None
    
    def _save_competitor_profile(self, profile: Dict):
        """Save competitor profile to database cache."""
        try:
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO gmb_competitors (
                        place_id, name, primary_category, additional_categories,
                        rating, review_count, photo_count, attributes,
                        hours, services, post_count, q_and_a_count,
                        last_scraped_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(place_id) DO UPDATE SET
                        name = excluded.name,
                        primary_category = excluded.primary_category,
                        additional_categories = excluded.additional_categories,
                        rating = excluded.rating,
                        review_count = excluded.review_count,
                        photo_count = excluded.photo_count,
                        attributes = excluded.attributes,
                        hours = excluded.hours,
                        services = excluded.services,
                        post_count = excluded.post_count,
                        q_and_a_count = excluded.q_and_a_count,
                        last_scraped_at = CURRENT_TIMESTAMP
                ''', (
                    profile.get('place_id'),
                    profile.get('name'),
                    profile.get('primary_category'),
                    json.dumps(profile.get('additional_categories', [])),
                    profile.get('rating'),
                    profile.get('review_count'),
                    profile.get('photo_count'),
                    json.dumps(profile.get('attributes', [])),
                    profile.get('hours'),
                    json.dumps(profile.get('services', [])),
                    profile.get('post_count', 0),
                    profile.get('q_and_a_count', 0)
                ))
                print(f"[Sniffer] Saved profile to cache: {profile.get('name')}")
        except Exception as e:
            print(f"[Sniffer] Error saving profile: {e}")
