"""
GMB Crawler V2 - Main Crawler Engine

Orchestrates browser driver and parsers to extract
all data categories from Google Maps listings.
"""

import time
import logging
from typing import Optional, Dict, Any, List, Union
from concurrent.futures import ThreadPoolExecutor, as_completed

from .driver import GMBDriverV2
from datetime import datetime
from .parsers import (
    BasicDetailsParser,
    LocationDataParser,
    ContactInfoParser,
    MediaAssetsParser,
    ReviewsRatingsParser,
    BusinessAttributesParser,
    OperatingHoursParser,
    PopularTimesParser,
    CompetitiveDataParser,
    AdditionalDataParser,
)
from .types import GMBBusinessData

logger = logging.getLogger(__name__)


class GMBCrawlerV2:
    """
    Main crawler for extracting all data from Google Maps listings.
    
    Usage:
        crawler = GMBCrawlerV2()
        data = crawler.extract_full(url)
    """
    
    # Available data categories
    CATEGORIES = [
        'basic_details',
        'location_data',
        'contact_info',
        'media_assets',
        'reviews_ratings',
        'business_attributes',
        'operating_hours',
        'popular_times',
        'competitive_data',
        'additional_data',
    ]
    
    def __init__(
        self, 
        headless: bool = False,
        proxy_url: str = None
    ):
        """
        Initialize crawler.
        
        Args:
            headless: Run browser in headless mode
            proxy_url: Optional proxy URL
        """
        self.headless = headless
        self.proxy_url = proxy_url
        
        # Initialize driver
        self.driver = GMBDriverV2(headless=headless, proxy_url=proxy_url)
    
    def _get_parser(self, category: str, html: str, url: str):
        """Get parser instance for category."""
        parser_map = {
            'basic_details': BasicDetailsParser,
            'location_data': LocationDataParser,
            'contact_info': ContactInfoParser,
            'media_assets': MediaAssetsParser,
            'reviews_ratings': ReviewsRatingsParser,
            'business_attributes': BusinessAttributesParser,
            'operating_hours': OperatingHoursParser,
            'popular_times': PopularTimesParser,
            'competitive_data': CompetitiveDataParser,
            'additional_data': AdditionalDataParser,
        }
        
        parser_class = parser_map.get(category)
        if parser_class:
            # Use keyword arguments to ensure correct parameter mapping
            return parser_class(html_content=html, url=url)
        return None
    
    def extract_full(
        self, 
        url_or_query: str,
        location: str = None,
        lat: float = None,
        lng: float = None
    ) -> Dict[str, Any]:
        """
        Extract all data categories from a business listing.
        Uses the proven GMB Core driver for reliable page loading.
        
        Args:
            url_or_query: Google Maps URL or business search query
            location: Optional location context for search
            lat: Optional latitude for geo context
            lng: Optional longitude for geo context
            
        Returns:
            GMBBusinessData dict with all extracted data
        """
        start_time = time.time()
        
        # Use the proven GMB Core driver that works for scan analysis
        try:
            from gmb_core.crawler.geo_driver import GeoCrawlerDriver
        except ImportError:
            try:
                from src.gmb_core.crawler.geo_driver import GeoCrawlerDriver
            except ImportError:
                logger.error("GMB Core driver not available")
                return self._empty_result(url_or_query, 'DRIVER_NOT_AVAILABLE')
        
        # Create core driver instance
        core_driver = GeoCrawlerDriver(headless=self.headless, proxy_url=self.proxy_url)
        
        # Determine if URL or search query
        if 'google.com/maps' in url_or_query or 'goo.gl' in url_or_query:
            # Use scan_place_details for direct URLs
            html, final_url = core_driver.scan_place_details(
                url_or_query, lat=lat, lng=lng
            )
            success = html is not None and len(html) > 1000
        else:
            # Use search_business for queries  
            html = core_driver.search_business(
                url_or_query, location=location, lat=lat, lng=lng
            )
            final_url = url_or_query
            success = html is not None and len(html) > 1000
        
        if not success:
            logger.error(f"Failed to fetch page: {url_or_query}")
            return self._empty_result(url_or_query, 'FETCH_FAILED')
        
        # Extract all categories using GMB Core parser
        result = self._extract_from_html(html, final_url)
        
        # Add metadata
        result['extraction_metadata'] = {
            'requested_url': url_or_query,
            'final_url': final_url,
            'extraction_time': time.time() - start_time,
            'success': True,
            'error': None,
            'extracted_at': datetime.now().isoformat(),
        }
        
        logger.info(f"Extraction successful for {url_or_query}. Categories: {list(result.keys())}")
        
        return result
    
    def extract_partial(
        self, 
        url: str, 
        categories: List[str],
        lat: float = None,
        lng: float = None
    ) -> Dict[str, Any]:
        """
        Extract only specified categories.
        
        Args:
            url: Google Maps URL
            categories: List of category names to extract
            lat: Optional latitude
            lng: Optional longitude
            
        Returns:
            Dict with only requested categories
        """
        # Validate categories
        valid_categories = [c for c in categories if c in self.CATEGORIES]
        if not valid_categories:
            logger.warning("No valid categories specified, using basic_details")
            valid_categories = ['basic_details']
        
        # Fetch page
        html, final_url, success = self.driver.extract_place_details(
            url, lat=lat, lng=lng
        )
        
        if not success:
            return self._empty_result(url, 'FETCH_FAILED')
        
        # Extract specified categories
        result = {}
        for category in valid_categories:
            parser = self._get_parser(category, html, final_url)
            if parser:
                try:
                    result[category] = parser.parse()
                except Exception as e:
                    logger.error(f"Error parsing {category}: {e}")
                    result[category] = {}
        
        return result
    
    def extract_batch(
        self, 
        urls: List[str],
        max_workers: int = 3,
        delay_between: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        Extract data from multiple URLs.
        
        Args:
            urls: List of Google Maps URLs
            max_workers: Maximum concurrent extractions
            delay_between: Delay between extractions in seconds
            
        Returns:
            List of extraction results
        """
        results = []
        
        for i, url in enumerate(urls):
            try:
                result = self.extract_full(url)
                results.append(result)
                
                # Add delay between requests
                if i < len(urls) - 1:
                    time.sleep(delay_between)
                    
            except Exception as e:
                logger.error(f"Error extracting {url}: {e}")
                results.append(self._empty_result(url, str(e)))
        
        return results
    
    def _extract_from_html(self, html: str, url: str) -> Dict[str, Any]:
        """
        Extract all categories from HTML content.
        Uses the battle-tested GMB Core parser for robust extraction.
        """
        # Import the working GMB Core parser
        try:
            from gmb_core.crawler.parsers import GoogleMapsParser
            use_core_parser = True
        except ImportError:
            try:
                from src.gmb_core.crawler.parsers import GoogleMapsParser
                use_core_parser = True
            except ImportError:
                use_core_parser = False
                logger.warning("GMB Core parser not available, using V2 parsers")
        
        if use_core_parser:
            # Use the proven GMB Core parser
            core_parser = GoogleMapsParser()
            core_data = core_parser.parse_place_details(html)
            
            # Map GMB Core output to V2 format
            result = self._map_core_to_v2(core_data, url)

            # [POLYFILL] Extract Q&A entries using V2 parser (missing in GMB Core)
            try:
                qa_parser = AdditionalDataParser(html, url)
                entries = qa_parser.extract_qa_entries()
                if entries:
                    result['additional_data']['qa_entries'] = entries
                    if not result['additional_data']['qa_count']:
                         result['additional_data']['qa_count'] = len(entries)
            except Exception as e:
                logger.debug(f"Q&A polyfill failed: {e}")
        else:
            # Fallback to V2 parsers
            result = {}
            for category in self.CATEGORIES:
                parser = self._get_parser(category, html, url)
                if parser:
                    try:
                        result[category] = parser.parse()
                    except Exception as e:
                        logger.error(f"Error parsing {category}: {e}")
                        result[category] = {}
        
        return result
    
    def _map_core_to_v2(self, core_data: Dict, url: str) -> Dict[str, Any]:
        """Map GMB Core parser output to V2 format."""
        import re
        
        # Extract coordinates from URL if not in data
        lat = core_data.get('latitude')
        lng = core_data.get('longitude')
        
        if not lat or not lng:
            lat_match = re.search(r'!3d(-?\d+\.\d+)', url)
            lng_match = re.search(r'!4d(-?\d+\.\d+)', url)
            if lat_match and lng_match:
                lat = float(lat_match.group(1))
                lng = float(lng_match.group(1))
            else:
                coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
                if coord_match:
                    lat = float(coord_match.group(1))
                    lng = float(coord_match.group(2))
        
        # Extract CID from URL
        cid = None
        cid_match = re.search(r':0x([a-fA-F0-9]+)', url)
        if cid_match:
            try:
                cid = str(int(cid_match.group(1), 16))
            except:
                pass
        
        # Extract place_id
        place_id = core_data.get('place_id')
        if not place_id:
            place_match = re.search(r'!1s(0x[a-f0-9]+:0x[a-f0-9]+)', url)
            if place_match:
                place_id = place_match.group(1)
        
        # Helper for hours mapping
        def map_hours(day_data):
            if not day_data:
                return self._empty_day()
            
            # Check if it's a closed day
            if isinstance(day_data, dict) and day_data.get('open') == 'Closed':
                return {
                    'open': None,
                    'close': None,
                    'is_closed': True,
                    'is_24_hours': False,
                    'periods': []
                }
                
            return {
                'open': day_data.get('open'),
                'close': day_data.get('close'),
                'is_closed': False,
                'is_24_hours': day_data.get('open') == '00:00' and day_data.get('close') == '23:59',
                'periods': [{'open': day_data.get('open'), 'close': day_data.get('close')}] if day_data.get('open') else []
            }

        # Hours processing
        hours_data = core_data.get('hours', {}) or {}
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        operating_hours = {
            day: map_hours(hours_data.get(day)) for day in days
        }
        operating_hours.update({
            'current_status': core_data.get('business_status'),
            'opens_at': None,
            'closes_at': None,
            'is_24_hours': hours_data.get('is_24_hours', False),
            'is_temporarily_closed': core_data.get('business_status') == 'TEMPORARILY_CLOSED',
            'is_permanently_closed': core_data.get('business_status') == 'PERMANENTLY_CLOSED',
            'special_hours': [],
        })
        
        # Popular times processing
        pop_times = core_data.get('popular_times') or {}

        # Reviews processing
        review_details = core_data.get('review_details', {}) or {}
        breakdown = review_details.get('breakdown', {}) or {}
        
        # Attributes processing
        raw_attributes = core_data.get('attributes', [])
        # categorized attributes
        attributes = {
            'service_options': {},
            'accessibility': {},
            'offerings': {},
            'dining_options': {},
            'amenities': {},
            'atmosphere': {},
            'crowd': {},
            'planning': {},
            'payments': {},
            'children': {},
            'raw_attributes': raw_attributes,
        }
        
        # Simple keyword-based categorization for V2 compatibility
        for attr in raw_attributes:
            attr_lower = attr.lower()
            if 'wheelchair' in attr_lower:
                attributes['accessibility'][attr] = True
            elif 'credit' in attr_lower or 'pay' in attr_lower or 'cash' in attr_lower:
                attributes['payments'][attr] = True
            elif 'reservation' in attr_lower or 'appointment' in attr_lower:
                attributes['planning'][attr] = True
            elif 'delivery' in attr_lower or 'takeout' in attr_lower or 'dine-in' in attr_lower:
                attributes['service_options'][attr] = True
            elif 'kid' in attr_lower or 'child' in attr_lower:
                attributes['children'][attr] = True
            elif 'casual' in attr_lower or 'cozy' in attr_lower or 'romantic' in attr_lower:
                attributes['atmosphere'][attr] = True

        return {
            'basic_details': {
                'title': core_data.get('name'),
                'cid': cid,
                'place_id': place_id,
                'primary_category': core_data.get('primary_category'),
                'subcategories': core_data.get('additional_categories', []),
                'description': core_data.get('description'),
                'is_claimed': core_data.get('claimed_status'),
                'claim_status': 'CLAIMED' if core_data.get('claimed_status') is True else ('UNCLAIMED' if core_data.get('claimed_status') is False else 'UNKNOWN'),
            },
            'location_data': {
                'full_address': core_data.get('address'),
                'address_components': core_data.get('address_components', {}),
                'latitude': lat,
                'longitude': lng,
                'plus_code': core_data.get('plus_code'),
                'google_maps_url': url,
            },
            'contact_info': {
                'primary_phone': core_data.get('phone'),
                'additional_phones': [],
                'primary_email': None,
                'additional_emails': [],
                'website_url': core_data.get('website'),
                'domain': self._extract_domain(core_data.get('website')),
                'social_media': [],
                'menu_url': None,
                'order_url': None,
                'reservation_url': None,
            },
            'media_assets': {
                'total_photo_count': core_data.get('photo_count'),
                'video_count': core_data.get('video_count'),
                'photo_urls': core_data.get('photo_urls', []),
                'photo_details': core_data.get('photos', []),
                'cover_image_url': core_data.get('photo_urls', [None])[0] if core_data.get('photo_urls') else None,
                'logo_url': None,
                'street_view_available': False,
                'street_view_url': None,
                'has_360_photos': False,
                'photo_categories': [],
                'video_urls': [],
            },
            'reviews_ratings': {
                'overall_rating': core_data.get('rating'),
                'total_reviews': core_data.get('review_count'),
                'rating_distribution': {
                    'five_star': breakdown.get(5, 0),
                    'four_star': breakdown.get(4, 0),
                    'three_star': breakdown.get(3, 0),
                    'two_star': breakdown.get(2, 0),
                    'one_star': breakdown.get(1, 0)
                },
                'review_summaries': review_details.get('summaries', []),
                'place_topics': [],
                'recent_reviews': review_details.get('recent_reviews', []),
                'reviews_per_rating': {
                    5: breakdown.get(5, 0),
                    4: breakdown.get(4, 0),
                    3: breakdown.get(3, 0),
                    2: breakdown.get(2, 0),
                    1: breakdown.get(1, 0)
                },
            },
            'business_attributes': attributes,
            'operating_hours': operating_hours,
            'popular_times': {
                'monday': pop_times.get('monday', []),
                'tuesday': pop_times.get('tuesday', []),
                'wednesday': pop_times.get('wednesday', []),
                'thursday': pop_times.get('thursday', []),
                'friday': pop_times.get('friday', []),
                'saturday': pop_times.get('saturday', []),
                'sunday': pop_times.get('sunday', []),
                'live_busyness': pop_times.get('current_busyness'),
                'live_busyness_percent': pop_times.get('live_busyness_percent'),
                'typical_time_spent': None,
                'best_times_to_visit': [],
            },
            'competitive_data': {
                'people_also_search': [],
                'similar_places': [],
                'nearby_businesses': core_data.get('competitors', []),
            },
            'additional_data': {
                'qa_count': core_data.get('qa_count', 0),
                'qa_entries': [],
                'posts': core_data.get('owner_posts', []),
                'posts_count': core_data.get('post_count'),
                'last_post_date': core_data.get('last_post_date'),
                'menu_items': core_data.get('menu_items', []),
                'services': core_data.get('services', []),
                'products': core_data.get('products', []),
                'price_range': None,
                'founded_year': None,
                'years_in_business': None,
                'eco_certifications': [],
                'booking_links': [],
                'order_links': [],
            },
        }
    
    def _empty_day(self) -> Dict:
        """Return empty day structure."""
        return {
            'open': None,
            'close': None,
            'is_closed': False,
            'is_24_hours': False,
            'periods': [],
        }
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        if not url:
            return None
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return None
    
    def _empty_result(self, url: str, error: str) -> Dict[str, Any]:
        """Create empty result structure."""
        result = {category: {} for category in self.CATEGORIES}
        result['extraction_metadata'] = {
            'requested_url': url,
            'final_url': url,
            'extraction_time': 0,
            'success': False,
            'error': error,
        }
        return result
    
    def get_available_categories(self) -> List[str]:
        """Get list of available data categories."""
        return self.CATEGORIES.copy()


# ==================== Convenience Functions ====================

def extract_gmb_data(url: str, headless: bool = False) -> Dict[str, Any]:
    """
    Convenience function to extract GMB data from URL.
    
    Args:
        url: Google Maps URL
        headless: Run in headless mode
        
    Returns:
        Extracted business data
    """
    crawler = GMBCrawlerV2(headless=headless)
    return crawler.extract_full(url)


def extract_gmb_batch(urls: List[str], headless: bool = False) -> List[Dict[str, Any]]:
    """
    Convenience function for batch extraction.
    
    Args:
        urls: List of Google Maps URLs
        headless: Run in headless mode
        
    Returns:
        List of extracted business data
    """
    crawler = GMBCrawlerV2(headless=headless)
    return crawler.extract_batch(urls)
