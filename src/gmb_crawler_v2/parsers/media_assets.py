"""
GMB Crawler V2 - Media Assets Parser

Extracts visual media information:
- Logo
- Cover/hero image
- Photo count and URLs
- Photo categories
- Video count and URLs
- Street View availability
- 360° photos
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class MediaAssetsParser(BaseParser):
    """
    Parser for media assets (photos, videos, Street View).
    """
    
    # Photo selectors
    PHOTO_SELECTORS = [
        'button[aria-label*="photo"]',
        'img[src*="googleusercontent.com"]',
        'div[class*="ZKCDEc"] img',
    ]
    
    # Photo category names
    PHOTO_CATEGORIES = [
        'All', 'Latest', 'Videos', 'By owner', 'Street View',
        'Food & drink', 'Menu', 'Atmosphere', 'Outside', 'Inside',
        '360°', 'Rooms', 'Lobby', 'Pool', 'Exterior'
    ]
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract media assets from Google Maps page.
        
        Returns:
            MediaAssets TypedDict
        """
        result = {
            'logo_url': self.extract_logo(),
            'cover_image_url': self.extract_cover_image(),
            'total_photo_count': self.extract_photo_count(),
            'photo_urls': self.extract_photo_urls(),
            'photo_details': [],  # Would need pagination/clicking to get full details
            'video_count': self.extract_video_count(),
            'video_urls': [],
            'street_view_available': self.check_street_view(),
            'street_view_url': self.extract_street_view_url(),
            'has_360_photos': self.check_360_photos(),
            'photo_categories': self.extract_photo_categories(),
        }
        
        return result
    
    def extract_logo(self) -> Optional[str]:
        """Extract business logo URL."""
        if not self.soup:
            return None
        
        # Logo is often in a specific container
        logo_selectors = [
            'img.lu1Ew',
            'img[class*="logo"]',
            'div[data-item-id="logo"] img',
        ]
        
        for selector in logo_selectors:
            element = self.soup.select_one(selector)
            if element:
                src = element.get('src', '')
                if src and 'googleusercontent' in src:
                    return self.get_high_res_url(src)
        
        return None
    
    def extract_cover_image(self) -> Optional[str]:
        """Extract the main cover/hero image URL."""
        if not self.soup:
            return None
        
        # Cover image is usually the first large image
        cover_selectors = [
            'div.ZKCDEc img',
            'div[role="img"]',
            'button[aria-label*="Photo"] img',
            'div[jsname] > img[src*="googleusercontent"]',
        ]
        
        for selector in cover_selectors:
            element = self.soup.select_one(selector)
            if element:
                src = element.get('src', '')
                if src and 'googleusercontent' in src:
                    return self.get_high_res_url(src)
        
        return None
    
    def extract_photo_count(self) -> Optional[int]:
        """
        Extract total number of photos.
        Falls back to length of extracted photo_urls if explicit count not found.
        """
        count = None
        
        if self.soup:
            # Look for photo count in buttons/labels
            # Priority: Complex number format (1,234) -> Parentheses format (Photos (5)) -> Simple format (5 Photos)
            patterns = [
                r'(\d{1,3}(?:,\d{3})*)\s*photos?',   # 1,234 photos
                r'photos?\s*\((\d{1,3}(?:,\d{3})*)\)', # Photos (1,234)
                r'photos?\s*\((\d+)\)',               # Photos (5)
                r'See\s+all\s+(\d{1,3}(?:,\d{3})*)',  # See all 1,234
                r'See\s+all\s+(\d+)',                 # See all 5
                r'(\d+)\s*photos?',                   # 5 photos
                r'(\d+)\s*images?',                   # 5 images
                r'All\s*\((\d{1,3}(?:,\d{3})*)\)',    # All (1,234) - Common in photo tab
                r'All\s*\((\d+)\)',                   # All (5)
                r'(\d{1,3}(?:,\d{3})*)\s*media',      # 1,234 media
            ]
            
            # Check broad range of elements
            photo_elements = self.soup.select('button, div[role="tab"], div[aria-label], span[aria-label]')
            
            for el in photo_elements:
                # Check aria-label
                label = el.get('aria-label', '')
                if label:
                    for pattern in patterns:
                        match = re.search(pattern, label, re.IGNORECASE)
                        if match:
                            try:
                                count_str = match.group(1).replace(',', '')
                                parsed_count = int(count_str)
                                # Sanity check: GMB photos are usually > 0. Avoid parsing years like "2023 photos"
                                if parsed_count < 1000000: 
                                    if count is None or parsed_count > count:
                                         count = parsed_count
                            except ValueError:
                                continue
                
                # Check text content for some elements
                if el.name in ['button', 'div'] and not count:
                    text = el.get_text()
                    if text:
                        for pattern in patterns:
                             match = re.search(pattern, text, re.IGNORECASE)
                             if match:
                                try:
                                    count_str = match.group(1).replace(',', '')
                                    parsed_count = int(count_str)
                                    if count is None or parsed_count > count:
                                        count = parsed_count
                                except ValueError:
                                    continue

        # FALLBACK: Use length of extracted photos if they exist
        # This ensures we never return 0/None if we actually have photos
        extracted_photos = self.extract_photo_urls()
        photo_len = len(extracted_photos)
        
        if count is None:
            return photo_len if photo_len > 0 else None
            
        # If extracted count is higher than parsed count (rare but possible)
        # return the higher number
        return max(count, photo_len)
    
    def extract_photo_urls(self, max_photos: int = 1000) -> List[str]:
        """
        Extract sample photo URLs visible on the page.
        Increased default max_photos to capture more gallery items.
        
        CSS Selector Reference (from guide):
        - Business photos: img[src*="AF1QipM"], img[src*="AF1QipN"], img[src*="AF1QipO"]
        - These prefixes indicate user-uploaded business photos vs Google's imagery
        
        Args:
            max_photos: Maximum number of photos to extract
        """
        photos = []
        
        if not self.soup:
            return photos
            
        # 0. Check for injected gallery photos (from geo_driver.py)
        injected_script = self.soup.select_one('script#extracted-gallery-photos')
        if injected_script:
            import json
            try:
                gallery_urls = json.loads(injected_script.string or '[]')
                if gallery_urls:
                    # Clean the URLs just in case
                    cleaned_gallery = [self.get_high_res_url(u) for u in gallery_urls]
                    photos.extend(cleaned_gallery)
            except Exception:
                pass
        
        # Priority 1: Find AF1Qip* photos (business-uploaded per guide)
        # These patterns indicate user-uploaded content:
        # - AF1QipM, AF1QipN, AF1QipO = business photos
        af1_patterns = ['AF1QipM', 'AF1QipN', 'AF1QipO']
        
        for pattern in af1_patterns:
            for img in self.soup.select(f'img[src*="{pattern}"]'):
                if len(photos) >= max_photos:
                    break
                src = img.get('src', '')
                if src:
                    high_res = self.get_high_res_url(src)
                    if high_res not in photos:
                        photos.append(high_res)
        
        # Priority 2: Other googleusercontent images (covers fallback)
        if len(photos) < max_photos:
            img_elements = self.soup.select('img[src*="googleusercontent"]')
            
            for img in img_elements:
                if len(photos) >= max_photos:
                    break
                
                src = img.get('src', '')
                if src:
                    # Skip maps/street view markers (contain /v1/ or streetview)
                    if '/v1/' in src or 'streetview' in src.lower():
                        continue
                    # Get high-res version
                    high_res = self.get_high_res_url(src)
                    if high_res not in photos:
                        photos.append(high_res)
        
        return photos[:max_photos]
    
    def get_high_res_url(self, url: str) -> str:
        """
        Convert Google photo URL to high resolution version.
        
        Google URLs often have size params like =w100-h100
        Remove or replace to get full resolution.
        """
        if not url:
            return url
        
        # Remove size constraints
        # Format: ...=w100-h200-... or ...=s100-...
        cleaned = re.sub(r'=w\d+-h\d+.*$', '', url)
        cleaned = re.sub(r'=s\d+.*$', '', cleaned)
        
        # Add high-res param if nothing at end
        if not cleaned.endswith('='):
            cleaned += '=s0'  # s0 = original size
        
        return cleaned
    
    def extract_video_count(self) -> Optional[int]:
        """Extract number of videos."""
        if not self.soup:
            return None
        
        # Look for "Videos" tab with count
        # Try multiple selectors for the video tab/button
        video_selectors = [
             'button[aria-label*="Video"]',
             'div[aria-label*="Video"]',
             'button[data-tab-index="Videos"]' 
        ]
        
        for selector in video_selectors:
            elements = self.soup.select(selector)
            for el in elements:
                label = el.get('aria-label', '') or el.get_text()
                # Pattern: Videos (5) or 5 videos
                patterns = [
                    r'Videos?\s*\((\d+)\)',
                    r'(\d+)\s*videos?'
                ]
                for pattern in patterns:
                    match = re.search(pattern, label, re.IGNORECASE)
                    if match:
                        return int(match.group(1).replace(',', ''))
        
        # Check for video category in photo categories chips
        video_category = self.find_element_by_text(r'Videos?\s*\(\d+\)')
        if video_category:
            text = video_category.get_text() if hasattr(video_category, 'get_text') else str(video_category)
            match = re.search(r'(\d+)', text)
            if match:
                return int(match.group(1))
        
        return None
    
    def check_street_view(self) -> bool:
        """Check if Street View is available."""
        if not self.soup:
            return False
        
        # Look for Street View button/image
        street_view_indicators = [
            'button[aria-label*="Street View"]',
            'div[aria-label*="Street View"]',
            'img[alt*="Street View"]',
            '[data-item-id="streetview"]',
        ]
        
        for selector in street_view_indicators:
            if self.soup.select_one(selector):
                return True
        
        # Check text
        if self.find_element_by_text('Street View'):
            return True
        
        return False
    
    def extract_street_view_url(self) -> Optional[str]:
        """Extract Street View URL if available."""
        if not self.soup:
            return None
        
        # Street View links often have specific format
        street_view_link = self.soup.select_one('a[href*="layer=c"]')
        if street_view_link:
            return street_view_link.get('href')
        
        street_view_link = self.soup.select_one('a[href*="!1s"]')
        if street_view_link:
            href = street_view_link.get('href', '')
            if 'streetview' in href.lower() or 'pano' in href.lower():
                return href
        
        return None
    
    def check_360_photos(self) -> bool:
        """Check if 360° photos are available."""
        if not self.soup:
            return False
        
        # Look for 360 indicators
        indicators_360 = [
            'button[aria-label*="360"]',
            'div[aria-label*="360"]',
            'img[alt*="360"]',
        ]
        
        for selector in indicators_360:
            if self.soup.select_one(selector):
                return True
        
        # Check text
        if self.find_element_by_text(r'360\s*°?'):
            return True
        
        return False
    
    def extract_photo_categories(self) -> List[str]:
        """Extract available photo filter categories."""
        categories = []
        
        if not self.soup:
            return categories
        
        # Photo categories are usually in tab buttons
        category_buttons = self.soup.select('button[role="tab"]')
        
        for button in category_buttons:
            text = button.get_text(strip=True)
            # Remove count in parentheses
            text = re.sub(r'\s*\(\d+\)\s*', '', text)
            
            if text and text in self.PHOTO_CATEGORIES:
                categories.append(text)
        
        # Alternative: look for category chips
        if not categories:
            for category in self.PHOTO_CATEGORIES:
                if self.find_element_by_text(category):
                    categories.append(category)
        
        return list(set(categories))  # Remove duplicates
