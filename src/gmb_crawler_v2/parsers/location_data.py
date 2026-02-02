"""
GMB Crawler V2 - Location Data Parser

Extracts location and address information using robust extraction methods.
"""

import re
from typing import Optional, List, Dict, Any, Tuple
from .base_parser import BaseParser


class LocationDataParser(BaseParser):
    """
    Parser for location and address data.
    Uses multiple fallback methods for robust extraction.
    """
    
    def parse(self) -> Dict[str, Any]:
        """Extract location data."""
        lat, lng = self._extract_coordinates()
        
        result = {
            'full_address': self._extract_full_address(),
            'address_components': self._parse_address_components(),
            'latitude': lat,
            'longitude': lng,
            'plus_code': self._extract_plus_code(),
            'google_maps_url': self.url,
        }
        
        return result
    
    def _extract_full_address(self) -> Optional[str]:
        """Extract full address string."""
        if not self.soup:
            return None
        
        # Method 1: Button with address data-item-id
        addr_btn = self.soup.select_one('button[data-item-id="address"]')
        if addr_btn:
            label = addr_btn.get('aria-label', '')
            if label:
                # Clean "Address: " prefix
                address = label.replace('Address:', '').strip()
                if address:
                    return address
            text = addr_btn.get_text(strip=True)
            if text:
                return text
        
        # Method 2: aria-label containing address
        for el in self.soup.select('[aria-label*="address"], [aria-label*="Address"]'):
            label = el.get('aria-label', '')
            if label and len(label) > 10:
                return label.replace('Address:', '').strip()
        
        # Method 3: div with address class
        for selector in ['div[class*="address"]', 'span[class*="address"]', 'div.rogA2c']:
            try:
                el = self.soup.select_one(selector)
                if el:
                    text = el.get_text(strip=True)
                    if text and len(text) > 10:
                        return text
            except:
                continue
        
        # Method 4: Look for text patterns that look like addresses
        text = self.soup.get_text()
        
        # Pattern for US addresses: street number + street + city, state ZIP
        us_pattern = r'\d+\s+\w+(?:\s+\w+)*,\s*\w+(?:\s+\w+)*,\s*[A-Z]{2}\s+\d{5}'
        match = re.search(us_pattern, text)
        if match:
            return match.group(0)
        
        return None
    
    def _parse_address_components(self) -> Dict[str, Optional[str]]:
        """Parse address into components."""
        components = {
            'street_number': None,
            'street_name': None,
            'street_address': None,
            'city': None,
            'state': None,
            'postal_code': None,
            'country': None,
            'country_code': None,
            'neighborhood': None,
            'sublocality': None,
        }
        
        full_address = self._extract_full_address()
        if not full_address:
            return components
        
        # Try to parse the address
        parts = [p.strip() for p in full_address.split(',')]
        
        if len(parts) >= 1:
            # First part is usually street address
            street = parts[0]
            street_match = re.match(r'^(\d+)\s+(.+)', street)
            if street_match:
                components['street_number'] = street_match.group(1)
                components['street_name'] = street_match.group(2)
            components['street_address'] = street
        
        if len(parts) >= 2:
            # Second part might be city or neighborhood
            components['city'] = parts[1].strip()
        
        if len(parts) >= 3:
            # Third part might be state + ZIP
            state_zip = parts[2].strip()
            # Match patterns like "CA 90210" or "California 90210"
            match = re.match(r'^([A-Za-z\s]+)\s+(\d{5}(?:-\d{4})?)', state_zip)
            if match:
                components['state'] = match.group(1).strip()
                components['postal_code'] = match.group(2)
            else:
                components['state'] = state_zip
        
        if len(parts) >= 4:
            # Fourth part is usually country
            components['country'] = parts[3].strip()
        
        return components
    
    def _extract_coordinates(self) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude."""
        # First try from URL
        lat, lng = self.extract_coordinates_from_url()
        if lat and lng:
            return lat, lng
        
        # Try from page meta tags
        if self.soup:
            # OG meta tags
            og_url = self.soup.select_one('meta[property="og:url"]')
            if og_url:
                content = og_url.get('content', '')
                url_lat, url_lng = self._parse_coords_from_url(content)
                if url_lat and url_lng:
                    return url_lat, url_lng
            
            # Canonical link
            canonical = self.soup.select_one('link[rel="canonical"]')
            if canonical:
                href = canonical.get('href', '')
                url_lat, url_lng = self._parse_coords_from_url(href)
                if url_lat and url_lng:
                    return url_lat, url_lng
        
        return None, None
    
    def _parse_coords_from_url(self, url: str) -> Tuple[Optional[float], Optional[float]]:
        """Parse coordinates from a URL string."""
        if not url:
            return None, None
        
        # Method 1: !3d and !4d format
        lat_match = re.search(r'!3d(-?\d+\.\d+)', url)
        lng_match = re.search(r'!4d(-?\d+\.\d+)', url)
        if lat_match and lng_match:
            return float(lat_match.group(1)), float(lng_match.group(1))
        
        # Method 2: @lat,lng format
        coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
        if coord_match:
            return float(coord_match.group(1)), float(coord_match.group(2))
        
        return None, None
    
    def _extract_plus_code(self) -> Optional[str]:
        """Extract Plus Code (Open Location Code)."""
        if not self.soup:
            return None
        
        # Plus codes look like: "V5VW+FR New York, USA" or "V5VW+FR"
        text = self.soup.get_text()
        
        # Pattern for plus codes
        plus_pattern = r'([A-Z0-9]{4}\+[A-Z0-9]{2,})'
        match = re.search(plus_pattern, text)
        if match:
            return match.group(1)
        
        return None
