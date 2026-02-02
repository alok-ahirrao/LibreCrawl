"""
GMB Crawler V2 - Basic Details Parser

Extracts core business identification data using robust extraction methods.
"""

import re
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup
from .base_parser import BaseParser


class BasicDetailsParser(BaseParser):
    """
    Parser for basic business identification data.
    Uses multiple fallback methods for robust extraction.
    """
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract basic details from Google Maps page.
        """
        result = {
            'title': self._extract_title(),
            'cid': self._extract_cid(),
            'place_id': self._extract_place_id(),
            'primary_category': self._extract_primary_category(),
            'subcategories': self._extract_subcategories(),
            'description': self._extract_description(),
            'is_claimed': None,
            'claim_status': 'UNKNOWN',
        }
        
        # Extract claim status
        claimed = self._extract_claim_status()
        result['is_claimed'] = claimed
        if claimed is True:
            result['claim_status'] = 'CLAIMED'
        elif claimed is False:
            result['claim_status'] = 'UNCLAIMED'
        
        return result
    
    def _extract_title(self) -> Optional[str]:
        """
        Extract business name using multiple methods.
        
        CSS Selector Reference (from guide):
        - Primary: h1.DUwDvf or h1.fontHeadlineLarge
        - Fallback: meta[property="og:title"], split on ' · '
        - Last: title tag, remove " - Google Maps" suffix
        """
        if not self.soup:
            return self._extract_name_from_url()
        
        # Method 1: Try multiple h1 selectors (most reliable)
        name_selectors = [
            'h1.DUwDvf',
            'h1.fontHeadlineLarge',
            'h1',
            'span.DUwDvf',
            'div[role="main"] h1',
            'div.lMbq3e h1',
            'div[data-attrid="title"] span',
        ]
        
        for selector in name_selectors:
            try:
                title = self.soup.select_one(selector)
                if title:
                    name_text = title.get_text(strip=True)
                    if name_text and len(name_text) > 1:
                        return name_text
            except:
                continue
        
        # Method 2: OG meta tag (per parsing guide)
        og_title = self.soup.select_one('meta[property="og:title"]')
        if og_title:
            content = og_title.get('content', '')
            if content:
                # OG title format is usually "Business Name · Category · Address"
                # Take first part before '·' or '-'
                parts = re.split(r'\s*[·\-]\s*', content)
                if parts and len(parts[0].strip()) > 1:
                    return parts[0].strip()
        
        # Method 3: Extract from title tag
        title_tag = self.soup.find('title')
        if title_tag:
            title_text = title_tag.get_text(strip=True)
            # Title format is usually "Business Name - Google Maps"
            if ' - Google Maps' in title_text:
                return title_text.replace(' - Google Maps', '').strip()
            elif title_text and 'Google Maps' not in title_text:
                return title_text.strip()
        
        # Method 4: Extract from aria-label on main
        main = self.soup.select_one('div[role="main"]')
        if main:
            label = main.get('aria-label', '')
            if label and len(label) > 1 and len(label) < 100:
                # Clean concatenated info like "Name4.5(123)"
                match = re.search(r'^(.+?)(\d+\.?\d*)', label)
                if match:
                    name = match.group(1).strip()
                    if len(name) > 1:
                        return name
                return label
        
        # Method 5: Extract from URL
        return self._extract_name_from_url()
    
    def _extract_name_from_url(self) -> Optional[str]:
        """Extract business name from URL path."""
        if not self.url:
            return None
        
        # Match /place/{name}/ pattern
        match = re.search(r'/place/([^/@]+)', self.url)
        if match:
            name = match.group(1)
            from urllib.parse import unquote
            name = unquote(name).replace('+', ' ')
            return name
        
        return None
    
    def _extract_cid(self) -> Optional[str]:
        """Extract CID (Customer ID) from URL or page."""
        # First try URL
        cid = self.extract_cid_from_url()
        if cid:
            return cid
        
        # Try to find in page data
        if self.soup:
            scripts = self.soup.find_all('script')
            for script in scripts:
                content = script.string or ''
                match = re.search(r'"ludocid":"(\d+)"', content)
                if match:
                    return match.group(1)
        
        return None
    
    def _extract_place_id(self) -> Optional[str]:
        """Extract Place ID from URL or page."""
        # First try URL
        place_id = self.extract_place_id_from_url()
        if place_id:
            return place_id
        
        # Try to find in page data
        if self.soup:
            scripts = self.soup.find_all('script')
            for script in scripts:
                content = script.string or ''
                patterns = [
                    r'"place_id":"([^"]+)"',
                    r'"placeId":"([^"]+)"',
                ]
                for pattern in patterns:
                    match = re.search(pattern, content)
                    if match:
                        result = match.group(1)
                        if result.startswith('ChI') or result.startswith('0x'):
                            return result
        
        return None
    
    def _extract_primary_category(self) -> Optional[str]:
        """
        Extract primary business category.
        
        CSS Selector Reference (from guide):
        - Primary: button[jsaction*="category"]
        - Fallback: button.DkEaL, font classes (fontButtonSmall, fontBodyMedium)
        - From OG title: second part after ' · '
        """
        if not self.soup:
            return None
        
        # Method 1: button with category action (most reliable)
        cat_btn = self.soup.select_one('button[jsaction*="category"]')
        if cat_btn:
            text = cat_btn.get_text(strip=True)
            if text and len(text) < 50:
                return text
        
        # Method 2: Look for category patterns with guide selectors
        category_selectors = [
            'button.DkEaL',
            'span.DkEaL',
            'button.fontButtonSmall',  # Added from guide
            'span.fontButtonSmall',    # Added from guide
            'div[class*="fontBodyMedium"]',
        ]
        
        for selector in category_selectors:
            try:
                elements = self.soup.select(selector)
                for el in elements:
                    text = el.get_text(strip=True)
                    if text and len(text) < 40 and self._is_likely_category(text):
                        return text
            except:
                continue
        
        # Method 3: Extract from OG title (second part after '·')
        og_title = self.soup.select_one('meta[property="og:title"]')
        if og_title:
            content = og_title.get('content', '')
            if content and '·' in content:
                parts = content.split('·')
                if len(parts) >= 2:
                    cat = parts[1].strip()
                    if cat and len(cat) < 50 and self._is_likely_category(cat):
                        return cat
        
        # Method 4: Parse from header text
        main = self.soup.select_one('div[role="main"]')
        if main:
            text = main.get_text()[:500]
            # Pattern: "Rating · Category" or "4.5(123) · Category"
            match = re.search(r'·\s*([A-Za-z][A-Za-z\s]{2,30})(?:·|$)', text)
            if match:
                cat = match.group(1).strip()
                if self._is_likely_category(cat):
                    return cat
        
        return None
    
    def _extract_subcategories(self) -> List[str]:
        """Extract additional categories."""
        categories = []
        if not self.soup:
            return categories
        
        # Look for all category buttons
        cat_btns = self.soup.select('button[jsaction*="category"]')
        for btn in cat_btns[1:]:  # Skip first (primary)
            text = btn.get_text(strip=True)
            if text and len(text) < 50 and text not in categories:
                categories.append(text)
        
        return categories
    
    def _is_likely_category(self, text: str) -> bool:
        """Check if text looks like a business category."""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Filter out common non-category strings
        exclude = [
            'open', 'closed', 'temporarily', 'permanently',
            'hours', 'directions', 'website', 'call', 'save',
            'share', 'photos', 'reviews', 'about', 'menu',
            'am', 'pm', 'km', 'mi', 'min', '$'
        ]
        
        for exc in exclude:
            if exc in text_lower:
                return False
        
        # Categories are usually short
        if len(text) > 50 or len(text) < 3:
            return False
        
        # Should contain mostly letters
        letter_count = sum(1 for c in text if c.isalpha())
        if letter_count < len(text) * 0.5:
            return False
        
        return True
    
    def _extract_description(self) -> Optional[str]:
        """Extract business description."""
        if not self.soup:
            return None
        
        # Try multiple description selectors
        desc_selectors = [
            'div.PYvSYb',
            'div[data-attrid="kc:/local:merchant_description"]',
            'div.WeS02d',
            'div[aria-label*="About"]',
        ]
        
        for selector in desc_selectors:
            try:
                el = self.soup.select_one(selector)
                if el:
                    text = el.get_text(strip=True)
                    if text and len(text) > 20:
                        # Clean "About" prefix if present
                        if text.startswith('About'):
                            text = text[5:].strip()
                        return text
            except:
                continue
        
        return None
    
    def _extract_claim_status(self) -> Optional[bool]:
        """Extract whether business is claimed/verified."""
        if not self.soup:
            return None
        
        # Look for verified indicators
        verified_selectors = [
            'svg[aria-label*="verified"]',
            'span[aria-label*="verified"]',
            'img[alt*="verified"]',
            '[class*="verified"]',
        ]
        
        for selector in verified_selectors:
            try:
                if self.soup.select_one(selector):
                    return True
            except:
                continue
        
        # Look for "Claim this business" button
        text = self.soup.get_text().lower()
        if 'claim this business' in text:
            return False
        
        if 'own this business' in text:
            return False
        
        # Check for owner responses (indicates claimed)
        if 'response from the owner' in text:
            return True
        
        return None
