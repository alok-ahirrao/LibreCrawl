"""
GMB Crawler V2 - Contact Info Parser

Extracts contact information using robust extraction methods.
"""

import re
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse
from .base_parser import BaseParser


class ContactInfoParser(BaseParser):
    """
    Parser for contact information.
    Uses multiple fallback methods for robust extraction.
    """
    
    def parse(self) -> Dict[str, Any]:
        """Extract contact information."""
        result = {
            'primary_phone': self._extract_phone(),
            'additional_phones': [],
            'primary_email': self._extract_email(),
            'additional_emails': [],
            'website_url': self._extract_website(),
            'domain': None,
            'social_media': self._extract_social_media(),
            'menu_url': self._extract_menu_url(),
            'order_url': self._extract_order_url(),
            'reservation_url': self._extract_reservation_url(),
        }
        
        # Extract domain from website
        if result['website_url']:
            result['domain'] = self._get_domain(result['website_url'])
        
        return result
    
    def _extract_phone(self) -> Optional[str]:
        """
        Extract primary phone number.
        
        CSS Selector Reference (from guide):
        - Primary: button[data-item-id="phone:tel:XXXXX"]
        - Fallback: button[data-item-id*="phone"], a[href^="tel:"]
        - aria-label contains phone number
        """
        if not self.soup:
            return None
        
        # Method 1: Button with specific phone data-item-id (most reliable per guide)
        # Pattern: data-item-id="phone:tel:+1234567890"
        for phone_btn in self.soup.select('button[data-item-id^="phone:tel:"]'):
            data_id = phone_btn.get('data-item-id', '')
            match = re.search(r'phone:tel:([^\s]+)', data_id)
            if match:
                return match.group(1)
        
        # Method 2: Button with generic phone data-item-id
        phone_btn = self.soup.select_one('button[data-item-id*="phone"]')
        if phone_btn:
            # Get from aria-label first
            label = phone_btn.get('aria-label', '')
            if label:
                # Clean "Phone: " prefix
                phone = label.replace('Phone:', '').strip()
                if phone:
                    return phone
            # Get from text
            phone = phone_btn.get_text(strip=True)
            if phone and re.search(r'\d', phone):
                return phone
        
        # Method 3: aria-label containing phone number
        for el in self.soup.select('[aria-label*="phone"], [aria-label*="Phone"]'):
            label = el.get('aria-label', '')
            phone_match = re.search(r'[\+\(]?[0-9][0-9 .\-\(\)]{8,}[0-9]', label)
            if phone_match:
                return phone_match.group(0)
        
        # Method 4: Link with tel: href
        tel_link = self.soup.select_one('a[href^="tel:"]')
        if tel_link:
            href = tel_link.get('href', '')
            return href.replace('tel:', '')
        
        # Method 5: Text pattern search for US/international numbers
        text = self.soup.get_text()
        phone_patterns = [
            r'(?:\+1|1)?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}',  # US
            r'\+\d{1,3}[\s.-]?\d{2,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4}',  # International
        ]
        for pattern in phone_patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(0).strip()
        
        return None
    
    def _extract_email(self) -> Optional[str]:
        """Extract email address."""
        if not self.soup:
            return None
        
        # Method 1: mailto link
        mailto = self.soup.select_one('a[href^="mailto:"]')
        if mailto:
            href = mailto.get('href', '')
            return href.replace('mailto:', '').split('?')[0]
        
        # Method 2: Text pattern
        text = self.soup.get_text()
        email_pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        match = re.search(email_pattern, text)
        if match:
            return match.group(0)
        
        return None
    
    def _extract_website(self) -> Optional[str]:
        """Extract website URL."""
        if not self.soup:
            return None
        
        # Method 1: Button/link with website data-item-id
        website_btn = self.soup.select_one('a[data-item-id="authority"], button[data-item-id="authority"]')
        if website_btn:
            href = website_btn.get('href', '')
            if href and href.startswith('http'):
                return href
            # Get from aria-label
            label = website_btn.get('aria-label', '')
            if label:
                # Extract URL from label like "Website: example.com"
                url_match = re.search(r'https?://[^\s]+', label)
                if url_match:
                    return url_match.group(0)
                # Just domain
                if '.' in label:
                    domain = label.replace('Website:', '').strip()
                    if domain and ' ' not in domain:
                        return f'https://{domain}'
        
        # Method 2: aria-label with website
        for el in self.soup.select('[aria-label*="website"], [aria-label*="Website"]'):
            label = el.get('aria-label', '')
            url_match = re.search(r'https?://[^\s]+', label)
            if url_match:
                return url_match.group(0)
        
        # Method 3: Link with jsaction containing website
        for el in self.soup.select('a[jsaction*="website"]'):
            href = el.get('href', '')
            if href and href.startswith('http'):
                return href
        
        return None
    
    def _extract_social_media(self) -> List[Dict[str, Any]]:
        """Extract social media links."""
        social = []
        if not self.soup:
            return social
        
        social_patterns = {
            'facebook': r'facebook\.com',
            'instagram': r'instagram\.com',
            'twitter': r'twitter\.com|x\.com',
            'linkedin': r'linkedin\.com',
            'youtube': r'youtube\.com',
            'tiktok': r'tiktok\.com',
            'pinterest': r'pinterest\.com',
        }
        
        for link in self.soup.select('a[href]'):
            href = link.get('href', '')
            for platform, pattern in social_patterns.items():
                if re.search(pattern, href, re.IGNORECASE):
                    # Extract handle from URL
                    handle = None
                    handle_match = re.search(r'/([^/?\s]+)/?$', href)
                    if handle_match:
                        handle = handle_match.group(1)
                    
                    # Avoid duplicates
                    if not any(s['platform'] == platform for s in social):
                        social.append({
                            'platform': platform,
                            'url': href,
                            'handle': handle,
                        })
                    break
        
        return social
    
    def _extract_menu_url(self) -> Optional[str]:
        """Extract menu URL."""
        if not self.soup:
            return None
        
        menu_btn = self.soup.select_one('a[data-item-id*="menu"], button[aria-label*="Menu"]')
        if menu_btn:
            href = menu_btn.get('href', '')
            if href and href.startswith('http'):
                return href
        
        return None
    
    def _extract_order_url(self) -> Optional[str]:
        """Extract online ordering URL."""
        if not self.soup:
            return None
        
        for el in self.soup.select('a[data-item-id*="order"], button[aria-label*="order"]'):
            href = el.get('href', '')
            if href and href.startswith('http'):
                return href
            label = el.get('aria-label', '')
            url_match = re.search(r'https?://[^\s]+', label)
            if url_match:
                return url_match.group(0)
        
        return None
    
    def _extract_reservation_url(self) -> Optional[str]:
        """Extract reservation/booking URL."""
        if not self.soup:
            return None
        
        for el in self.soup.select('a[data-item-id*="reserv"], a[data-item-id*="book"], button[aria-label*="reserv"]'):
            href = el.get('href', '')
            if href and href.startswith('http'):
                return href
        
        return None
    
    def _get_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except:
            return None
