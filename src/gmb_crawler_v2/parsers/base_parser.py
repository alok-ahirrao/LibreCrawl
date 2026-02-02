"""
GMB Crawler V2 - Base Parser

Common utilities and base class for all specialized parsers.
"""

import re
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote

logger = logging.getLogger(__name__)


class BaseParser:
    """
    Base class for all GMB parsers.
    Provides common utilities for HTML parsing and data extraction.
    """
    
    def __init__(self, html_content: str = None, soup: BeautifulSoup = None, url: str = None):
        """
        Initialize parser with HTML content or BeautifulSoup object.
        
        Args:
            html_content: Raw HTML string
            soup: Pre-parsed BeautifulSoup object
            url: Original URL for reference
        """
        if soup:
            self.soup = soup
        elif html_content:
            self.soup = BeautifulSoup(html_content, 'html.parser')
        else:
            self.soup = None
        
        self.url = url
        self.errors: List[str] = []
        self.warnings: List[str] = []
    
    def set_html(self, html_content: str) -> None:
        """Update parser with new HTML content."""
        self.soup = BeautifulSoup(html_content, 'html.parser')
        self.errors = []
        self.warnings = []
    
    def set_soup(self, soup: BeautifulSoup) -> None:
        """Update parser with pre-parsed soup."""
        self.soup = soup
        self.errors = []
        self.warnings = []
    
    def parse(self) -> Dict[str, Any]:
        """
        Parse and return extracted data.
        Override in subclasses.
        """
        raise NotImplementedError("Subclasses must implement parse()")
    
    # ==================== URL Utilities ====================
    
    def extract_place_id_from_url(self, url: str = None) -> Optional[str]:
        """
        Extract Place ID from Google Maps URL.
        
        URLs can have formats like:
        - .../data=!3m1!4b1!4m6!3m5!1s0x89c259...:0x...
        - ftid=0x89c259...
        """
        url = url or self.url
        if not url:
            return None
        
        # Try ftid parameter
        if 'ftid=' in url:
            match = re.search(r'ftid=([^&]+)', url)
            if match:
                return match.group(1)
        
        # Try !1s format in data parameter
        match = re.search(r'!1s(0x[a-fA-F0-9]+:[a-fA-F0-9x]+)', url)
        if match:
            return match.group(1)
        
        # Try place_id in URL path
        match = re.search(r'/place/[^/]+/([^/]+)/', url)
        if match and match.group(1).startswith('Ch'):
            return match.group(1)
        
        return None
    
    def extract_cid_from_url(self, url: str = None) -> Optional[str]:
        """
        Extract CID (Customer ID / ludocid) from URL.
        """
        url = url or self.url
        if not url:
            return None
        
        # Try ludocid parameter
        match = re.search(r'ludocid=(\d+)', url)
        if match:
            return match.group(1)
        
        # Try cid parameter
        match = re.search(r'cid=(\d+)', url)
        if match:
            return match.group(1)
        
        # Try extracting from data parameter (after 0x...:)
        match = re.search(r':0x([a-fA-F0-9]+)', url)
        if match:
            # Convert hex to decimal
            try:
                return str(int(match.group(1), 16))
            except ValueError:
                pass
        
        return None
    
    def extract_coordinates_from_url(self, url: str = None) -> Tuple[Optional[float], Optional[float]]:
        """
        Extract latitude and longitude from Google Maps URL.
        
        Format: @lat,lng,zoom or !3d{lat}!4d{lng}
        """
        url = url or self.url
        if not url:
            return None, None
        
        # Try @lat,lng,zoom format
        match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
        if match:
            return float(match.group(1)), float(match.group(2))
        
        # Try !3d and !4d format
        lat_match = re.search(r'!3d(-?\d+\.\d+)', url)
        lng_match = re.search(r'!4d(-?\d+\.\d+)', url)
        if lat_match and lng_match:
            return float(lat_match.group(1)), float(lng_match.group(2))
        
        return None, None
    
    # ==================== Text Extraction Utilities ====================
    
    def get_text_by_selector(self, selector: str, default: str = None) -> Optional[str]:
        """Get text content from first matching element."""
        if not self.soup:
            return default
        
        element = self.soup.select_one(selector)
        if element:
            return element.get_text(strip=True)
        return default
    
    def get_all_text_by_selector(self, selector: str) -> List[str]:
        """Get text from all matching elements."""
        if not self.soup:
            return []
        
        elements = self.soup.select(selector)
        return [el.get_text(strip=True) for el in elements if el.get_text(strip=True)]
    
    def get_attr_by_selector(self, selector: str, attr: str, default: str = None) -> Optional[str]:
        """Get attribute value from first matching element."""
        if not self.soup:
            return default
        
        element = self.soup.select_one(selector)
        if element:
            return element.get(attr, default)
        return default
    
    def find_element_by_text(self, text: str, tag: str = None) -> Optional[Any]:
        """Find element containing specific text."""
        if not self.soup:
            return None
        
        if tag:
            return self.soup.find(tag, string=re.compile(text, re.IGNORECASE))
        return self.soup.find(string=re.compile(text, re.IGNORECASE))
    
    def find_elements_by_text(self, text: str, tag: str = None) -> List[Any]:
        """Find all elements containing specific text."""
        if not self.soup:
            return []
        
        if tag:
            return self.soup.find_all(tag, string=re.compile(text, re.IGNORECASE))
        return self.soup.find_all(string=re.compile(text, re.IGNORECASE))
    
    # ==================== Data Extraction Utilities ====================
    
    def extract_number(self, text: str, default: int = None) -> Optional[int]:
        """Extract first number from text."""
        if not text:
            return default
        
        # Remove commas and other separators
        text = text.replace(',', '').replace(' ', '')
        match = re.search(r'(\d+)', text)
        if match:
            return int(match.group(1))
        return default
    
    def extract_float(self, text: str, default: float = None) -> Optional[float]:
        """Extract first decimal number from text."""
        if not text:
            return default
        
        match = re.search(r'(\d+\.?\d*)', text)
        if match:
            return float(match.group(1))
        return default
    
    def extract_phone(self, text: str) -> Optional[str]:
        """Extract phone number from text."""
        if not text:
            return None
        
        # Match various phone formats
        pattern = r'[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]'
        match = re.search(pattern, text)
        if match:
            return match.group(0).strip()
        return None
    
    def extract_email(self, text: str) -> Optional[str]:
        """Extract email address from text."""
        if not text:
            return None
        
        pattern = r'[\w\.-]+@[\w\.-]+\.\w+'
        match = re.search(pattern, text)
        if match:
            return match.group(0)
        return None
    
    def extract_domain(self, url: str) -> Optional[str]:
        """Extract domain from URL."""
        if not url:
            return None
        
        try:
            parsed = urlparse(url)
            domain = parsed.netloc
            # Remove www. prefix
            if domain.startswith('www.'):
                domain = domain[4:]
            return domain
        except Exception:
            return None
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text."""
        if not text:
            return ""
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    def parse_json_from_script(self, pattern: str = None) -> Optional[Dict]:
        """
        Extract JSON data from script tags.
        
        Args:
            pattern: Regex pattern to find specific JSON
        """
        if not self.soup:
            return None
        
        scripts = self.soup.find_all('script')
        for script in scripts:
            content = script.string
            if not content:
                continue
            
            if pattern:
                match = re.search(pattern, content)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except json.JSONDecodeError:
                        continue
            else:
                # Try to find any JSON object
                try:
                    start = content.find('{')
                    end = content.rfind('}')
                    if start != -1 and end != -1:
                        return json.loads(content[start:end+1])
                except json.JSONDecodeError:
                    continue
        
        return None
    
    # ==================== Aria Label Parsing ====================
    
    def parse_aria_label(self, element) -> Dict[str, str]:
        """
        Parse data from aria-label attribute.
        Often contains structured info like "4.5 stars 1,234 reviews"
        """
        if not element:
            return {}
        
        label = element.get('aria-label', '')
        result = {}
        
        # Extract rating
        rating_match = re.search(r'(\d+\.?\d*)\s*star', label, re.IGNORECASE)
        if rating_match:
            result['rating'] = float(rating_match.group(1))
        
        # Extract review count
        review_match = re.search(r'([\d,]+)\s*review', label, re.IGNORECASE)
        if review_match:
            result['reviews'] = int(review_match.group(1).replace(',', ''))
        
        # Extract price level
        price_match = re.search(r'(\$+)', label)
        if price_match:
            result['price_level'] = price_match.group(1)
        
        return result
    
    # ==================== Error Handling ====================
    
    def add_error(self, message: str) -> None:
        """Add an extraction error."""
        self.errors.append(message)
        logger.error(f"[{self.__class__.__name__}] {message}")
    
    def add_warning(self, message: str) -> None:
        """Add an extraction warning."""
        self.warnings.append(message)
        logger.warning(f"[{self.__class__.__name__}] {message}")
    
    def safe_extract(self, func, default=None, error_msg: str = None):
        """
        Safely execute extraction function with error handling.
        """
        try:
            result = func()
            return result if result is not None else default
        except Exception as e:
            if error_msg:
                self.add_warning(f"{error_msg}: {str(e)}")
            else:
                self.add_warning(f"Extraction error: {str(e)}")
            return default
