"""
GMB Crawler V2 - Operating Hours Parser

Extracts business hours using robust extraction methods.
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class OperatingHoursParser(BaseParser):
    """
    Parser for operating hours data.
    Uses multiple fallback methods for robust extraction.
    """
    
    DAY_NAMES = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    def parse(self) -> Dict[str, Any]:
        """Extract operating hours."""
        result = {
            'monday': self._empty_day(),
            'tuesday': self._empty_day(),
            'wednesday': self._empty_day(),
            'thursday': self._empty_day(),
            'friday': self._empty_day(),
            'saturday': self._empty_day(),
            'sunday': self._empty_day(),
            'current_status': self._extract_current_status(),
            'opens_at': None,
            'closes_at': None,
            'is_24_hours': False,
            'is_temporarily_closed': False,
            'is_permanently_closed': False,
            'special_hours': [],
        }
        
        # Extract hours for each day
        hours_data = self._extract_weekly_hours()
        for day, times in hours_data.items():
            if day in result:
                result[day] = times
        
        # Check for 24 hours
        result['is_24_hours'] = self._check_24_hours()
        
        # Check closed status
        result['is_temporarily_closed'] = self._check_temporarily_closed()
        result['is_permanently_closed'] = self._check_permanently_closed()
        
        return result
    
    def _empty_day(self) -> Dict[str, Any]:
        """Return empty day structure."""
        return {
            'open': None,
            'close': None,
            'is_closed': False,
            'is_24_hours': False,
            'periods': [],
        }
    
    def _extract_current_status(self) -> Optional[str]:
        """Extract current open/closed status."""
        if not self.soup:
            return None
        
        # Method 1: aria-label with open/closed
        for el in self.soup.select('[aria-label*="Open"], [aria-label*="Closed"]'):
            label = el.get('aria-label', '')
            if 'Opens' in label or 'Open now' in label:
                return label
            if 'Closes' in label or 'Closed' in label:
                return label
        
        # Method 2: Text search
        text = self.soup.get_text()
        
        # Open patterns
        if re.search(r'Open\s*now', text, re.IGNORECASE):
            return 'Open now'
        
        opens_match = re.search(r'Opens?\s+(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:AM|PM))', text, re.IGNORECASE)
        if opens_match:
            return f'Opens at {opens_match.group(1)}'
        
        # Closed patterns
        closes_match = re.search(r'Closes?\s+(?:at\s+)?(\d{1,2}(?::\d{2})?\s*(?:AM|PM))', text, re.IGNORECASE)
        if closes_match:
            return f'Closes at {closes_match.group(1)}'
        
        if 'Temporarily closed' in text:
            return 'Temporarily closed'
        
        if 'Permanently closed' in text:
            return 'Permanently closed'
        
        return None
    
    def _extract_weekly_hours(self) -> Dict[str, Dict]:
        """Extract hours for all days of the week."""
        hours = {}
        
        if not self.soup:
            return hours
        
        # Method 1: Look for hours table/grid
        for row in self.soup.select('table tr, div[class*="hours"] div'):
            text = row.get_text().lower()
            
            for day in self.DAY_NAMES:
                if day in text or day[:3] in text:
                    times = self._parse_times_from_text(text)
                    if times:
                        hours[day] = times
                        break
        
        # Method 2: aria-label patterns
        for el in self.soup.select('[aria-label]'):
            label = el.get('aria-label', '').lower()
            for day in self.DAY_NAMES:
                if day in label or day[:3] + ',' in label:
                    times = self._parse_times_from_text(label)
                    if times and day not in hours:
                        hours[day] = times
        
        # Method 3: Search in full text
        text = self.soup.get_text()
        for day in self.DAY_NAMES:
            if day not in hours:
                # Look for patterns like "Monday: 9 AM - 5 PM" or "Mon 9:00 AM–5:00 PM"
                # Pattern 1: Standard "Mon 9:00 AM – 5:00 PM"
                # Updated to be more flexible with spacing and separators
                pattern = rf'{day[:3]}[a-z]*\s*[:\s]*(\d{{1,2}}(?::\d{{2}})?\s*(?:AM|PM|am|pm)?)\s*[-–to]+\s*(\d{{1,2}}(?::\d{{2}})?\s*(?:AM|PM|am|pm)?)'
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    hours[day] = {
                        'open': match.group(1).upper(),
                        'close': match.group(2).upper(),
                        'is_closed': False,
                        'is_24_hours': False,
                        'periods': [{'open': match.group(1).upper(), 'close': match.group(2).upper()}],
                    }
                    continue

                # Pattern 2: "Mon: Closed"
                if re.search(rf'{day[:3]}[a-z]*\s*[:\s]*closed', text, re.IGNORECASE):
                    hours[day] = {
                        'open': None,
                        'close': None,
                        'is_closed': True,
                        'is_24_hours': False,
                        'periods': [],
                    }
                    continue
                
                # Pattern 3: "Mon: Open 24 hours"
                if re.search(rf'{day[:3]}[a-z]*\s*[:\s]*(?:open\s*)?24\s*hours', text, re.IGNORECASE):
                    hours[day] = {
                        'open': '12:00 AM',
                        'close': '11:59 PM',
                        'is_closed': False,
                        'is_24_hours': True,
                        'periods': [{'open': '12:00 AM', 'close': '11:59 PM'}],
                    }
                    continue
                    hours[day] = {
                        'open': None,
                        'close': None,
                        'is_closed': True,
                        'is_24_hours': False,
                        'periods': [],
                    }
        
        return hours
    
    def _parse_times_from_text(self, text: str) -> Optional[Dict]:
        """Parse opening/closing times from a text string."""
        # Check for closed
        if 'closed' in text.lower():
            return {
                'open': None,
                'close': None,
                'is_closed': True,
                'is_24_hours': False,
                'periods': [],
            }
        
        # Check for 24 hours
        if '24 hour' in text.lower() or 'open 24' in text.lower():
            return {
                'open': '12:00 AM',
                'close': '11:59 PM',
                'is_closed': False,
                'is_24_hours': True,
                'periods': [{'open': '12:00 AM', 'close': '11:59 PM'}],
            }
        
        # Extract time range
        time_pattern = r'(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)\s*[-–to]+\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM))'
        match = re.search(time_pattern, text, re.IGNORECASE)
        if match:
            return {
                'open': match.group(1).strip(),
                'close': match.group(2).strip(),
                'is_closed': False,
                'is_24_hours': False,
                'periods': [{'open': match.group(1).strip(), 'close': match.group(2).strip()}],
            }
        
        return None
    
    def _check_24_hours(self) -> bool:
        """Check if business is open 24 hours."""
        if not self.soup:
            return False
        
        text = self.soup.get_text().lower()
        return '24 hours' in text or 'open 24' in text
    
    def _check_temporarily_closed(self) -> bool:
        """Check if business is temporarily closed."""
        if not self.soup:
            return False
        
        text = self.soup.get_text().lower()
        return 'temporarily closed' in text
    
    def _check_permanently_closed(self) -> bool:
        """Check if business is permanently closed."""
        if not self.soup:
            return False
        
        text = self.soup.get_text().lower()
        return 'permanently closed' in text
