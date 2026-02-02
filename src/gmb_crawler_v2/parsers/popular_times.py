"""
GMB Crawler V2 - Popular Times Parser

Extracts traffic and busyness data:
- Hourly traffic by day (0-100 popularity index)
- Live busyness indicator
- Typical time spent
- Best times to visit
"""

import re
from typing import Optional, Dict, Any, List
from .base_parser import BaseParser


class PopularTimesParser(BaseParser):
    """
    Parser for popular times / traffic data.
    """
    
    # Day names for iteration
    DAYS = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    # Busyness level labels
    BUSYNESS_LABELS = {
        (0, 20): 'Not busy',
        (20, 40): 'A little busy',
        (40, 60): 'Moderately busy',
        (60, 80): 'Busy',
        (80, 100): 'Very busy',
    }
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract popular times data.
        
        Returns:
            PopularTimes TypedDict
        """
        result = {
            'monday': [],
            'tuesday': [],
            'wednesday': [],
            'thursday': [],
            'friday': [],
            'saturday': [],
            'sunday': [],
            'live_busyness': None,
            'live_busyness_percent': None,
            'typical_time_spent': self.extract_typical_time_spent(),
            'best_times_to_visit': [],
        }
        
        if not self.soup:
            return result
        
        # Extract weekly data
        weekly_data = self.extract_weekly_data()
        for day, data in weekly_data.items():
            if day in result:
                result[day] = data
        
        # Extract live busyness
        live_info = self.extract_live_busyness()
        result['live_busyness'] = live_info.get('label')
        result['live_busyness_percent'] = live_info.get('percent')
        
        # Calculate best times
        result['best_times_to_visit'] = self.calculate_best_times(weekly_data)
        
        return result
    
    def extract_weekly_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract hourly traffic data for each day."""
        data = {day: [] for day in self.DAYS}
        
        if not self.soup:
            return data
        
        # Look for popular times section
        pop_times_section = self.soup.select_one('div[aria-label*="Popular times"]')
        if not pop_times_section:
            pop_times_section = self.soup.select_one('div[class*="popular"]')
        
        if not pop_times_section:
            return data
        
        # Find day tabs/buttons
        day_buttons = pop_times_section.select('button[aria-label]')
        
        for day_name in self.DAYS:
            day_data = self.extract_day_data(pop_times_section, day_name)
            if day_data:
                data[day_name] = day_data
        
        # Alternative: extract from chart bars
        if not any(data.values()):
            chart_data = self.extract_from_chart()
            if chart_data:
                data.update(chart_data)
        
        return data
    
    def extract_day_data(self, section, day_name: str) -> List[Dict[str, Any]]:
        """Extract hourly data for a specific day."""
        hourly_data = []
        
        if not section:
            return hourly_data
        
        # Look for bars/chart elements
        # Popular times often uses divs with height representing popularity
        bars = section.select('div[class*="bar"], div[style*="height"]')
        
        for i, bar in enumerate(bars[:24]):  # Max 24 hours
            height = self.extract_bar_height(bar)
            if height is not None:
                hour = 6 + i  # Usually starts at 6 AM
                if hour >= 24:
                    hour -= 24
                
                hourly_data.append({
                    'hour': hour,
                    'popularity': height,
                    'label': self.get_busyness_label(height),
                })
        
        return hourly_data
    
    def extract_bar_height(self, bar) -> Optional[int]:
        """Extract popularity percentage from bar height."""
        if not bar:
            return None
        
        # Check style for height
        style = bar.get('style', '')
        height_match = re.search(r'height:\s*(\d+)%', style)
        if height_match:
            return int(height_match.group(1))
        
        # Check aria-label
        label = bar.get('aria-label', '')
        percent_match = re.search(r'(\d+)%', label)
        if percent_match:
            return int(percent_match.group(1))
        
        # Check for data attribute
        data_value = bar.get('data-value')
        if data_value and data_value.isdigit():
            return int(data_value)
        
        return None
    
    def extract_from_chart(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extract popular times from chart elements."""
        data = {}
        
        if not self.soup:
            return data
        
        # Look for SVG chart or chart container
        chart = self.soup.select_one('g[class*="popular"], svg[aria-label*="Popular"]')
        if not chart:
            return data
        
        # Extract bar data from chart
        bars = chart.select('rect, path')
        
        # This is simplified - real implementation would need more complex parsing
        for bar in bars:
            # Extract height and position to determine hour/day
            pass
        
        return data
    
    def extract_live_busyness(self) -> Dict[str, Any]:
        """Extract current live busyness indicator."""
        result = {'label': None, 'percent': None}
        
        if not self.soup:
            return result
        
        # Look for "Live" indicator
        live_indicators = [
            'div[aria-label*="Live"]',
            'span[class*="live"]',
            'div[class*="currently"]',
        ]
        
        for selector in live_indicators:
            element = self.soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                result['label'] = text
                
                # Extract percentage if present
                percent_match = re.search(r'(\d+)%', text)
                if percent_match:
                    result['percent'] = int(percent_match.group(1))
                
                # Check aria-label for more detail
                label = element.get('aria-label', '')
                if label:
                    result['label'] = label
                
                break
        
        # Also check for "Usually X busy" text
        busy_text = self.find_element_by_text('Usually')
        if busy_text:
            text = busy_text.get_text() if hasattr(busy_text, 'get_text') else str(busy_text)
            if 'busy' in text.lower():
                result['label'] = text
        
        return result
    
    def extract_typical_time_spent(self) -> Optional[str]:
        """Extract typical time spent at location."""
        if not self.soup:
            return None
        
        # Look for time spent text
        time_patterns = [
            r'People typically spend\s+(.+?)(?:here|at this)',
            r'typical(?:ly)?\s+spend\s+(.+)',
            r'(\d+\s*(?:min|hour|hr)s?\s*(?:to|-)\s*\d+\s*(?:min|hour|hr)s?)',
        ]
        
        page_text = self.soup.get_text()
        
        for pattern in time_patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        # Look for specific element
        time_element = self.soup.select_one('[aria-label*="time spent"], [data-item-id*="time"]')
        if time_element:
            return time_element.get_text(strip=True)
        
        return None
    
    def calculate_best_times(self, weekly_data: Dict[str, List]) -> List[str]:
        """
        Calculate best times to visit based on low traffic periods.
        
        Returns:
            List of strings like "Tuesday 2 PM", "Wednesday 10 AM"
        """
        best_times = []
        
        if not weekly_data:
            return best_times
        
        # Find periods with low popularity (< 30%)
        low_threshold = 30
        
        for day, hourly_data in weekly_data.items():
            for hour_info in hourly_data:
                popularity = hour_info.get('popularity', 100)
                hour = hour_info.get('hour', 0)
                
                if popularity < low_threshold and 8 <= hour <= 20:  # During business hours
                    # Format time
                    am_pm = 'AM' if hour < 12 else 'PM'
                    display_hour = hour if hour <= 12 else hour - 12
                    if display_hour == 0:
                        display_hour = 12
                    
                    time_str = f"{day.capitalize()} {display_hour} {am_pm}"
                    best_times.append(time_str)
        
        # Limit to top 5 best times
        return best_times[:5]
    
    def get_busyness_label(self, popularity: int) -> str:
        """Get human-readable busyness label for popularity score."""
        for (low, high), label in self.BUSYNESS_LABELS.items():
            if low <= popularity < high:
                return label
        
        return 'Very busy' if popularity >= 80 else 'Not busy'
