"""
GMB Crawler V2 - Business Attributes Parser

Extracts comprehensive business attributes:
- Service options (dine-in, takeout, delivery)
- Accessibility features
- Offerings (alcohol, vegan options, etc.)
- Dining options
- Amenities (WiFi, restroom, etc.)
- Atmosphere (casual, upscale, etc.)
- Crowd type (family-friendly, LGBTQ+ friendly)
- Payment methods
- Planning (reservations, appointments)
- Children features
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class BusinessAttributesParser(BaseParser):
    """
    Parser for business attributes and amenities.
    """
    
    # Attribute mapping: text pattern -> (category, attribute, value)
    ATTRIBUTE_PATTERNS = {
        # Service Options
        'dine-in': ('service_options', 'dine_in', True),
        'no dine-in': ('service_options', 'dine_in', False),
        'takeout': ('service_options', 'takeout', True),
        'takeaway': ('service_options', 'takeout', True),
        'no takeout': ('service_options', 'takeout', False),
        'delivery': ('service_options', 'delivery', True),
        'no delivery': ('service_options', 'delivery', False),
        'curbside pickup': ('service_options', 'curbside_pickup', True),
        'no-contact delivery': ('service_options', 'no_contact_delivery', True),
        'outdoor seating': ('service_options', 'outdoor_seating', True),
        'drive-through': ('service_options', 'drive_through', True),
        
        # Accessibility
        'wheelchair accessible entrance': ('accessibility', 'wheelchair_accessible_entrance', True),
        'wheelchair-accessible entrance': ('accessibility', 'wheelchair_accessible_entrance', True),
        'wheelchair accessible seating': ('accessibility', 'wheelchair_accessible_seating', True),
        'wheelchair-accessible seating': ('accessibility', 'wheelchair_accessible_seating', True),
        'wheelchair accessible restroom': ('accessibility', 'wheelchair_accessible_restroom', True),
        'wheelchair-accessible restroom': ('accessibility', 'wheelchair_accessible_restroom', True),
        'wheelchair accessible parking': ('accessibility', 'wheelchair_accessible_parking', True),
        'wheelchair-accessible parking': ('accessibility', 'wheelchair_accessible_parking', True),
        
        # Offerings
        'serves alcohol': ('offerings', 'serves_alcohol', True),
        'serves beer': ('offerings', 'serves_beer', True),
        'beer': ('offerings', 'serves_beer', True),
        'serves cocktails': ('offerings', 'serves_cocktails', True),
        'cocktails': ('offerings', 'serves_cocktails', True),
        'serves wine': ('offerings', 'serves_wine', True),
        'wine': ('offerings', 'serves_wine', True),
        'serves coffee': ('offerings', 'serves_coffee', True),
        'coffee': ('offerings', 'serves_coffee', True),
        'vegan options': ('offerings', 'serves_vegan', True),
        'serves vegetarian': ('offerings', 'serves_vegetarian', True),
        'vegetarian options': ('offerings', 'serves_vegetarian', True),
        'halal food': ('offerings', 'halal_food', True),
        'halal': ('offerings', 'halal_food', True),
        'organic dishes': ('offerings', 'organic_dishes', True),
        'small plates': ('offerings', 'small_plates', True),
        'late-night food': ('offerings', 'late_night_food', True),
        'happy hour': ('offerings', 'happy_hour', True),
        
        # Dining Options
        'serves breakfast': ('dining_options', 'serves_breakfast', True),
        'breakfast': ('dining_options', 'serves_breakfast', True),
        'serves brunch': ('dining_options', 'serves_brunch', True),
        'brunch': ('dining_options', 'serves_brunch', True),
        'serves lunch': ('dining_options', 'serves_lunch', True),
        'lunch': ('dining_options', 'serves_lunch', True),
        'serves dinner': ('dining_options', 'serves_dinner', True),
        'dinner': ('dining_options', 'serves_dinner', True),
        'serves dessert': ('dining_options', 'serves_dessert', True),
        'dessert': ('dining_options', 'serves_dessert', True),
        'has seating': ('dining_options', 'has_seating', True),
        'seating available': ('dining_options', 'has_seating', True),
        'catering': ('dining_options', 'has_catering', True),
        'counter service': ('dining_options', 'counter_service', True),
        
        # Amenities
        'bar onsite': ('amenities', 'has_bar_onsite', True),
        'has bar': ('amenities', 'has_bar_onsite', True),
        'restroom': ('amenities', 'has_restroom', True),
        'has restroom': ('amenities', 'has_restroom', True),
        'wi-fi': ('amenities', 'has_wifi', True),
        'wifi': ('amenities', 'has_wifi', True),
        'free wi-fi': ('amenities', 'free_wifi', True),
        'free wifi': ('amenities', 'free_wifi', True),
        'dogs allowed': ('amenities', 'dogs_allowed', True),
        'pet friendly': ('amenities', 'dogs_allowed', True),
        'live music': ('amenities', 'live_music', True),
        'live performances': ('amenities', 'live_performances', True),
        'rooftop seating': ('amenities', 'rooftop_seating', True),
        'rooftop': ('amenities', 'rooftop_seating', True),
        'fireplace': ('amenities', 'fireplace', True),
        'private dining': ('amenities', 'private_dining', True),
        
        # Atmosphere
        'casual': ('atmosphere', 'casual', True),
        'cozy': ('atmosphere', 'cozy', True),
        'upscale': ('atmosphere', 'upscale', True),
        'trendy': ('atmosphere', 'trendy', True),
        'romantic': ('atmosphere', 'romantic', True),
        'historic': ('atmosphere', 'historic', True),
        'modern': ('atmosphere', 'modern', True),
        
        # Crowd
        'family-friendly': ('crowd', 'family_friendly', True),
        'good for kids': ('crowd', 'family_friendly', True),
        'lgbtq+ friendly': ('crowd', 'lgbtq_friendly', True),
        'lgbtq friendly': ('crowd', 'lgbtq_friendly', True),
        'transgender safe space': ('crowd', 'transgender_safe_space', True),
        'good for groups': ('crowd', 'good_for_groups', True),
        'groups': ('crowd', 'good_for_groups', True),
        'popular with tourists': ('crowd', 'popular_with_tourists', True),
        'tourists': ('crowd', 'popular_with_tourists', True),
        'college': ('crowd', 'college_crowd', True),
        
        # Payments
        'credit cards': ('payments', 'accepts_credit_cards', True),
        'accepts credit cards': ('payments', 'accepts_credit_cards', True),
        'debit cards': ('payments', 'accepts_debit_cards', True),
        'accepts debit cards': ('payments', 'accepts_debit_cards', True),
        'nfc mobile payments': ('payments', 'accepts_nfc', True),
        'mobile payments': ('payments', 'accepts_nfc', True),
        'cash only': ('payments', 'accepts_cash_only', True),
        'cash-only': ('payments', 'accepts_cash_only', True),
        'checks': ('payments', 'accepts_checks', True),
        
        # Planning
        'reservations': ('planning', 'accepts_reservations', True),
        'accepts reservations': ('planning', 'accepts_reservations', True),
        'reservations required': ('planning', 'reservations_required', True),
        'walk-ins welcome': ('planning', 'walk_ins_welcome', True),
        'appointment required': ('planning', 'appointment_required', True),
        
        # Children
        'good for kids': ('children', 'good_for_kids', True),
        'kids menu': ('children', 'has_kids_menu', True),
        "kids' menu": ('children', 'has_kids_menu', True),
        'high chairs': ('children', 'has_high_chairs', True),
        'highchairs': ('children', 'has_high_chairs', True),
        'changing tables': ('children', 'has_changing_tables', True),
    }
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract business attributes.
        
        Returns:
            BusinessAttributes TypedDict
        """
        result = {
            'service_options': {},
            'accessibility': {},
            'offerings': {},
            'dining_options': {},
            'amenities': {},
            'atmosphere': {},
            'crowd': {},
            'payments': {},
            'planning': {},
            'children': {},
            'raw_attributes': [],
        }
        
        if not self.soup:
            return result
        
        # Extract all attribute text from the page
        raw_attributes = self.extract_raw_attributes()
        result['raw_attributes'] = raw_attributes
        
        # Parse each attribute
        for attr_text in raw_attributes:
            self.parse_attribute(attr_text, result)
        
        return result
    
    def extract_raw_attributes(self) -> List[str]:
        """Extract all attribute strings from the page."""
        attributes = []
        
        if not self.soup:
            return attributes
        
        # Attribute selectors - various layouts used by Google
        selectors = [
            'div[class*="LTs0Rc"]',  # Attribute rows
            'div[aria-label*="Highlights"]',
            'div[aria-label*="Service options"]',
            'div[aria-label*="Accessibility"]',
            'div[aria-label*="Offerings"]',
            'div[aria-label*="Dining options"]',
            'div[aria-label*="Amenities"]',
            'div[aria-label*="Atmosphere"]',
            'div[aria-label*="Crowd"]',
            'div[aria-label*="Planning"]',
            'div[aria-label*="Payments"]',
            'span.YhemCb',  # Attribute names
            'span.RcCsl',  # Attribute chips
        ]
        
        for selector in selectors:
            elements = self.soup.select(selector)
            for el in elements:
                text = el.get_text(strip=True)
                if text and len(text) < 100:  # Filter out long text
                    # Clean up the text
                    text = self.clean_attribute_text(text)
                    if text and text not in attributes:
                        attributes.append(text)
        
        # Also look in About section
        about_section = self.soup.select_one('div[aria-label="About"]')
        if about_section:
            # Find attribute lists
            list_items = about_section.select('li, div[role="listitem"]')
            for item in list_items:
                text = item.get_text(strip=True)
                text = self.clean_attribute_text(text)
                if text and text not in attributes:
                    attributes.append(text)
        
        return attributes
    
    def clean_attribute_text(self, text: str) -> str:
        """Clean and normalize attribute text."""
        if not text:
            return text
        
        # Remove checkmarks and other symbols
        text = re.sub(r'[✓✔✕✖☑☒]', '', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing punctuation
        text = text.strip(' ·•')
        
        return text.lower()
    
    def parse_attribute(self, attr_text: str, result: Dict) -> None:
        """
        Parse a single attribute and add to result.
        
        Args:
            attr_text: Raw attribute text
            result: Result dict to update
        """
        if not attr_text:
            return
        
        attr_lower = attr_text.lower()
        
        # Check against known patterns
        for pattern, (category, attribute, value) in self.ATTRIBUTE_PATTERNS.items():
            if pattern in attr_lower:
                if category in result:
                    result[category][attribute] = value
                break
    
    def get_attribute_categories(self) -> List[str]:
        """Get list of all attribute category names."""
        return [
            'service_options',
            'accessibility',
            'offerings',
            'dining_options',
            'amenities',
            'atmosphere',
            'crowd',
            'payments',
            'planning',
            'children',
        ]
