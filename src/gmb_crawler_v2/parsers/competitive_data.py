"""
GMB Crawler V2 - Competitive Data Parser

Extracts competitor and related business information:
- People also search for
- Similar places
- Nearby businesses
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class CompetitiveDataParser(BaseParser):
    """
    Parser for competitive/related business data.
    """
    
    # Section selectors
    COMPETITOR_SELECTORS = [
        'div[aria-label*="People also search"]',
        'div[aria-label*="Similar"]',
        'div[aria-label*="Related"]',
        'div[class*="RiRi5e"]',  # Related business cards
    ]
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract competitive data.
        
        Returns:
            CompetitiveData TypedDict
        """
        result = {
            'people_also_search': self.extract_people_also_search(),
            'similar_places': self.extract_similar_places(),
            'nearby_businesses': self.extract_nearby_businesses(),
        }
        
        return result
    
    def extract_people_also_search(self) -> List[Dict[str, Any]]:
        """Extract 'People also search for' businesses."""
        if not self.soup:
            return []
        
        # Find the section
        section = self.soup.select_one('div[aria-label*="People also search"]')
        if not section:
            # Try scrolling section at bottom
            section = self.find_element_by_text('People also search')
            if section:
                section = section.find_parent('div')
        
        if not section:
            return []
        
        return self.parse_business_cards(section)
    
    def extract_similar_places(self) -> List[Dict[str, Any]]:
        """Extract similar places suggestions."""
        if not self.soup:
            return []
        
        # Find similar places section
        section = self.soup.select_one('div[aria-label*="Similar"]')
        if not section:
            section = self.find_element_by_text('Similar places')
            if section:
                section = section.find_parent('div')
        
        if not section:
            return []
        
        return self.parse_business_cards(section)
    
    def extract_nearby_businesses(self) -> List[Dict[str, Any]]:
        """Extract nearby businesses from map area."""
        if not self.soup:
            return []
        
        businesses = []
        
        # Look for nearby places in the listing
        nearby_section = self.soup.select_one('div[aria-label*="Nearby"]')
        if nearby_section:
            businesses = self.parse_business_cards(nearby_section)
        
        return businesses
    
    def parse_business_cards(self, container) -> List[Dict[str, Any]]:
        """Parse business cards from a container."""
        businesses = []
        
        if not container:
            return businesses
        
        # Find individual business cards
        card_selectors = [
            'div[jsaction*="click"]',
            'a[href*="/maps/place"]',
            'div[class*="card"]',
        ]
        
        cards = []
        for selector in card_selectors:
            cards = container.select(selector)
            if cards:
                break
        
        for card in cards[:10]:  # Limit to 10
            business = self.parse_single_card(card)
            if business and business.get('name'):
                businesses.append(business)
        
        return businesses
    
    def parse_single_card(self, card) -> Optional[Dict[str, Any]]:
        """Parse a single business card element."""
        if not card:
            return None
        
        business = {
            'name': None,
            'place_id': None,
            'rating': None,
            'review_count': None,
            'category': None,
            'address': None,
            'distance': None,
        }
        
        # Extract name
        name_el = card.select_one('div[class*="dbg0pd"], span[class*="title"], h3, h4')
        if name_el:
            business['name'] = name_el.get_text(strip=True)
        else:
            # Try aria-label
            label = card.get('aria-label', '')
            if label:
                # Name is often the first part before rating
                match = re.match(r'^([^·\d]+)', label)
                if match:
                    business['name'] = match.group(1).strip()
        
        # Extract place_id from link
        link = card.select_one('a[href*="/maps/place"]')
        if link:
            href = link.get('href', '')
            place_id = self.extract_place_id_from_url(href)
            if place_id:
                business['place_id'] = place_id
        
        # Extract rating
        rating_el = card.select_one('span[class*="yi40Hd"], span[aria-label*="star"]')
        if rating_el:
            text = rating_el.get_text(strip=True)
            rating = self.extract_float(text)
            if rating and 0 <= rating <= 5:
                business['rating'] = rating
        
        # Extract review count
        review_el = card.select_one('span[class*="UY7F9"]')
        if review_el:
            text = review_el.get_text(strip=True)
            # Format: (123)
            count = self.extract_number(text)
            if count:
                business['review_count'] = count
        
        # Extract category
        category_el = card.select_one('span[class*="jLbRgd"], span[class*="category"]')
        if category_el:
            business['category'] = category_el.get_text(strip=True)
        
        # Extract address
        address_el = card.select_one('span[class*="address"]')
        if address_el:
            business['address'] = address_el.get_text(strip=True)
        
        # Extract distance
        distance_el = card.select_one('span[class*="distance"]')
        if distance_el:
            business['distance'] = distance_el.get_text(strip=True)
        
        return business
    
    def extract_competitor_from_aria(self, element) -> Optional[Dict[str, Any]]:
        """Extract competitor info from aria-label."""
        if not element:
            return None
        
        label = element.get('aria-label', '')
        if not label:
            return None
        
        business = {
            'name': None,
            'place_id': None,
            'rating': None,
            'review_count': None,
            'category': None,
            'address': None,
            'distance': None,
        }
        
        # Parse aria-label format: "Name · Rating stars · Category"
        parts = label.split('·')
        
        if parts:
            business['name'] = parts[0].strip()
            
            for part in parts[1:]:
                part = part.strip()
                
                # Rating
                rating_match = re.search(r'(\d+\.?\d*)\s*star', part, re.IGNORECASE)
                if rating_match:
                    business['rating'] = float(rating_match.group(1))
                    continue
                
                # Review count
                review_match = re.search(r'\(?([\d,]+)\)?\s*review', part, re.IGNORECASE)
                if review_match:
                    business['review_count'] = int(review_match.group(1).replace(',', ''))
                    continue
                
                # Category (anything else that's short)
                if len(part) < 30 and not business['category']:
                    business['category'] = part
        
        return business if business.get('name') else None
