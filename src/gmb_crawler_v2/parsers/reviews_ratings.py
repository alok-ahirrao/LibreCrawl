"""
GMB Crawler V2 - Reviews & Ratings Parser

Extracts review and rating data using robust extraction methods.
Based on comprehensive GMB HTML parsing guide patterns.

CSS Selector Reference (from guide):
- Overall Rating: [aria-label*="stars"]
- Review Count: [aria-label*="reviews"]
- Rating Distribution: tr[aria-label*="stars"]
- Individual Reviews: div[data-review-id]
- Reviewer Name: div.fontBodyMedium
- Review Rating: span[aria-label*="star"]
- Review Text: span.fontBodyMedium (>50 chars)
- Review Date: span.fontBodySmall (contains "ago")
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class ReviewsRatingsParser(BaseParser):
    """
    Parser for reviews and ratings data.
    Uses multiple fallback methods for robust extraction.
    """
    
    # Regex patterns from the parsing guide
    PATTERNS = {
        'rating': r'(\d+\.?\d*)\s*stars?',
        'review_count': r'([\d,]+)\s*reviews?',
        'rating_and_count': r'(\d+\.?\d*)\s*stars?,\s*([\d,]+)\s*reviews?',
        'date_relative': r'\d+\s*(?:day|week|month|year)s?\s*ago',
    }
    
    def parse(self) -> Dict[str, Any]:
        """Extract reviews and ratings data."""
        result = {
            'overall_rating': self._extract_rating(),
            'total_reviews': self._extract_review_count(),
            'rating_distribution': self._extract_rating_distribution(),
            'review_summaries': self._extract_review_summaries(),
            'place_topics': self._extract_place_topics(),
            'recent_reviews': self._extract_recent_reviews(),
            'reviews_per_rating': {
                1: 0, 2: 0, 3: 0, 4: 0, 5: 0
            },
        }
        
        # Populate reviews_per_rating from distribution
        dist = result['rating_distribution']
        result['reviews_per_rating'] = {
            5: dist.get('five_star', 0),
            4: dist.get('four_star', 0),
            3: dist.get('three_star', 0),
            2: dist.get('two_star', 0),
            1: dist.get('one_star', 0),
        }
        
        return result
    
    def _extract_rating(self) -> Optional[float]:
        """Extract overall rating using multiple fallback methods."""
        if not self.soup:
            return None
        
        # Method 1: Combined rating and reviews in aria-label (most reliable)
        # Pattern: "4.5 stars, 347 reviews" or "4.5 stars 347 reviews"
        for el in self.soup.select('[aria-label]'):
            label = el.get('aria-label', '')
            match = re.search(self.PATTERNS['rating_and_count'], label, re.IGNORECASE)
            if match:
                return float(match.group(1))
        
        # Method 2: div[role="img"] with star rating aria-label
        rating_div = self.soup.select_one('div[role="img"][aria-label*="star"]')
        if rating_div:
            label = rating_div.get('aria-label', '')
            match = re.search(self.PATTERNS['rating'], label, re.IGNORECASE)
            if match:
                return float(match.group(1))
        
        # Method 3: span[role="img"] with stars
        for el in self.soup.select('span[role="img"][aria-label*="star"]'):
            label = el.get('aria-label', '')
            match = re.search(self.PATTERNS['rating'], label, re.IGNORECASE)
            if match:
                return float(match.group(1))
        
        # Method 4: Any element with star rating aria-label
        for el in self.soup.select('[aria-label]'):
            label = el.get('aria-label', '')
            match = re.search(r'(\d\.?\d?)\s*(?:out of 5|stars?|star rating)', label, re.IGNORECASE)
            if match:
                try:
                    rating = float(match.group(1))
                    if 1.0 <= rating <= 5.0:
                        return rating
                except ValueError:
                    continue
        
        # Method 5: Text pattern in header - rating followed by parenthetical count
        # Pattern: "4.5(123)" or "4.5 (123)"
        main = self.soup.select_one('div[role="main"]')
        if main:
            text = main.get_text()[:800]
            match = re.search(r'(\d\.\d)\s*[\(\s]*\d{1,6}[\)\s]*(?:reviews?)?', text, re.IGNORECASE)
            if match:
                return float(match.group(1))
        
        # Method 6: Look for fontDisplayLarge or rating span classes
        for selector in ['span.ceNzKf', 'span.fontDisplayLarge', 'span.F7nice span:first-child', 
                         'div.F7nice span', 'span.e4rVHe']:
            try:
                el = self.soup.select_one(selector)
                if el:
                    text = el.get_text(strip=True)
                    val = float(text)
                    if 1.0 <= val <= 5.0:
                        return val
            except (ValueError, AttributeError):
                continue
        
        # Method 7: JSON-LD structured data
        for script in self.soup.select('script[type="application/ld+json"]'):
            try:
                import json
                data = json.loads(script.string or '{}')
                if isinstance(data, dict):
                    agg_rating = data.get('aggregateRating', {})
                    if agg_rating.get('ratingValue'):
                        return float(agg_rating['ratingValue'])
            except:
                continue
        
        return None
    
    def _extract_review_count(self) -> Optional[int]:
        """Extract total review count using multiple fallback methods."""
        if not self.soup:
            return None
            
        full_text = self.soup.get_text()[:5000] # Get more context
        
        # Method 1: Combined rating and reviews in aria-label (Best/Most common)
        for el in self.soup.select('[aria-label]'):
            label = el.get('aria-label', '')
            match = re.search(self.PATTERNS['rating_and_count'], label, re.IGNORECASE)
            if match:
                return int(match.group(2).replace(',', ''))
        
        # Method 2: Regex search in full text (very reliable fallback)
        # Look for "(1,234)" after a rating-like number
        # Pattern: 4.8 (1,234)
        text_match = re.search(r'\d\.\d\s*\(([\d,]+)\)', full_text)
        if text_match:
            try:
                count = int(text_match.group(1).replace(',', ''))
                if 1 <= count < 1000000: # Sanity check
                    return count
            except: pass

        # Method 3: "1,234 reviews" text pattern
        reviews_text_match = re.search(r'([\d,]+)\s+reviews?', full_text, re.IGNORECASE)
        if reviews_text_match:
            try:
                count = int(reviews_text_match.group(1).replace(',', ''))
                return count
            except: pass

        # Method 4: Button with reviews aria-label
        reviews_btn = self.soup.select_one('button[aria-label*="review"]')
        if reviews_btn:
            label = reviews_btn.get('aria-label', '')
            match = re.search(self.PATTERNS['review_count'], label, re.IGNORECASE)
            if match:
                return int(match.group(1).replace(',', ''))
        
        # Method 5: Link with reviews
        reviews_link = self.soup.select_one('a[href*="reviews"], button[jsaction*="reviews"], a[data-item-id*="review"]')
        if reviews_link:
            text = reviews_link.get_text(strip=True)
            match = re.search(r'([0-9,]+)', text)
            if match:
                return int(match.group(1).replace(',', ''))

        # Method 6: K/M suffix (e.g. 1.2K reviews)
        km_match = re.search(r'([0-9.]+)\s*([KkMm])\s*reviews?', full_text)
        if km_match:
            num = float(km_match.group(1))
            suffix = km_match.group(2).upper()
            if suffix == 'K':
                return int(num * 1000)
            elif suffix == 'M':
                return int(num * 1000000)
        
        # Method 7: JSON-LD structured data
        for script in self.soup.select('script[type="application/ld+json"]'):
            try:
                import json
                data = json.loads(script.string or '{}')
                if isinstance(data, dict):
                    agg_rating = data.get('aggregateRating', {})
                    if agg_rating.get('reviewCount'):
                        return int(agg_rating['reviewCount'])
                    elif agg_rating.get('ratingCount'):
                        return int(agg_rating['ratingCount'])
            except:
                continue
        
        return None
    
    def _extract_rating_distribution(self) -> Dict[str, int]:
        """Extract rating distribution (5-star, 4-star, etc.) from table rows."""
        distribution = {
            'five_star': 0,
            'four_star': 0,
            'three_star': 0,
            'two_star': 0,
            'one_star': 0,
        }
        
        if not self.soup:
            return distribution
        
        key_map = {1: 'one_star', 2: 'two_star', 3: 'three_star', 4: 'four_star', 5: 'five_star'}
        
        # Method 1: tr elements with aria-labels like "5 stars, 347 reviews"
        for row in self.soup.select('tr[aria-label]'):
            label = row.get('aria-label', '')
            match = re.search(r'(\d+)\s*stars?,?\s*(\d+)\s*reviews?', label, re.IGNORECASE)
            if match:
                star_count = int(match.group(1))
                review_count = int(match.group(2))
                if 1 <= star_count <= 5:
                    distribution[key_map[star_count]] = review_count
        
        # Method 2: Any element with "X stars, Y reviews" pattern
        if sum(distribution.values()) == 0:
            for star_count in [5, 4, 3, 2, 1]:
                pattern = rf'{star_count}\s*stars?\s*[,:\s]*(\d[\d,]*)\s*reviews?'
                for el in self.soup.select('[aria-label]'):
                    label = el.get('aria-label', '')
                    match = re.search(pattern, label, re.IGNORECASE)
                    if match:
                        distribution[key_map[star_count]] = int(match.group(1).replace(',', ''))
                        break
        
        # Method 3: Look in table cells
        if sum(distribution.values()) == 0:
            tables = self.soup.select('table')
            for table in tables:
                rows = table.select('tr')
                for row in rows:
                    text = row.get_text()
                    for star_count in [5, 4, 3, 2, 1]:
                        pattern = rf'{star_count}\s*stars?\s*[,:\s]*(\d[\d,]*)'
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match and distribution[key_map[star_count]] == 0:
                            distribution[key_map[star_count]] = int(match.group(1).replace(',', ''))
        
        return distribution
    
    def _extract_review_summaries(self) -> List[str]:
        """Extract review summary highlights/chips."""
        summaries = []
        if not self.soup:
            return summaries
        
        # Look for review summary chips/tags with attribute data
        for el in self.soup.select('button[data-item-id*="attribute"]'):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 50:
                summaries.append(text)
        
        # Also look for chip-style elements
        for el in self.soup.select('div[class*="chip"] span, button[class*="chip"]'):
            text = el.get_text(strip=True)
            if text and 3 < len(text) < 50 and text not in summaries:
                summaries.append(text)
        
        return summaries[:10]  # Limit to 10
    
    def _extract_place_topics(self) -> List[str]:
        """Extract place topics/keywords mentioned in reviews."""
        topics = []
        if not self.soup:
            return topics
        
        # Look for topic chips
        for el in self.soup.select('button[class*="chip"], span[class*="chip"], div[class*="topic"]'):
            text = el.get_text(strip=True)
            if text and 2 < len(text) < 30 and text not in topics:
                # Filter out common non-topic strings
                if not any(x in text.lower() for x in ['review', 'photo', 'star', 'rating']):
                    topics.append(text)
        
        return topics[:15]  # Limit to 15
    
    def _extract_recent_reviews(self) -> List[Dict[str, Any]]:
        """Extract recent individual reviews using data-review-id containers."""
        reviews = []
        if not self.soup:
            return reviews
        
        # Primary method: div[data-review-id] containers (most reliable)
        review_containers = self.soup.select('div[data-review-id]')
        
        # Fallback: div with review-related classes
        if not review_containers:
            review_containers = self.soup.select('div[class*="review"]')
        
        for container in review_containers[:10]:  # Limit to 10
            review = self._parse_single_review(container)
            if review and (review.get('text') or review.get('rating')):
                reviews.append(review)
        
        return reviews
    
    def _parse_single_review(self, container) -> Dict[str, Any]:
        """Parse a single review container using guide patterns."""
        review = {
            'review_id': container.get('data-review-id'),
            'author_name': None,
            'author_photo_url': None,
            'author_review_count': None,
            'author_is_local_guide': False,
            'rating': None,
            'date': None,
            'relative_date': None,
            'text': None,
            'language': None,
            'has_owner_response': False,
            'owner_response_text': None,
            'owner_response_date': None,
            'photos': [],
        }
        
        try:
            # Author name - first fontBodyMedium in review div (per guide)
            author_el = container.select_one('div.fontBodyMedium')
            if author_el:
                name = author_el.get_text(strip=True)
                # Filter out if name looks like review text (too long)
                if len(name) < 50:
                    review['author_name'] = name
            
            # Fallback author extraction
            if not review['author_name']:
                for sel in ['button[jsaction*="reviewer"]', 'a[data-reviewer]', 'div[class*="reviewer"]']:
                    author_el = container.select_one(sel)
                    if author_el:
                        review['author_name'] = author_el.get_text(strip=True)
                        break
            
            # Author photo
            author_img = container.select_one('img[src*="googleusercontent.com"]')
            if author_img:
                src = author_img.get('src', '')
                # Typically author photos are small
                if 'gstatic' not in src:
                    review['author_photo_url'] = src
            
            # Rating from span[aria-label*="star"] (per guide)
            rating_el = container.select_one('span[aria-label*="star"]')
            if rating_el:
                label = rating_el.get('aria-label', '')
                match = re.search(r'(\d)', label)
                if match:
                    review['rating'] = int(match.group(1))
            
            # Fallback rating
            if not review['rating']:
                rating_el = container.select_one('span[role="img"][aria-label*="star"]')
                if rating_el:
                    label = rating_el.get('aria-label', '')
                    match = re.search(r'(\d)', label)
                    if match:
                        review['rating'] = int(match.group(1))
            
            # Date from fontBodySmall containing "ago" (per guide)
            for date_el in container.select('span.fontBodySmall'):
                text = date_el.get_text(strip=True)
                if re.search(self.PATTERNS['date_relative'], text, re.IGNORECASE):
                    review['relative_date'] = text
                    break
            
            # Fallback date extraction
            if not review['relative_date']:
                date_el = container.select_one('span[class*="date"], span[class*="time"]')
                if date_el:
                    review['relative_date'] = date_el.get_text(strip=True)
            
            # Review text - long fontBodyMedium span (per guide: >50 chars)
            for text_el in container.select('span.fontBodyMedium'):
                text = text_el.get_text(strip=True)
                if len(text) > 50:
                    review['text'] = text
                    break
            
            # Fallback text extraction
            if not review['text']:
                text_el = container.select_one('span[class*="review-text"], div[class*="review-text"]')
                if text_el:
                    review['text'] = text_el.get_text(strip=True)
                else:
                    # Last resort: get full container text
                    full_text = container.get_text(strip=True)
                    if len(full_text) > 50:
                        # Clean the text (remove author name if present)
                        if review['author_name'] and full_text.startswith(review['author_name']):
                            full_text = full_text[len(review['author_name']):].strip()
                        if len(full_text) > 50:
                            review['text'] = full_text[:500]  # Limit length
            
            # Review photos (from googleusercontent.com within review div)
            for img in container.select('img[src*="googleusercontent.com"]'):
                src = img.get('src', '')
                # Filter for review photos (typically contain AF1Qip)
                if src and 'AF1Qip' in src:
                    review['photos'].append(src)
            
            # Local guide badge
            container_text = container.get_text().lower()
            if 'local guide' in container_text:
                review['author_is_local_guide'] = True
            
            # Owner response detection
            if 'response from' in container_text or 'owner' in container_text and 'response' in container_text:
                review['has_owner_response'] = True
                
                # Try to extract owner response text
                response_section = container.select_one('div[class*="response"], div[class*="reply"]')
                if response_section:
                    review['owner_response_text'] = response_section.get_text(strip=True)
            
        except Exception:
            pass
        
        return review
