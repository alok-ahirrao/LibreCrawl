"""
Google Maps HTML Parsers
Extracts structured data from Google Maps search results.
"""
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import unquote, urlparse, parse_qs


class GoogleMapsParser:
    """
    Parses HTML content from Google Maps Search Results.
    """
    
    def parse_list_results(self, html_content: str) -> list:
        """
        Extract structured data from the sidebar list.
        
        Returns:
            List of dicts with rank, name, rating, reviews, place_id, etc.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        # Method 1: Parse from feed items
        items = soup.select('div[role="feed"] > div > div[jsaction]')
        
        rank_counter = 1
        seen_names = set()
        
        for item in items:
            try:
                # Find the main link
                link_anchor = item.select_one('a[href*="/maps/place/"]')
                if not link_anchor:
                    continue
                
                url = link_anchor.get('href', '')
                name = link_anchor.get('aria-label', '')
                
                if not name or name in seen_names:
                    continue
                
                seen_names.add(name)
                
                # Extract rating and reviews
                rating, review_count = self._extract_rating_reviews(item)
                
                # Extract Place ID from URL
                place_id = self._extract_place_id(url)
                
                # Extract category if available
                category = self._extract_category(item)
                
                # Extract address snippet
                address = self._extract_address(item)
                
                results.append({
                    'rank': rank_counter,
                    'name': name,
                    'url': url,
                    'rating': rating,
                    'reviews': review_count,
                    'place_id': place_id,
                    'category': category,
                    'address': address
                })
                
                rank_counter += 1
                
            except Exception as e:
                # Skip malformed items
                continue
        
        # Method 2: Fallback - parse from JSON-LD or embedded data
        if not results:
            results = self._parse_from_embedded_data(soup)
        
        return results
    
    def _extract_rating_reviews(self, item) -> tuple:
        """Extract rating and review count from an item."""
        rating = 0.0
        review_count = 0
        
        # Method 1: Try aria-label on rating span
        rating_span = item.select_one('span[role="img"]')
        if rating_span and rating_span.get('aria-label'):
            label = rating_span.get('aria-label')
            
            # Extract rating: "4.5 stars" or "4.5"
            rating_match = re.search(r'([0-9.]+)\s*(?:stars?)?', label, re.IGNORECASE)
            if rating_match:
                rating = float(rating_match.group(1))
            
            # Extract review count: "155 reviews" or "(155)"
            reviews_match = re.search(r'([0-9,]+)\s*reviews?', label, re.IGNORECASE)
            if reviews_match:
                review_count = int(reviews_match.group(1).replace(',', ''))
        
        # Method 2: Parse from full item text (handles "Name4.8(68)Category..." format)
        if rating == 0.0 or review_count == 0:
            full_text = item.get_text()
            
            # Extract rating: look for pattern like "4.8" or "4.9"
            if rating == 0.0:
                rating_match = re.search(r'(\d+\.\d+)', full_text)
                if rating_match:
                    try:
                        potential_rating = float(rating_match.group(1))
                        # Ratings are typically 1.0 to 5.0
                        if 1.0 <= potential_rating <= 5.0:
                            rating = potential_rating
                    except:
                        pass
            
            # Extract review count: look for pattern like "(68)" or "(155)"
            # Extract review count: look for pattern like "(68)" or "(155)"
            if review_count == 0:
                reviews_match = re.search(r'\((\d+)\)', full_text)
                if reviews_match:
                    review_count = int(reviews_match.group(1))
                else:
                    # Look for "68 reviews" pattern in text
                    reviews_text_match = re.search(r'(\d+)\s+reviews', full_text, re.IGNORECASE)
                    if reviews_text_match:
                        review_count = int(reviews_text_match.group(1))
                    
                    # Fallback: Look for number immediately following rating "4.8 68"
                    # Pattern: 4.8 followed by optional space/newline then digits
                    if rating > 0:
                        try:
                            # Sanitize text to remove newlines for better regex matching
                            clean_text = ' '.join(full_text.split())
                            loose_match = re.search(fr'{rating}\s*\(?(\d+)\)?', clean_text)
                            if loose_match:
                                review_count = int(loose_match.group(1))
                        except Exception as e:
                            print(f"[Parser] Regex warning: {e}")

        if review_count == 0:
             print(f"[Parser] Review extraction failed for '{full_text[:50]}'. HTML snippet:\n{item.prettify()[:1000]}")
        
        print(f"[Parser] Extracted rating={rating}, reviews={review_count}")
        return rating, review_count
    
    def _extract_place_id(self, url: str) -> str:
        """
        Extract Place ID from Google Maps URL.
        
        URLs can look like:
        - /maps/place/Business+Name/@lat,lng,z/data=!3m1!4b1!4m6!3m5!1s0x89c259...:0x...
        - Place ID is in the !1s part of the data parameter
        """
        if not url:
            return None
        
        try:
            # Method 1: Extract standard Place ID (ChIJ...) 
            # Often preceded by !19s in the data string
            # We look for the "ChIJ" pattern which is the standard Google Place ID prefix
            chij_match = re.search(r'(ChIJ[a-zA-Z0-9_-]+)', url)
            if chij_match:
                return chij_match.group(1)

            # Method 2: Extract from data parameter (legacy CID hex format)
            # Looking for pattern like !1s0x89c259a61c75684f:0x79bedc079c7a7c9a
            place_id_match = re.search(r'!1s(0x[a-f0-9]+:0x[a-f0-9]+)', url)
            if place_id_match:
                return place_id_match.group(1)
            
            # Method 3: Look in decoded URL
            decoded_url = unquote(url)
            
            # Try ChIJ again in decoded URL
            chij_match = re.search(r'(ChIJ[a-zA-Z0-9_-]+)', decoded_url)
            if chij_match:
                return chij_match.group(1)
            
            # Try legacy CID again in decoded URL
            place_id_match = re.search(r'!1s(0x[a-f0-9]+:0x[a-f0-9]+)', decoded_url)
            if place_id_match:
                return place_id_match.group(1)
            
            # Method 4: Extract from ftid parameter
            ftid_match = re.search(r'ftid=([^&]+)', decoded_url)
            if ftid_match:
                return ftid_match.group(1)
            
        except Exception:
            pass
        
        return None
    
    def _extract_category(self, item) -> str:
        """Extract business category from item using text analysis."""
        try:
            full_text = item.get_text()
            
            # Method 1: Look for dot separator after rating/reviews
            # Pattern: "4.8(155) · Dentist" or "4.8 (155) · Dentist"
            # Note: The dot is often U+00B7 (·)
            cat_match = re.search(r'\d+\.?\d*\s*\(\d+\)\s*·\s*([^·\n\r]+)', full_text)
            if cat_match:
                candidate = cat_match.group(1).strip()
                # Categories are usually short (e.g. "Dental clinic", "Pizza restaurant")
                if len(candidate) < 40 and not any(char.isdigit() for char in candidate):
                    return candidate

            # Method 2: Look for text immediately following "Start" rating pattern if no reviews
            # Pattern: "NICE DENTAL CLINIC 4.9Dental clinic"
            # We look for the rating number, then capture text until next number or separator
            rating_text_match = re.search(r'\d\.\d\s*([A-Za-z\s]+)(?:·|$)', full_text)
            if rating_text_match:
                 candidate = rating_text_match.group(1).strip()
                 if len(candidate) > 2 and len(candidate) < 40:
                     return candidate

            # Method 3: Fallback to existing class-based lookup but safer
            texts = item.select('div[class*="fontBodyMedium"]')
            for text_div in texts:
                text = text_div.get_text(strip=True)
                if text and len(text) < 40 and '$' not in text:
                    # Filter out common non-category text
                    lower_text = text.lower()
                    if not any(x in lower_text for x in ['open', 'closed', 'min', 'km', 'mile', 'am', 'pm']):
                        return text
                        
        except Exception as e:
            print(f"[Parser] Error extracting category: {e}")
        return None
    
    def _extract_address(self, item) -> str:
        """Extract address snippet from item."""
        try:
            # Look for text that resembles an address
            all_text = item.get_text(separator='|').split('|')
            
            for text in all_text:
                text = text.strip()
                if not text or len(text) < 10:
                    continue
                
                # Skip if it's a short category name or rating text
                if len(text) < 20 and not any(c.isdigit() for c in text):
                    continue
                
                # Check if it looks like an address:
                # - Contains plus code like "2RP2+MP"
                # - Contains numbers + text (like "123 Main" or state codes)
                # - Contains location indicators
                is_address = False
                
                # Plus code pattern (Indian addresses often use these)
                if re.search(r'[A-Z0-9]{4}\+[A-Z0-9]{2,}', text):
                    is_address = True
                
                # Has number at start (street number) or contains postal code
                if re.search(r'^\d+|,\s*\d{5,6}|\d{6}$', text):
                    is_address = True
                
                # Contains common address words (expanded for India)
                addr_words = ['road', 'rd', 'street', 'st', 'avenue', 'ave', 'lane', 
                             'nagar', 'colony', 'sector', 'opposite', 'near', 'behind',
                             'market', 'chowk', 'circle', 'maharashtra', 'delhi', 'mumbai',
                             'bangalore', 'hyderabad', 'chennai', 'kolkata', 'pune', 'jaipur']
                if any(word in text.lower() for word in addr_words):
                    is_address = True
                
                if is_address:
                    # Clean up the address - remove rating/review prefixes if any
                    cleaned = re.sub(r'^\d+\.\d+\s*(\(\d+\))?\s*·?\s*', '', text)
                    if len(cleaned) >= 10:
                        return cleaned
                        
        except Exception as e:
            print(f"[Parser] Address extraction error: {e}")
        return None
    
    def _parse_from_embedded_data(self, soup) -> list:
        """Fallback: parse from embedded JavaScript data."""
        results = []
        
        try:
            # Look for script tags with embedded data
            scripts = soup.find_all('script')
            for script in scripts:
                text = script.string or ''
                
                # Look for array patterns with place data
                # This is a heuristic approach for when the feed parsing fails
                if 'searchResults' in text or 'placeResults' in text:
                    # Try to extract JSON-like structures
                    # This is fragile and version-dependent
                    pass
        except:
            pass
        
        return results
    def parse_business_search(self, html_content: str, query: str = None) -> dict:
        """
        Parse business search results and return structured data with confidence.
        
        Args:
            html_content: Raw HTML from Google Maps search
            query: Original search query for confidence scoring
            
        Returns:
            Dict with confidence score and list of matched businesses
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        matches = []
        
        # Method 1: Try parsing from results feed FIRST (this is more reliable)
        items = soup.select('div[role="feed"] > div > div[jsaction]')
        
        if items:
            # We have a feed - parse individual items
            rank = 0
            seen_names = set()
            
            for item in items:
                try:
                    link_anchor = item.select_one('a[href*="/maps/place/"]')
                    if not link_anchor:
                        continue
                    
                    url = link_anchor.get('href', '')
                    aria_label = link_anchor.get('aria-label', '')
                    
                    # Debug: show full aria-label and sample of item text
                    item_text = item.get_text()[:150].replace('\n', ' ')
                    print(f"[Parser] aria_label FULL: '{aria_label}'")
                    print(f"[Parser] item_text sample: '{item_text}...'")
                    
                    
                    # Primary: Clean the aria-label (most reliable for actual displayed name)
                    name = self._clean_business_name(aria_label)
                    
                    # Fallback: Extract from URL
                    if not name:
                        name = self._extract_name_from_url(url)
                    
                    print(f"[Parser] Extracted name: '{name}'")
                    
                    if not name or name in seen_names:
                        continue
                    
                    seen_names.add(name)
                    rank += 1
                    
                    # Extract all data
                    rating, review_count = self._extract_rating_reviews(item)
                    place_id = self._extract_place_id(url)
                    category = self._extract_category(item)
                    address = self._extract_address(item)
                    lat, lng = self._extract_coordinates_from_url(url)
                    phone = self._extract_phone(item)
                    is_open = self._extract_open_status(item)
                    
                    matches.append({
                        'place_id': place_id,
                        'name': name,
                        'rating': rating,
                        'review_count': review_count,
                        'address': address,
                        'phone': phone,
                        'lat': lat,
                        'lng': lng,
                        'is_open': is_open,
                        'category': category
                    })
                    
                    # Limit to top 5 results
                    if rank >= 5:
                        break
                        
                except Exception as e:
                    continue
        
        # Method 2: If no feed items found, try single business page detection
        if not matches:
            h1_title = soup.select_one('h1')
            if h1_title:
                h1_text = h1_title.get_text(strip=True)
                # Only use H1 if it looks like a business name (not generic like "Results")
                if h1_text and len(h1_text) > 2 and h1_text.lower() not in ['results', 'search', 'maps', 'google maps']:
                    single_biz = self._parse_single_business_page(soup, h1_title)
                    if single_biz and single_biz.get('name'):
                        matches.append(single_biz)
        
        
        # Calculate confidence based on results
        if len(matches) == 0:
            confidence = 0.0
        elif len(matches) == 1:
            confidence = self._calculate_confidence(query, matches[0]['name'], single_match=True)
        else:
            # Multiple matches - lower confidence
            best_match_conf = max(
                self._calculate_confidence(query, m['name'], single_match=False) 
                for m in matches
            )
            confidence = best_match_conf * 0.8  # Reduce confidence for ambiguity
        
        return {
            'confidence': confidence,
            'matches': matches
        }
    
    def _parse_single_business_page(self, soup, h1_element) -> dict:
        """Parse data from a single business detail page."""
        result = {
            'place_id': None,
            'name': h1_element.get_text(strip=True) if h1_element else None,
            'rating': 0.0,
            'review_count': 0,
            'address': None,
            'phone': None,
            'lat': None,
            'lng': None,
            'is_open': None,
            'category': None
        }
        
        try:
            # Rating
            rating_div = soup.select_one('div[role="img"][aria-label*="star"]')
            if rating_div:
                label = rating_div.get('aria-label', '')
                rating_match = re.search(r'([0-9.]+)\s*star', label)
                if rating_match:
                    result['rating'] = float(rating_match.group(1))
            
            # Reviews count
            reviews_btn = soup.select_one('button[aria-label*="review"]')
            if reviews_btn:
                label = reviews_btn.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*review', label, re.IGNORECASE)
                if count_match:
                    result['review_count'] = int(count_match.group(1).replace(',', ''))
            
            # Address
            addr_btn = soup.select_one('button[data-item-id="address"]')
            if addr_btn:
                result['address'] = addr_btn.get('aria-label', '').replace('Address: ', '')
            
            # Phone
            phone_btn = soup.select_one('button[data-item-id*="phone"]')
            if phone_btn:
                result['phone'] = phone_btn.get('aria-label', '').replace('Phone: ', '')
            
            # Category
            cat_btn = soup.select_one('button[jsaction*="category"]')
            if cat_btn:
                result['category'] = cat_btn.get_text(strip=True)
            
            # Open status - search for text content instead of :has-text()
            # Look for open/closed status in aria-labels or text content
            all_text = soup.get_text().lower()
            if 'open now' in all_text or 'opens at' in all_text:
                result['is_open'] = True
            elif 'closed' in all_text and 'permanently closed' not in all_text:
                result['is_open'] = False
            
            # Extract coordinates from URL in the page
            canonical = soup.select_one('link[rel="canonical"]')
            if canonical:
                href = canonical.get('href', '')
                lat, lng = self._extract_coordinates_from_url(href)
                result['lat'] = lat
                result['lng'] = lng
            
            # Try to get place_id from page URL
            meta_url = soup.select_one('meta[property="og:url"]')
            if meta_url:
                result['place_id'] = self._extract_place_id(meta_url.get('content', ''))
                
        except Exception as e:
            print(f"Error parsing single business page: {e}")
        
        return result
    
    def _extract_coordinates_from_url(self, url: str) -> tuple:
        """Extract lat/lng from Google Maps URL, prioritizing precise pin location."""
        try:
            # Method 1: Precise Pin Location (!3d...!4d...)
            # URL often contains: data=!3d40.748817!4d-73.985428
            lat_match = re.search(r'!3d(-?[0-9.]+)', url)
            lng_match = re.search(r'!4d(-?[0-9.]+)', url)
            
            if lat_match and lng_match:
                return float(lat_match.group(1)), float(lng_match.group(1))

            # Method 2: Center of Viewport (@lat,lng,z)
            # Pattern: @40.7580,-73.9855,15z - less accurate but good fallback
            coord_match = re.search(r'@(-?[0-9.]+),(-?[0-9.]+)', url)
            if coord_match:
                return float(coord_match.group(1)), float(coord_match.group(2))
                
        except Exception as e:
            print(f"[Parser] Error extracting coordinates: {e}")
        return None, None
    
    def _extract_name_from_url(self, url: str) -> str:
        """Extract business name from Google Maps URL path."""
        try:
            # URL format: /maps/place/Business+Name/@lat,lng or /maps/place/Business+Name/data=...
            # Extract the part between /place/ and the next /
            match = re.search(r'/maps/place/([^/@]+)', url)
            if match:
                name_encoded = match.group(1)
                # URL decode: + becomes space, %XX becomes character
                from urllib.parse import unquote_plus
                name = unquote_plus(name_encoded)
                # Clean up any remaining encoding artifacts
                name = name.replace('+', ' ')
                return name.strip()
        except:
            pass
        return None
    
    def _clean_business_name(self, aria_label: str) -> str:
        """Clean business name from aria-label that may contain concatenated info."""
        if not aria_label:
            return None
        
        # Debug: show what we're cleaning
        print(f"[Parser] Cleaning aria_label: '{aria_label[:80]}...'")
        
        # aria-label often looks like: "Business Name4.5Dental clinic·Address..."
        # or "NICE DENTAL CLINIC4.9(13)Dental clinic·Hospital..."
        # We want just the business name part
        
        # Method 1: Split on rating pattern - looks for number.number pattern
        # Pattern: letters followed immediately by a digit (like "Name4.5")
        match = re.search(r'^(.+?)(\d+\.\d+)', aria_label)
        if match:
            name = match.group(1).strip()
            if len(name) >= 2:
                print(f"[Parser] Cleaned via Method 1: '{name}'")
                return name
        
        # Method 2: Split on just any digit followed by decimal (rating like 4.9)
        parts = re.split(r'\d+\.\d+', aria_label, maxsplit=1)
        if parts and parts[0]:
            name = parts[0].strip()
            if len(name) >= 2:
                print(f"[Parser] Cleaned via Method 2: '{name}'")
                return name
        
        # Method 3: Split on the middle dot separator (·)
        parts = aria_label.split('·')
        if parts:
            first_part = parts[0].strip()
            # Remove trailing rating/review patterns like "4.5" or "4.9(13)"
            cleaned = re.sub(r'\d+\.?\d*\s*(\(\d+\))?$', '', first_part).strip()
            if len(cleaned) >= 2:
                print(f"[Parser] Cleaned via Method 3: '{cleaned}'")
                return cleaned
        
        # Fallback: return first 50 chars if nothing else works
        fallback = aria_label[:50].strip() if len(aria_label) > 50 else aria_label.strip()
        print(f"[Parser] Fallback name: '{fallback}'")
        return fallback
    
    def _extract_phone(self, item) -> str:
        """Extract phone number from item."""
        try:
            # Phone often appears in aria-label or text
            phone_pattern = r'\+?[0-9]{1,3}[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}'
            text = item.get_text()
            match = re.search(phone_pattern, text)
            if match:
                return match.group(0)
        except:
            pass
        return None
    
    def _extract_open_status(self, item) -> bool:
        """Extract open/closed status from item."""
        try:
            text = item.get_text().lower()
            if 'open' in text and 'closed' not in text:
                return True
            elif 'closed' in text:
                return False
        except:
            pass
        return None
    
    def _calculate_confidence(self, query: str, name: str, single_match: bool = False) -> float:
        """Calculate confidence score for a business match."""
        if not query or not name:
            return 0.5
        
        query_lower = query.lower().strip()
        name_lower = name.lower().strip()
        
        # Exact match
        if query_lower == name_lower:
            return 0.98 if single_match else 0.90
        
        # Query is a substring of name
        if query_lower in name_lower:
            # Calculate how much of the name is covered
            coverage = len(query_lower) / len(name_lower)
            base_conf = 0.75 + (coverage * 0.20)
            return min(base_conf + 0.05 if single_match else base_conf, 0.95)
        
        # Name contains query words
        query_words = set(query_lower.split())
        name_words = set(name_lower.split())
        common_words = query_words.intersection(name_words)
        
        if common_words:
            word_match_ratio = len(common_words) / len(query_words)
            base_conf = 0.50 + (word_match_ratio * 0.35)
            return base_conf + 0.05 if single_match else base_conf
        
        # Low confidence fallback
        return 0.40 if single_match else 0.30
    
    def parse_place_details(self, html_content: str) -> dict:
        """
        Parse detailed information from a place page.
        
        Returns:
            dict with full place details including categories, attributes, etc.
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        details = {
            'name': None,
            'rating': None,
            'review_count': None,
            'primary_category': None,
            'additional_categories': [],
            'address': None,
            'phone': None,
            'website': None,
            'hours': None,
            'attributes': [],
            'photo_count': None
        }
        
        try:
            # Name
            title = soup.select_one('h1')
            if title:
                details['name'] = title.get_text(strip=True)
            
            # Rating and reviews
            rating_div = soup.select_one('div[role="img"][aria-label*="stars"]')
            if rating_div:
                label = rating_div.get('aria-label', '')
                rating_match = re.search(r'([0-9.]+)\s*stars?', label)
                if rating_match:
                    details['rating'] = float(rating_match.group(1))
            
            reviews_span = soup.select_one('button[aria-label*="reviews"]')
            if reviews_span:
                label = reviews_span.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*reviews?', label)
                if count_match:
                    details['review_count'] = int(count_match.group(1).replace(',', ''))
            
            # Categories
            category_button = soup.select_one('button[jsaction*="category"]')
            if category_button:
                details['primary_category'] = category_button.get_text(strip=True)
            
            # Additional categories in "About" section
            about_section = soup.select('div[aria-label="About"] div')
            for div in about_section:
                text = div.get_text(strip=True)
                if text and text != details['primary_category']:
                    # Could be additional category
                    details['additional_categories'].append(text)
            
            # Address
            address_link = soup.select_one('button[data-item-id="address"]')
            if address_link:
                details['address'] = address_link.get('aria-label', '').replace('Address: ', '')
            
            # Phone
            phone_link = soup.select_one('button[data-item-id*="phone"]')
            if phone_link:
                details['phone'] = phone_link.get('aria-label', '').replace('Phone: ', '')
            
            # Website
            website_link = soup.select_one('a[data-item-id="authority"]')
            if website_link:
                details['website'] = website_link.get('href', '')
            
            # Photo count
            photos_tab = soup.select_one('button[aria-label*="photos"]')
            if photos_tab:
                label = photos_tab.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)', label)
                if count_match:
                    details['photo_count'] = int(count_match.group(1).replace(',', ''))
            
            # Attributes (service options, amenities, etc.)
            attribute_sections = soup.select('div[class*="attributes"] div')
            for attr_div in attribute_sections:
                attr_text = attr_div.get_text(strip=True)
                if attr_text:
                    details['attributes'].append(attr_text)
            
        except Exception as e:
            print(f"Error parsing place details: {e}")
        
        return details


class LocalPackParser:
    """
    Parser specifically for Google Search Local Pack (3-pack).
    """
    
    def parse_local_pack(self, html_content: str) -> list:
        """
        Extract businesses from the Google Search local pack.
        
        Args:
            html_content: HTML from Google Search results page
            
        Returns:
            List of business dicts
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        results = []
        
        # Local pack is usually in a div with class containing 'local'
        local_pack = soup.select('div[data-attrid="kc:/local:local_listing"] a')
        
        rank = 1
        for item in local_pack[:3]:  # Local pack has max 3
            try:
                name = item.get('aria-label', item.get_text(strip=True))
                url = item.get('href', '')
                
                # Extract basic info
                results.append({
                    'rank': rank,
                    'name': name,
                    'url': url,
                    'place_id': self._extract_ludocid(url)
                })
                
                rank += 1
                
            except Exception:
                continue
        
        return results
    
    def _extract_ludocid(self, url: str) -> str:
        """Extract ludocid (location document ID) from Google URL."""
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            return params.get('ludocid', [None])[0]
        except:
            return None
