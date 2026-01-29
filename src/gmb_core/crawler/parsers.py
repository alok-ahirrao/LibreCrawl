"""
Google Maps HTML Parsers
Extracts structured data from Google Maps search results.
"""
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import unquote, urlparse, parse_qs
from ..logger import log


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
        """
        Extract rating and review count from a Google Maps listing item.
        Uses multiple extraction strategies for robustness.
        """
        rating = 0.0
        review_count = 0
        full_text = item.get_text() if item else ""
        
        # Method 1: Try aria-label on span[role="img"] (most reliable)
        rating_span = item.select_one('span[role="img"]')
        if rating_span and rating_span.get('aria-label'):
            label = rating_span.get('aria-label')
            log.debug(f"[Parser] Found aria-label: '{label}'")
            
            # Extract rating: "4.5 stars" or "4.5"
            rating_match = re.search(r'([0-9.]+)\s*(?:stars?)?', label, re.IGNORECASE)
            if rating_match:
                try:
                    rating = float(rating_match.group(1))
                except ValueError:
                    pass
            
            # Extract review count: "155 reviews", "(155)", "1,234 reviews", "1.2K reviews"
            # Pattern 1: Comma-separated number + "reviews"
            reviews_match = re.search(r'([0-9,]+)\s*reviews?', label, re.IGNORECASE)
            if reviews_match:
                review_count = int(reviews_match.group(1).replace(',', ''))
            else:
                # Pattern 2: K/M suffix (e.g., "1.2K reviews" or "1K")
                km_match = re.search(r'([0-9.]+)\s*([KkMm])\s*(?:reviews?)?', label)
                if km_match:
                    num = float(km_match.group(1))
                    suffix = km_match.group(2).upper()
                    if suffix == 'K':
                        review_count = int(num * 1000)
                    elif suffix == 'M':
                        review_count = int(num * 1000000)
        
        # Method 2: Try alternative span selectors 
        if review_count == 0:
            # Look for spans with review count in various formats
            all_spans = item.select('span')
            for span in all_spans:
                span_text = span.get_text(strip=True)
                # Check for parentheses format: "(155)" or "(1,234)"
                paren_match = re.search(r'^\(([0-9,]+)\)$', span_text)
                if paren_match:
                    review_count = int(paren_match.group(1).replace(',', ''))
                    log.debug(f"[Parser] Found review count from span (paren): {review_count}")
                    break
                # Check for K/M format in span: "1.2K" or "155"
                km_span_match = re.search(r'^([0-9.]+)\s*([KkMm])?$', span_text)
                if km_span_match and len(span_text) < 10:
                    num = float(km_span_match.group(1))
                    suffix = km_span_match.group(2)
                    if suffix:
                        suffix = suffix.upper()
                        if suffix == 'K':
                            review_count = int(num * 1000)
                        elif suffix == 'M':
                            review_count = int(num * 1000000)
                    elif num > 0 and num == int(num):
                        # Could be a plain number review count - validate context
                        # Check if previous sibling is a rating
                        prev = span.find_previous_sibling()
                        if prev and re.search(r'\d\.\d', prev.get_text(strip=True)):
                            review_count = int(num)
                    if review_count > 0:
                        log.debug(f"[Parser] Found review count from span (km): {review_count}")
                        break
        
        # Method 3: Parse from full item text (handles "Name4.8(68)Category..." format)
        if rating == 0.0 or review_count == 0:
            # Extract rating if still not found
            if rating == 0.0:
                rating_match = re.search(r'(\d+\.\d+)', full_text)
                if rating_match:
                    try:
                        potential_rating = float(rating_match.group(1))
                        if 1.0 <= potential_rating <= 5.0:
                            rating = potential_rating
                    except ValueError:
                        pass
            
            # Extract review count from text
            if review_count == 0:
                # Clean text - normalize whitespace
                clean_text = ' '.join(full_text.split())
                
                # Pattern 1: Parentheses format "(68)" or "(1,234)"
                paren_match = re.search(r'\(([0-9,]+)\)', clean_text)
                if paren_match:
                    review_count = int(paren_match.group(1).replace(',', ''))
                    log.debug(f"[Parser] Found review count from text (paren): {review_count}")
                
                # Pattern 2: "68 reviews" or "1,234 reviews"
                if review_count == 0:
                    reviews_text_match = re.search(r'([0-9,]+)\s+reviews?', clean_text, re.IGNORECASE)
                    if reviews_text_match:
                        review_count = int(reviews_text_match.group(1).replace(',', ''))
                        log.debug(f"[Parser] Found review count from text (reviews): {review_count}")
                
                # Pattern 3: K/M suffix "1.2K reviews" or just "1.2K" after rating
                if review_count == 0:
                    km_text_match = re.search(r'([0-9.]+)\s*([KkMm])\s*(?:reviews?)?', clean_text)
                    if km_text_match:
                        num = float(km_text_match.group(1))
                        suffix = km_text_match.group(2).upper()
                        if suffix == 'K':
                            review_count = int(num * 1000)
                        elif suffix == 'M':
                            review_count = int(num * 1000000)
                        log.debug(f"[Parser] Found review count from text (K/M): {review_count}")
                
                # Pattern 4: Number immediately after rating "4.8 68" or "4.868"
                if review_count == 0 and rating > 0:
                    try:
                        # Look for rating followed by a number
                        rating_str = str(rating)
                        # Pattern like "4.8 68" or "4.8(68)"
                        after_rating_match = re.search(
                            rf'{re.escape(rating_str)}\s*\(?([0-9,]+)\)?',
                            clean_text
                        )
                        if after_rating_match:
                            potential_count = int(after_rating_match.group(1).replace(',', ''))
                            # Sanity check: review count should be reasonable
                            if potential_count > 0 and potential_count < 1000000:
                                review_count = potential_count
                                log.debug(f"[Parser] Found review count after rating: {review_count}")
                    except Exception as e:
                        log.debug(f"[Parser] Regex warning: {e}")
        
        # Method 4: Try extracting from aria-label on buttons or links
        if review_count == 0:
            review_buttons = item.select('button[aria-label*="review"], a[aria-label*="review"]')
            for btn in review_buttons:
                label = btn.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*reviews?', label, re.IGNORECASE)
                if count_match:
                    review_count = int(count_match.group(1).replace(',', ''))
                    log.debug(f"[Parser] Found review count from button aria-label: {review_count}")
                    break
        
        # Method 5: Look for review count in sibling/child divs with specific patterns
        if review_count == 0:
            # Pattern: Find divs containing just a number near rating elements
            for div in item.select('div, span'):
                div_text = div.get_text(strip=True)
                # Match standalone numbers like "68" or "421" or "(68)"
                standalone_match = re.match(r'^\(?(\d{1,6})\)?$', div_text)
                if standalone_match:
                    potential = int(standalone_match.group(1))
                    # Validate: should be reasonable count and not a year or other number
                    if 1 <= potential <= 100000 and potential != int(rating * 10):
                        # Check if there's a rating nearby
                        parent = div.parent
                        if parent:
                            parent_text = parent.get_text()
                            if re.search(r'\d\.\d', parent_text):
                                review_count = potential
                                log.debug(f"[Parser] Found review count from standalone number: {review_count}")
                                break
        
        # Method 6: Search all aria-labels in the item for review patterns
        if review_count == 0:
            for elem in item.select('[aria-label]'):
                label = elem.get('aria-label', '')
                # Look for patterns like "4.8 stars 68 reviews" or just "68 reviews"
                count_match = re.search(r'(\d{1,6})\s*(?:Reviews?|ratings?)', label, re.IGNORECASE)
                if count_match:
                    review_count = int(count_match.group(1))
                    log.debug(f"[Parser] Found review count from any aria-label: {review_count}")
                    break
        
        # Method 7: Text pattern - look for number followed by closing paren anywhere
        if review_count == 0:
            # Sometimes the format is like "Rating4.8(68)Category" without spaces
            compact_match = re.search(r'(\d\.\d)\((\d{1,6})\)', full_text)
            if compact_match:
                if rating == 0.0:
                    rating = float(compact_match.group(1))
                review_count = int(compact_match.group(2))
                log.debug(f"[Parser] Found review count from compact format: {review_count}")
        
        # Debug logging for failed extractions
        if review_count == 0:
            log.debug(f"[Parser] ⚠️ Review extraction FAILED for text: '{full_text[:100]}...'")
            # Log the HTML structure for debugging
            try:
                html_snippet = item.prettify()[:500]
                log.debug(f"[Parser] HTML snippet:\n{html_snippet}")
            except:
                pass
        else:
            log.debug(f"[Parser] ✅ Extracted rating={rating}, reviews={review_count}")
        
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
            # Method 1: Extract standard Place ID (ChIJ...) - PRIMARY
            # We strictly prioritize this over the hex CID format
            # Look for ChIJ in the raw URL first
            chij_match = re.search(r'(ChIJ[a-zA-Z0-9_-]+)', url)
            if chij_match:
                return chij_match.group(1)
            
            # Method 2: Look for ChIJ in decoded URL (sometimes encoded)
            decoded_url = unquote(url)
            chij_match_decoded = re.search(r'(ChIJ[a-zA-Z0-9_-]+)', decoded_url)
            if chij_match_decoded:
                return chij_match_decoded.group(1)

            # Method 3: Extract from data parameter (legacy CID hex format) - FALLBACK ONLY
            # Only use this if no ChIJ ID was found
            # Looking for pattern like !1s0x89c259a61c75684f:0x79bedc079c7a7c9a
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
            log.debug(f"[Parser] Error extracting category: {e}")
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
            log.debug(f"[Parser] Address extraction error: {e}")
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
                    log.debug(f"[Parser] aria_label FULL: '{aria_label}'")
                    log.debug(f"[Parser] item_text sample: '{item_text}...'")
                    
                    
                    # Primary: Clean the aria-label (most reliable for actual displayed name)
                    name = self._clean_business_name(aria_label)
                    
                    # Fallback: Extract from URL
                    if not name:
                        name = self._extract_name_from_url(url)
                    
                    log.debug(f"[Parser] Extracted name: '{name}'")
                    
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
            log.error(f"Error parsing single business page: {e}")
        
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
            log.debug(f"[Parser] Error extracting coordinates: {e}")
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
        log.debug(f"[Parser] Cleaning aria_label: '{aria_label[:80]}...'")
        
        # aria-label often looks like: "Business Name4.5Dental clinic·Address..."
        # or "NICE DENTAL CLINIC4.9(13)Dental clinic·Hospital..."
        # We want just the business name part
        
        # Method 1: Split on rating pattern - looks for number.number pattern
        # Pattern: letters followed immediately by a digit (like "Name4.5")
        match = re.search(r'^(.+?)(\d+\.\d+)', aria_label)
        if match:
            name = match.group(1).strip()
            if len(name) >= 2:
                log.debug(f"[Parser] Cleaned via Method 1: '{name}'")
                return name
        
        # Method 2: Split on just any digit followed by decimal (rating like 4.9)
        parts = re.split(r'\d+\.\d+', aria_label, maxsplit=1)
        if parts and parts[0]:
            name = parts[0].strip()
            if len(name) >= 2:
                log.debug(f"[Parser] Cleaned via Method 2: '{name}'")
                return name
        
        # Method 3: Split on the middle dot separator (·)
        parts = aria_label.split('·')
        if parts:
            first_part = parts[0].strip()
            # Remove trailing rating/review patterns like "4.5" or "4.9(13)"
            cleaned = re.sub(r'\d+\.?\d*\s*(\(\d+\))?$', '', first_part).strip()
            if len(cleaned) >= 2:
                log.debug(f"[Parser] Cleaned via Method 3: '{cleaned}'")
                return cleaned
        
        # Fallback: return first 50 chars if nothing else works
        fallback = aria_label[:50].strip() if len(aria_label) > 50 else aria_label.strip()
        log.debug(f"[Parser] Fallback name: '{fallback}'")
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
            # 1️⃣ Business Identity
            'name': None,
            'primary_category': None,
            'additional_categories': [],
            'business_status': None,  # 'OPEN' | 'CLOSED' | 'TEMPORARILY_CLOSED' | 'PERMANENTLY_CLOSED'
            'claimed_status': None,   # True = claimed, False = unclaimed, None = unknown
            
            # 2️⃣ NAP (Location & Contact)
            'address': None,
            'address_components': {
                'street': None,
                'city': None,
                'state': None,
                'postal_code': None,
                'country': None
            },
            'plus_code': None,
            'latitude': None,
            'longitude': None,
            'phone': None,
            'website': None,
            
            # 3️⃣ Ratings & Reviews
            'rating': None,
            'review_count': None,
            'review_details': {},  # breakdown, summaries, recent_reviews with enhanced fields
            
            # 4️⃣ Business Hours
            'hours': None,  # Will include regular_hours, special_hours, last_confirmed
            
            # 5️⃣ Attributes
            'attributes': [],
            
            # 6️⃣ Owner Activity
            'post_count': None,
            'last_post_date': None,
            'owner_posts': [],  # Enhanced with post_id, media_url
            
            # 7️⃣ Popular Times
            'popular_times': None,  # Structured by day_of_week
            
            # 8️⃣ Media
            'photo_count': None,
            'video_count': None,
            'photo_urls': [],
            'photos': [],  # Enhanced with photo_id, category, uploaded_by, upload_date
            
            # 9️⃣ Engagement Actions
            'engagement_buttons': {
                'directions': False,
                'call': False,
                'website': False,
                'booking': False,
                'menu': False,
                'order_online': False
            },
            
            # Additional: Competitors
            'competitors': [],
            'service_area': None,
            'qa_count': 0,
            'description': None
        }
        
        try:
            # Name - try multiple selectors for robustness
            name_selectors = [
                'h1',
                'h1.DUwDvf',
                'h1.fontHeadlineLarge',
                'span.DUwDvf',
                'div[role="main"] h1',
                'div.lMbq3e h1',
                'div[data-attrid="title"] span',
            ]
            for selector in name_selectors:
                title = soup.select_one(selector)
                if title:
                    name_text = title.get_text(strip=True)
                    if name_text and len(name_text) > 1:
                        details['name'] = name_text
                        log.debug(f"[Parser] Found name '{name_text}' using selector: {selector}")
                        break
            
            # Fallback: extract from title tag
            if not details['name']:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text(strip=True)
                    # Title format is usually "Business Name - Google Maps"
                    if ' - Google Maps' in title_text:
                        details['name'] = title_text.replace(' - Google Maps', '').strip()
                        log.debug(f"[Parser] Found name from title tag: {details['name']}")
                    elif title_text and 'Google Maps' not in title_text:
                        details['name'] = title_text.strip()
            
            # ========== RATING EXTRACTION ==========
            # Method 1: div with stars in aria-label (handles "star" and "stars")
            rating_div = soup.select_one('div[role="img"][aria-label*="star"]')
            if rating_div:
                label = rating_div.get('aria-label', '')
                rating_match = re.search(r'([0-9.]+)\s*stars?', label, re.IGNORECASE)
                if rating_match:
                    details['rating'] = float(rating_match.group(1))
                    log.debug(f"[Parser] Found rating from aria-label: {details['rating']}")
            
            # Method 2: span[role="img"] with aria-label containing stars
            if not details['rating']:
                for el in soup.select('span[role="img"][aria-label*="star"]'):
                    label = el.get('aria-label', '')
                    rating_match = re.search(r'([0-9.]+)\s*stars?', label, re.IGNORECASE)
                    if rating_match:
                        details['rating'] = float(rating_match.group(1))
                        log.debug(f"[Parser] Found rating from span[role=img]: {details['rating']}")
                        break
            
            # Method 3: span with class ceNzKf or similar rating display classes
            if not details['rating']:
                for selector in ['span.ceNzKf', 'span.fontDisplayLarge', 'span.F7nice span:first-child', 
                                 'span.fontBodyMedium', 'div.F7nice span', 'span.e4rVHe']:
                    rating_span = soup.select_one(selector)
                    if rating_span:
                        text = rating_span.get_text(strip=True)
                        try:
                            val = float(text)
                            if 1.0 <= val <= 5.0:
                                details['rating'] = val
                                log.debug(f"[Parser] Found rating via {selector}: {details['rating']}")
                                break
                        except ValueError:
                            continue
            
            # Method 4: Look for any element with aria-label like "4.5 out of 5 stars"
            if not details['rating']:
                for el in soup.select('[aria-label]'):
                    label = el.get('aria-label', '')
                    # Matches "4.5 out of 5" or "4.5 stars" or "4.5 star rating"
                    rating_match = re.search(r'(\d\.\d)\s*(?:out of 5|stars?|star rating)', label, re.IGNORECASE)
                    if rating_match:
                        details['rating'] = float(rating_match.group(1))
                        log.debug(f"[Parser] Found rating from aria-label pattern: {details['rating']}")
                        break
            
            # Method 5: Search for pattern X.X followed by review count in parentheses
            if not details['rating']:
                header_area = soup.select_one('div[role="main"]') or soup
                header_text = header_area.get_text()[:800]  # First 800 chars
                # Look for patterns like "4.5(123)" or "4.5 (123)" or "4.5 123 reviews"
                rating_patt = re.search(r'(\d\.\d)\s*[\(\s]*\d{1,6}[\)\s]*(?:reviews?)?', header_text, re.IGNORECASE)
                if rating_patt:
                    try:
                        details['rating'] = float(rating_patt.group(1))
                        log.debug(f"[Parser] Found rating from header text pattern: {details['rating']}")
                    except:
                        pass
            
            # Method 6: Extract from JSON-LD structured data if present
            if not details['rating']:
                for script in soup.select('script[type="application/ld+json"]'):
                    try:
                        import json
                        data = json.loads(script.string or '{}')
                        if isinstance(data, dict):
                            # Check for aggregateRating
                            agg_rating = data.get('aggregateRating', {})
                            if agg_rating.get('ratingValue'):
                                details['rating'] = float(agg_rating['ratingValue'])
                                log.debug(f"[Parser] Found rating from JSON-LD: {details['rating']}")
                                break
                    except:
                        continue
            
            # Debug: Log if rating extraction failed
            if not details['rating']:
                log.debug(f"[Parser] ⚠️ Rating extraction FAILED - no patterns matched")
            
            # ========== DEBUG: CAPTURE HTML SAMPLES ==========
            # Save header area HTML to debug file for pattern analysis
            try:
                header_area = soup.select_one('div[role="main"]')
                if header_area:
                    header_text = header_area.get_text()[:1500]
                    log.debug(f"[Parser] DEBUG - Header text sample (first 500 chars): {header_text[:500]}")
                    
                    # Look for any patterns like "(XXX)" in the header
                    all_parens = re.findall(r'\((\d+)\)', header_text)
                    log.debug(f"[Parser] DEBUG - All numbers in parentheses found: {all_parens}")
                    
                    # Save to file for deep analysis
                    with open('debug_html_sample.txt', 'w', encoding='utf-8') as f:
                        f.write(f"=== Header Text ===\n{header_text}\n\n")
                        f.write(f"=== All parenthesis numbers ===\n{all_parens}\n\n")
                        f.write(f"=== Rating found ===\n{details['rating']}\n")
            except Exception as e:
                log.debug(f"[Parser] DEBUG capture error: {e}")
            
            # ========== REVIEW COUNT EXTRACTION ==========
            # Method 1: Button with aria-label containing "reviews"
            reviews_btn = soup.select_one('button[aria-label*="review"]')
            if reviews_btn:
                label = reviews_btn.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*reviews?', label, re.IGNORECASE)
                if count_match:
                    details['review_count'] = int(count_match.group(1).replace(',', ''))
                    log.debug(f"[Parser] Found reviews from button aria-label: {details['review_count']}")
            
            # Method 2: Any element with aria-label matching "X reviews"
            if not details['review_count']:
                for el in soup.select('[aria-label]'):
                    label = el.get('aria-label', '')
                    count_match = re.search(r'([0-9,]+)\s*reviews?', label, re.IGNORECASE)
                    if count_match:
                        details['review_count'] = int(count_match.group(1).replace(',', ''))
                        log.debug(f"[Parser] Found reviews from any aria-label: {details['review_count']}")
                        break
            
            # Method 3: Link with reviews in href or jsaction
            if not details['review_count']:
                reviews_link = soup.select_one('a[href*="reviews"], button[jsaction*="reviews"], a[data-item-id*="review"]')
                if reviews_link:
                    link_text = reviews_link.get_text(strip=True)
                    count_match = re.search(r'([0-9,]+)', link_text)
                    if count_match:
                        details['review_count'] = int(count_match.group(1).replace(',', ''))
                        log.debug(f"[Parser] Found reviews from link text: {details['review_count']}")
            
            # Method 4: F7nice span pattern (common Google Maps class)
            if not details['review_count']:
                review_elements = soup.select('span.F7nice span, span.F7nice, div.F7nice span')
                for el in review_elements:
                    text = el.get_text(strip=True)
                    # Check for parenthesis format: "(155)"
                    paren_match = re.search(r'\((\d{1,6})\)', text)
                    if paren_match:
                        details['review_count'] = int(paren_match.group(1))
                        log.debug(f"[Parser] Found reviews from F7nice paren: {details['review_count']}")
                        break
                    # Check for plain number
                    if text.replace(',', '').isdigit():
                        details['review_count'] = int(text.replace(',', ''))
                        log.debug(f"[Parser] Found reviews from F7nice plain: {details['review_count']}")
                        break
            
            # Method 5: Extract from JSON-LD structured data
            if not details['review_count']:
                for script in soup.select('script[type="application/ld+json"]'):
                    try:
                        import json
                        data = json.loads(script.string or '{}')
                        if isinstance(data, dict):
                            agg_rating = data.get('aggregateRating', {})
                            if agg_rating.get('reviewCount'):
                                details['review_count'] = int(agg_rating['reviewCount'])
                                log.debug(f"[Parser] Found reviews from JSON-LD: {details['review_count']}")
                                break
                            elif agg_rating.get('ratingCount'):
                                details['review_count'] = int(agg_rating['ratingCount'])
                                log.debug(f"[Parser] Found reviews from JSON-LD ratingCount: {details['review_count']}")
                                break
                    except:
                        continue
            
            # Method 6: Search all text for review patterns
            if not details['review_count']:
                full_text = soup.get_text()[:1500]
                # Look for "(123)" pattern near rating
                paren_pattern = re.findall(r'\((\d{1,6})\)', full_text)
                for match in paren_pattern:
                    count = int(match)
                    if count > 0 and count < 100000:  # Reasonable review count
                        details['review_count'] = count
                        log.debug(f"[Parser] Found reviews from text pattern: {details['review_count']}")
                        break
                
                # Also try "X reviews" pattern in full text
                if not details['review_count']:
                    rev_pattern = re.search(r'(\d{1,6})\s*reviews?', full_text, re.IGNORECASE)
                    if rev_pattern:
                        details['review_count'] = int(rev_pattern.group(1))
                        log.debug(f"[Parser] Found reviews from 'X reviews' pattern: {details['review_count']}")
            
            # Method 7: Look for rating followed by review count in parentheses (common pattern: "4.9(168)")
            if not details['review_count'] and details['rating']:
                full_text = soup.get_text()[:3000]
                # Pattern: rating like 4.9 followed by number in parentheses or just number
                rating_str = str(details['rating'])
                # Look for patterns like "4.9(168)" or "4.9 (168)" or "4.9 · 168 reviews"
                patterns = [
                    rf'{rating_str}\s*\((\d{{1,6}})\)',  # 4.9(168)
                    rf'{rating_str}\s*·\s*\((\d{{1,6}})\)',  # 4.9 · (168)
                    rf'{rating_str}\s*\(\s*(\d{{1,6}})\s*\)',  # 4.9 ( 168 )
                    r'(\d{1,6})\s*(?:Google\s+)?reviews?',  # 168 Google reviews
                ]
                for pattern in patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        count = int(match.group(1))
                        # Validate - shouldn't be a year
                        if 1 <= count <= 50000:
                            details['review_count'] = count
                            log.debug(f"[Parser] Found reviews from rating-adjacent pattern: {details['review_count']}")
                            break
            
            # Method 8: Find all numbers in parentheses and pick first reasonable one near h1
            if not details['review_count']:
                # Find H1 area specifically
                h1 = soup.select_one('h1')
                if h1:
                    # Get parent and siblings
                    parent = h1.parent
                    if parent:
                        parent_text = parent.get_text()[:500]
                        paren_matches = re.findall(r'\((\d{1,5})\)', parent_text)
                        for match in paren_matches:
                            count = int(match)
                            if 1 <= count <= 50000:  # Reasonable review count
                                details['review_count'] = count
                                log.debug(f"[Parser] Found reviews from H1 parent area: {details['review_count']}")
                                break
            
            # Debug: Log if review extraction failed
            if not details['review_count']:
                log.debug(f"[Parser] ⚠️ Review count extraction FAILED - no patterns matched")
            
            log.debug(f"[Parser] Parsed details: name={details['name']}, rating={details['rating']}, reviews={details['review_count']}")
            
            # Categories
            category_button = soup.select_one('button[jsaction*="category"]')
            if category_button:
                details['primary_category'] = category_button.get_text(strip=True)
            
            # Fallback for category
            if not details['primary_category']:
                category_span = soup.select_one('button.DkEaL')
                if category_span:
                    details['primary_category'] = category_span.get_text(strip=True)
            
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
            
            # Fallback for address
            if not details['address']:
                address_div = soup.select_one('div.rogA2c, div.Io6YTe')
                if address_div:
                    details['address'] = address_div.get_text(strip=True)
            
            # Phone
            phone_link = soup.select_one('button[data-item-id*="phone"]')
            if phone_link:
                details['phone'] = phone_link.get('aria-label', '').replace('Phone: ', '')
            
            # Website
            website_link = soup.select_one('a[data-item-id="authority"]')
            if website_link:
                details['website'] = website_link.get('href', '')
            
            # ========== PHOTO COUNT EXTRACTION ==========
            potential_photo_counts = []
            
            # Method 1: Button with aria-label containing "photos"
            photos_tab = soup.select_one('button[aria-label*="photos"]')
            if photos_tab:
                label = photos_tab.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*photos?', label, re.IGNORECASE)
                if count_match:
                    count = int(count_match.group(1).replace(',', ''))
                    potential_photo_counts.append(count)
                    log.debug(f"[Parser] Found candidate photo count from button aria-label: {count}")
            
            # Method 2: Any element with aria-label containing photo count
            for el in soup.select('[aria-label*="photo"], [aria-label*="Photo"]'):
                label = el.get('aria-label', '')
                count_match = re.search(r'([0-9,]+)\s*photos?', label, re.IGNORECASE)
                if count_match:
                    count = int(count_match.group(1).replace(',', ''))
                    potential_photo_counts.append(count)
            
            # Method 3: Look for "See all X photos" or "All photos (X)" patterns
            for el in soup.select('button, a, div[role="button"]'):
                text = el.get_text(strip=True)
                # Match "See all 123 photos" or "All photos (123)" or "123 photos" or "123+ photos"
                count_match = re.search(r'(?:See all\s+)?([0-9,]+)\+?\s*photos?|\(?([0-9,]+)\)?\s*photos?', text, re.IGNORECASE)
                if count_match:
                    count_str = count_match.group(1) or count_match.group(2)
                    if count_str:
                        count = int(count_str.replace(',', ''))
                        potential_photo_counts.append(count)
            
            # Method 4: Look in tabs for Photos tab with count
            for tab in soup.select('button[role="tab"], div[role="tab"]'):
                text = tab.get_text(strip=True)
                if 'photo' in text.lower():
                    count_match = re.search(r'(\d+)', text)
                    if count_match:
                        count = int(count_match.group(1))
                        potential_photo_counts.append(count)
            
            # Method 5: Search page text for photo count patterns (e.g. "1,234 photos")
            full_text = soup.get_text()[:4000]
            photo_patterns = re.findall(r'([0-9,]+)\s*photos?', full_text, re.IGNORECASE)
            for p in photo_patterns:
                try:
                    count = int(p.replace(',', ''))
                    potential_photo_counts.append(count)
                except:
                    pass

            # SELECT BEST PHOTO COUNT
            # Filter valid counts (ignore years like 2024, 2025 unless clearly a count)
            valid_counts = []
            for count in potential_photo_counts:
                # Basic sanity check: > 0 
                if count > 0:
                    # Filter out likely years 2000-2050 unless we have very high confidence from a specific selector
                    if 2000 <= count <= 2050:
                        continue 
                    if count > 100000:
                        continue # Skip unlikely high numbers
                    valid_counts.append(count)
            
            if valid_counts:
                # Take the maximum valid count found
                # Typically the total count is the largest number associated with "photos"
                details['photo_count'] = max(valid_counts)
                log.debug(f"[Parser] Selected best photo count: {details['photo_count']} from candidates: {valid_counts}")
            else:
                 log.debug(f"[Parser] ⚠️ Photo count extraction FAILED - no valid patterns found")
            
            # Reset photo count if it ended up being None
            if details['photo_count'] is None:
                 log.debug(f"[Parser] Photo count is None after selection logic")
            
            # ========== BUSINESS HOURS EXTRACTION ==========
            details['hours'] = self._extract_business_hours(soup)
            
            # ========== ENHANCED ATTRIBUTES EXTRACTION ==========
            details['attributes'] = self._extract_service_attributes(soup)
            
            # ========== SERVICE AREA EXTRACTION ==========
            details['service_area'] = self._extract_service_area(soup)
            
            # ========== Q&A SUMMARY ==========
            details['qa_count'] = self._extract_qa_count(soup)
            
            # ========== POSTS SUMMARY ==========
            posts_info = self._extract_posts_info(soup)
            details['post_count'] = posts_info.get('count', 0)
            details['last_post_date'] = posts_info.get('last_post_date')
            
            # ========== DESCRIPTION ==========
            details['description'] = self._extract_description(soup)
            
            # ========== NEW: ENHANCED DATA EXTRACTION ==========
            # Competitors (People also search for)
            details['competitors'] = self._extract_competitors(soup)
            
            # Review details (breakdown, summaries, recent with enhanced fields)
            details['review_details'] = self._extract_review_details(soup)
            
            # Popular times (structured by day)
            details['popular_times'] = self._extract_popular_times(soup)
            
            # Owner posts (enhanced with post_id, media_url)
            details['owner_posts'] = self._extract_owner_posts(soup)
            
            # Photo sample URLs
            details['photo_urls'] = self._extract_photo_urls(soup)
            
            # ========== NEW: ADDITIONAL ENHANCED EXTRACTIONS ==========
            
            # Coordinates (latitude/longitude) from page URL
            canonical = soup.select_one('link[rel="canonical"]')
            og_url = soup.select_one('meta[property="og:url"]')
            page_url = canonical.get('href') if canonical else (og_url.get('content') if og_url else '')
            if page_url:
                lat, lng = self._extract_coordinates_from_url(page_url)
                details['latitude'] = lat
                details['longitude'] = lng
            
            # Business status
            details['business_status'] = self._extract_business_status(soup)
            
            # Claimed status
            details['claimed_status'] = self._extract_claimed_status(soup)
            
            # Address components
            if details['address']:
                details['address_components'] = self._parse_address_components(details['address'])
            
            # Plus code
            details['plus_code'] = self._extract_plus_code(soup)
            
            # Engagement buttons
            details['engagement_buttons'] = self._extract_engagement_buttons(soup)
            
            # Video count
            details['video_count'] = self._extract_video_count(soup)
            
            # Enhanced photo details
            details['photos'] = self._extract_photo_details(soup)
            
            log.debug(f"[Parser] Parsed details: name={details['name']}, rating={details['rating']}, reviews={details['review_count']}, hours={bool(details['hours'])}, attrs={len(details['attributes'])}, competitors={len(details['competitors'])}, coords=({details['latitude']}, {details['longitude']})")
            
        except Exception as e:
            log.error(f"Error parsing place details: {e}")
            import traceback
            traceback.print_exc()
        
        return details
    
    def _extract_business_hours(self, soup) -> dict:
        """
        Extract business hours from Google Maps place page.
        
        Returns:
            dict with daily hours, e.g.:
            {
                'monday': {'open': '09:00', 'close': '18:00'},
                'tuesday': {'open': '09:00', 'close': '18:00'},
                ...
                'is_24_hours': False,
                'is_temporarily_closed': False
            }
        """
        hours = {}
        days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
        
        try:
            # Method 1: Look for hours button with aria-label containing the schedule
            hours_btn = soup.select_one('button[data-item-id="oh"], button[aria-label*="hour"]')
            if hours_btn:
                label = hours_btn.get('aria-label', '')
                # Parse schedules like "Monday 9 AM to 6 PM, Tuesday 9 AM to 6 PM..."
                for day in days:
                    day_pattern = rf'{day}\s*:?\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)\s*(?:to|-|–)\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)'
                    match = re.search(day_pattern, label, re.IGNORECASE)
                    if match:
                        hours[day] = {'open': match.group(1).strip(), 'close': match.group(2).strip()}
                    elif 'closed' in label.lower() and day in label.lower():
                        hours[day] = {'open': 'Closed', 'close': 'Closed'}
            
            # Method 2: Look for expanded hours table
            hours_table = soup.select('table.eK4R0e tr, div[class*="hours"] table tr')
            for row in hours_table:
                cells = row.select('td')
                if len(cells) >= 2:
                    day_text = cells[0].get_text(strip=True).lower()
                    time_text = cells[1].get_text(strip=True)
                    
                    for day in days:
                        if day in day_text:
                            if 'closed' in time_text.lower():
                                hours[day] = {'open': 'Closed', 'close': 'Closed'}
                            elif '24 hour' in time_text.lower() or 'open 24' in time_text.lower():
                                hours[day] = {'open': '00:00', 'close': '23:59'}
                                hours['is_24_hours'] = True
                            else:
                                time_match = re.search(r'(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)\s*(?:to|-|–)\s*(\d{1,2}(?::\d{2})?\s*(?:AM|PM)?)', time_text, re.IGNORECASE)
                                if time_match:
                                    hours[day] = {'open': time_match.group(1), 'close': time_match.group(2)}
            
            # Method 3: Check for "Open 24 hours" or "Temporarily closed"
            full_text = soup.get_text().lower()
            if 'open 24 hours' in full_text:
                hours['is_24_hours'] = True
            if 'temporarily closed' in full_text:
                hours['is_temporarily_closed'] = True
                
        except Exception as e:
            log.debug(f"[Parser] Hours extraction error: {e}")
        
        return hours if hours else None
    
    def _extract_service_attributes(self, soup) -> list:
        """
        Extract service attributes/amenities from Google Maps place page.
        
        Returns:
            List of attribute strings, e.g.:
            ['Dine-in', 'Takeout', 'Delivery', 'Wheelchair accessible', 'Free Wi-Fi']
        """
        attributes = []
        
        try:
            # Method 1: Service options section (dine-in, takeout, delivery, etc.)
            service_options = soup.select('div[aria-label*="Service options"] li, div[class*="service"] span')
            for opt in service_options:
                text = opt.get_text(strip=True)
                if text and len(text) > 1 and text not in attributes:
                    attributes.append(text)
            
            # Method 2: Amenities/Features section
            amenity_selectors = [
                'div[aria-label*="Amenities"] li',
                'div[aria-label*="Highlights"] li',
                'div[aria-label*="About"] li',
                'div[class*="amenities"] span',
                'div[class*="attributes"] span',
                'div.Ufn8mc span'
            ]
            for selector in amenity_selectors:
                for el in soup.select(selector):
                    text = el.get_text(strip=True)
                    # Filter out common non-attribute text
                    skip_words = ['reviews', 'photos', 'about', 'overview', 'directions', 'send to phone']
                    if text and len(text) > 1 and text not in attributes and text.lower() not in skip_words:
                        attributes.append(text)
            
            # Method 3: Parse from About tab content
            about_section = soup.select('div[data-tab="about"] span, div[aria-label="About this place"] li')
            for el in about_section:
                text = el.get_text(strip=True)
                if text and len(text) > 2 and len(text) < 50 and text not in attributes:
                    attributes.append(text)
            
            # Method 4: Look for check/tick marks indicating features
            for el in soup.select('[aria-label*="has"], [aria-label*="offers"]'):
                label = el.get('aria-label', '')
                if label and 'has' in label.lower() or 'offers' in label.lower():
                    # Extract just the feature name
                    feature = re.sub(r'^(has|offers)\s+', '', label, flags=re.IGNORECASE).strip()
                    if feature and feature not in attributes:
                        attributes.append(feature)
                        
        except Exception as e:
            log.debug(f"[Parser] Attributes extraction error: {e}")
        
        return attributes[:20]  # Limit to 20 attributes
    
    def _extract_service_area(self, soup) -> dict:
        """
        Extract service area for service-area businesses (SABs).
        
        Returns:
            dict with service area info:
            {
                'type': 'SERVICE_AREA' or 'STOREFRONT',
                'areas': ['City A', 'City B', 'Region X']
            }
        """
        service_area = {'type': 'STOREFRONT', 'areas': []}
        
        try:
            # Look for "Serves X area" or "Service area" text
            full_text = soup.get_text()
            
            # Check for service area business indicator
            if 'serves' in full_text.lower() and 'area' in full_text.lower():
                service_area['type'] = 'SERVICE_AREA'
            
            # Look for service area list
            service_area_section = soup.select('div[aria-label*="Service area"] li, div[class*="service-area"] span')
            for el in service_area_section:
                area = el.get_text(strip=True)
                if area and area not in service_area['areas']:
                    service_area['areas'].append(area)
            
            # Also check for "Serves X and nearby areas" pattern
            serves_match = re.search(r'Serves\s+(.+?)(?:\s+and\s+nearby|\s+area|$)', full_text)
            if serves_match:
                areas_text = serves_match.group(1)
                # Split by commas or "and"
                for area in re.split(r',|and', areas_text):
                    area = area.strip()
                    if area and area not in service_area['areas']:
                        service_area['areas'].append(area)
                        
        except Exception as e:
            log.debug(f"[Parser] Service area extraction error: {e}")
        
        return service_area
    
    def _extract_qa_count(self, soup) -> int:
        """
        Extract Q&A count from Google Maps place page.
        
        Returns:
            int: Number of Q&A entries
        """
        qa_count = 0
        
        try:
            # Look for Q&A tab or section
            qa_patterns = [
                r'(\d+)\s*(?:questions?\s*(?:and\s*)?)?(?:answers?)?',
                r'Q\s*&\s*A\s*\((\d+)\)',
                r'(\d+)\s*Q\s*&\s*A'
            ]
            
            # Check Q&A button/tab
            for el in soup.select('button[aria-label*="question"], button[aria-label*="Q&A"], button[data-tab*="qa"]'):
                label = el.get('aria-label', '') + el.get_text()
                for pattern in qa_patterns:
                    match = re.search(pattern, label, re.IGNORECASE)
                    if match:
                        qa_count = int(match.group(1))
                        break
                if qa_count > 0:
                    break
            
            # Fallback: search in full text
            if qa_count == 0:
                full_text = soup.get_text()[:3000]
                
                # Add specific pattern for "See all X questions"
                qa_patterns.append(r'See\s*all\s*(\d+)\s*questions?')
                
                for pattern in qa_patterns:
                    match = re.search(pattern, full_text, re.IGNORECASE)
                    if match:
                        qa_count = int(match.group(1))
                        break
                        
        except Exception as e:
            log.debug(f"[Parser] Q&A extraction error: {e}")
        
        return qa_count
    
    def _extract_posts_info(self, soup) -> dict:
        """
        Extract posts/updates information from Google Maps place page.
        
        Returns:
            dict with post info:
            {
                'count': 5,
                'last_post_date': '2024-01-15'  # or None
            }
        """
        posts_info = {'count': 0, 'last_post_date': None}
        
        try:
            # Look for Updates/Posts tab
            for el in soup.select('button[aria-label*="Updates"], button[aria-label*="Posts"], button[data-tab*="updates"]'):
                label = el.get('aria-label', '') + el.get_text()
                count_match = re.search(r'(\d+)\s*(?:updates?|posts?)', label, re.IGNORECASE)
                if count_match:
                    posts_info['count'] = int(count_match.group(1))
                    break
            
            # Look for recent post dates
            post_dates = soup.select('div[class*="post"] time, div[class*="update"] time, span[class*="date"]')
            for el in post_dates:
                date_text = el.get('datetime', el.get_text(strip=True))
                if date_text:
                    # Try to parse relative dates like "2 days ago"
                    if 'ago' in date_text.lower():
                        posts_info['last_post_date'] = 'recent'
                        break
                    # Try ISO format
                    try:
                        from datetime import datetime
                        posts_info['last_post_date'] = datetime.fromisoformat(date_text.replace('Z', '+00:00')).strftime('%Y-%m-%d')
                        break
                    except:
                        pass
                        
        except Exception as e:
            log.debug(f"[Parser] Posts extraction error: {e}")
        
        return posts_info
    
    def _extract_description(self, soup) -> str:
        """
        Extract business description from Google Maps place page.
        
        Returns:
            str: Business description or None
        """
        description = None
        
        try:
            # Look for description in About section
            desc_selectors = [
                'div[aria-label*="description"] p',
                'div[class*="about"] p',
                'div[data-attrid*="description"] span',
                'meta[name="description"]'
            ]
            
            for selector in desc_selectors:
                el = soup.select_one(selector)
                if el:
                    if el.name == 'meta':
                        text = el.get('content', '')
                    else:
                        text = el.get_text(strip=True)
                    
                    # Filter out generic/short text
                    if text and len(text) > 20:
                        # Remove "Google Maps" suffix if present
                        text = re.sub(r'\s*-?\s*Google Maps$', '', text)
                        description = text[:500]  # Limit to 500 chars
                        break
                        
        except Exception as e:
            log.debug(f"[Parser] Description extraction error: {e}")
        
        return description

    def _extract_competitors(self, soup) -> list:
        """
        Extract "People also search for" / competitor businesses.
        
        Returns:
            List of dicts with competitor info:
            [{'name': str, 'rating': float, 'reviews': int, 'category': str}]
        """
        competitors = []
        
        try:
            # Look for "People also search for" section
            # Common selectors for this section
            section_selectors = [
                'div[aria-label*="People also search"]',
                'div[aria-label*="Similar"]',
                'div[data-attrid*="also_search"]',
                'div:has(> div:contains("People also search for"))',
            ]
            
            section = None
            for selector in section_selectors:
                try:
                    section = soup.select_one(selector)
                    if section:
                        break
                except:
                    continue
            
            # Fallback: Find by header text
            if not section:
                headers = soup.find_all(['h2', 'h3', 'div', 'span'])
                for h in headers:
                    text = h.get_text(strip=True)
                    if 'People also search for' in text or 'Similar' in text:
                        section = h.parent
                        break
            
            if section:
                # Find all business items within the section
                items = section.select('a[href*="/maps/place/"], div[data-cid]')
                
                for item in items[:10]:  # Limit to 10 competitors
                    try:
                        comp = {}
                        
                        # Extract name
                        name_el = item.select_one('[aria-label], span.fontHeadlineSmall, span.fontBodyMedium')
                        if name_el:
                            comp['name'] = name_el.get('aria-label', name_el.get_text(strip=True))
                        else:
                            comp['name'] = item.get_text(strip=True).split('\n')[0][:50]
                        
                        if not comp['name'] or len(comp['name']) < 2:
                            continue
                        
                        # Extract rating and reviews from text
                        text = item.get_text()
                        rating_match = re.search(r'(\d\.\d)', text)
                        if rating_match:
                            comp['rating'] = float(rating_match.group(1))
                        
                        reviews_match = re.search(r'\((\d+)\)', text)
                        if reviews_match:
                            comp['reviews'] = int(reviews_match.group(1))
                        
                        # Extract category
                        cat_match = re.search(r'(\d\.\d)\s*\(?\d+\)?\s*([A-Za-z][A-Za-z\s]+)', text)
                        if cat_match:
                            comp['category'] = cat_match.group(2).strip()[:30]
                        
                        if comp not in competitors:
                            competitors.append(comp)
                            
                    except Exception as e:
                        continue
                        
        except Exception as e:
            log.debug(f"[Parser] Competitors extraction error: {e}")
        
        return competitors[:5]  # Return top 5 competitors

    def _extract_review_details(self, soup) -> dict:
        """
        Extract detailed review information including rating breakdown and top reviews.
        
        Returns:
            dict with:
            {
                'breakdown': {5: int, 4: int, 3: int, 2: int, 1: int},
                'summaries': [str, str, str],  # Top 3 review snippets
                'recent_reviews': [{'author': str, 'rating': float, 'date': str, 'text': str}]
            }
        """
        review_details = {
            'breakdown': {},
            'summaries': [],
            'recent_reviews': []
        }
        
        try:
            # === RATING BREAKDOWN ===
            # Look for star distribution bars/counts
            breakdown_selectors = [
                'div[aria-label*="stars,"]',
                'table[class*="rating"] tr',
                'div[class*="histogram"] div',
            ]
            
            for selector in breakdown_selectors:
                items = soup.select(selector)
                for item in items:
                    text = item.get('aria-label', item.get_text())
                    # Pattern: "5 stars, 80%" or "5 stars 1000 reviews"
                    # Also handle simple "5" followed by number in a table
                    match = re.search(r'(\d)\s*stars?\s*,?\s*(\d+)%?', text, re.IGNORECASE)
                    if match:
                        stars = int(match.group(1))
                        count_or_pct = int(match.group(2))
                        review_details['breakdown'][stars] = count_or_pct
                        
            # Fallback: Try to parse generic "5 4 3 2 1" list if strictly ordered
            if not review_details['breakdown']:
                 # Look for sequence of 5 numbers for star counts
                 # This is risky but helps when aria-labels are missing
                 pass
            
            # === REVIEW SUMMARIES ===
            # Look for highlighted review quotes
            summary_selectors = [
                'div[class*="review"] blockquote',
                'div[class*="summary"] span',
                'span[class*="quote"]',
            ]
            
            for selector in summary_selectors:
                for el in soup.select(selector)[:3]:
                    text = el.get_text(strip=True)
                    if text and len(text) > 15 and text not in review_details['summaries']:
                        # Clean up quotes
                        text = text.strip('"').strip('"').strip('"')
                        review_details['summaries'].append(text[:150])
            
            # Fallback: Look for text in quotes
            if not review_details['summaries']:
                full_text = soup.get_text()
                # Find quoted text patterns
                quotes = re.findall(r'"([^"]{20,150})"', full_text)
                for q in quotes[:3]:
                    if q not in review_details['summaries']:
                        review_details['summaries'].append(q)
            
            # === RECENT REVIEWS ===
            # Look for individual review cards
            review_selectors = [
                'div[data-review-id]',
                'div[class*="review"] div[class*="card"]',
                'div[jscontroller][data-hveid]',
                'div[aria-label*="Review"][role="article"]', # Generic accessible selector
            ]
            
            for selector in review_selectors:
                found_reviews = soup.select(selector)
                if not found_reviews: continue
                
                for review_el in found_reviews[:5]:
                    try:
                        review = {
                            'review_id': None,
                            'author': None,
                            'author_profile_url': None,
                            'rating': None,
                            'date': None,
                            'text': None,
                            'owner_reply': {
                                'text': None,
                                'date': None
                            }
                        }
                        
                        # Review ID
                        review_id = review_el.get('data-review-id', '')
                        if review_id:
                            review['review_id'] = review_id
                        
                        # Author and profile URL
                        author_el = review_el.select_one('[class*="author"], [class*="name"], span.d4r55')
                        if author_el:
                            review['author'] = author_el.get_text(strip=True)[:50]
                        
                        # Author profile link
                        author_link = review_el.select_one('a[href*="/contrib/"], a[href*="/maps/contrib/"]')
                        if author_link:
                            review['author_profile_url'] = author_link.get('href', '')
                        
                        # Rating
                        rating_el = review_el.select_one('[aria-label*="star"]')
                        if rating_el:
                            match = re.search(r'(\d)', rating_el.get('aria-label', ''))
                            if match:
                                review['rating'] = int(match.group(1))
                        
                        # Date
                        date_el = review_el.select_one('[class*="date"], time, span.rsqaWe')
                        if date_el:
                            review['date'] = date_el.get('datetime', date_el.get_text(strip=True))[:20]
                        
                        # Text
                        text_el = review_el.select_one('[class*="text"], span.wiI7pd')
                        if text_el:
                            review['text'] = text_el.get_text(strip=True)[:300]
                        
                        # Owner reply - look for response section
                        reply_section = review_el.select_one('[class*="owner-response"], [class*="reply"], div[class*="response"]')
                        if reply_section:
                            reply_text_el = reply_section.select_one('span, p, div[class*="text"]')
                            if reply_text_el:
                                review['owner_reply']['text'] = reply_text_el.get_text(strip=True)[:300]
                            
                            reply_date_el = reply_section.select_one('[class*="date"], time')
                            if reply_date_el:
                                review['owner_reply']['date'] = reply_date_el.get('datetime', reply_date_el.get_text(strip=True))[:20]
                        
                        # Also look for "Response from the owner" pattern
                        if not review['owner_reply']['text']:
                            response_header = review_el.find(string=re.compile(r'Response from', re.IGNORECASE))
                            if response_header and response_header.parent:
                                response_parent = response_header.parent.parent
                                if response_parent:
                                    response_text = response_parent.get_text(strip=True)
                                    # Remove the header text
                                    response_text = re.sub(r'Response from.*?:', '', response_text, flags=re.IGNORECASE).strip()
                                    if response_text:
                                        review['owner_reply']['text'] = response_text[:300]
                        
                        if review.get('author') or review.get('text'):
                            review_details['recent_reviews'].append(review)
                            
                    except:
                        continue
                        
        except Exception as e:
            log.debug(f"[Parser] Review details extraction error: {e}")
        
        return review_details

    def _extract_popular_times(self, soup) -> dict:
        """
        Extract popular times / busyness data structured by day.
        
        Returns:
            dict with day -> hour -> busyness level:
            {
                'monday': {'9': 30, '10': 45, '11': 60, ...},
                'tuesday': {...},
                ...
                'current_busyness': 'Usually not too busy',
                'live_busyness_percent': 45  # Real-time if available
            }
        """
        days_of_week = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday']
        popular_times = {day: {} for day in days_of_week}
        popular_times['current_busyness'] = None
        popular_times['live_busyness_percent'] = None
        
        try:
            # Look for popular times graph/section
            times_section = soup.select_one('div[aria-label*="Popular times"], div[data-attrid*="popular_times"], div[class*="popular"]')
            
            if times_section:
                # Check for day tabs/buttons
                day_tabs = times_section.select('button[role="tab"], div[data-day]')
                current_day = 'monday'  # Default
                
                for tab in day_tabs:
                    tab_text = tab.get_text(strip=True).lower()
                    for day in days_of_week:
                        if day[:3] in tab_text or day in tab_text:
                            if tab.get('aria-selected') == 'true' or 'selected' in tab.get('class', []):
                                current_day = day
                            break
                
                # Extract busyness bars
                bars = times_section.select('div[aria-label*="busy"], div[style*="height"], div[class*="bar"]')
                
                for bar in bars:
                    label = bar.get('aria-label', '')
                    
                    # Pattern: "Usually 45% busy at 10 AM" or "45% busy at 10 AM on Monday"
                    match = re.search(r'(\d+)%\s*busy\s*at\s*(\d+)\s*(AM|PM)?(?:\s*on\s*(\w+))?', label, re.IGNORECASE)
                    if match:
                        pct = int(match.group(1))
                        hour = int(match.group(2))
                        am_pm = match.group(3)
                        day_mentioned = match.group(4)
                        
                        # Convert to 24-hour format
                        if am_pm and am_pm.upper() == 'PM' and hour < 12:
                            hour += 12
                        elif am_pm and am_pm.upper() == 'AM' and hour == 12:
                            hour = 0
                        
                        # Determine which day
                        target_day = current_day
                        if day_mentioned:
                            day_lower = day_mentioned.lower()
                            for d in days_of_week:
                                if d.startswith(day_lower[:3]):
                                    target_day = d
                                    break
                        
                        popular_times[target_day][str(hour)] = pct
                    
                    # Also try style-based extraction for bar heights
                    style = bar.get('style', '')
                    height_match = re.search(r'height:\s*(\d+)%', style)
                    if height_match and not label:
                        # This is less reliable but a fallback
                        pass
            
            # Current/Live busyness
            current_selectors = [
                '[aria-label*="Currently"]',
                '[aria-label*="usually"]',
                'div[class*="live"]',
                'span:contains("busy")'
            ]
            for selector in current_selectors:
                try:
                    current = soup.select_one(selector)
                    if current:
                        text = current.get('aria-label', current.get_text(strip=True))
                        popular_times['current_busyness'] = text[:80]
                        
                        # Try to extract percentage
                        pct_match = re.search(r'(\d+)%', text)
                        if pct_match:
                            popular_times['live_busyness_percent'] = int(pct_match.group(1))
                        break
                except:
                    continue
                
        except Exception as e:
            log.debug(f"[Parser] Popular times extraction error: {e}")
        
        # Only return if we have data
        has_data = any(popular_times[day] for day in days_of_week) or popular_times['current_busyness']
        return popular_times if has_data else None

    def _extract_owner_posts(self, soup) -> list:
        """
        Extract business owner posts/updates with enhanced metadata.
        
        Returns:
            List of posts:
            [{
                'post_id': str,
                'content': str,
                'media_url': str,
                'date': str
            }]
        """
        posts = []
        
        try:
            # Look for "From the owner" or "Updates" section
            post_selectors = [
                'div[aria-label*="From the owner"] div[data-post-id]',
                'div[aria-label*="From the owner"] div[class*="post"]',
                'div[aria-label*="Updates"] div[class*="post"]',
                'div[data-attrid*="updates"] div[class*="item"]',
                'div[class*="owner-post"]',
            ]
            
            seen_posts = set()
            
            for selector in post_selectors:
                for post_el in soup.select(selector)[:5]:
                    try:
                        post = {
                            'post_id': None,
                            'content': None,
                            'media_url': None,
                            'date': None
                        }
                        
                        # Post ID
                        post_id = post_el.get('data-post-id', '')
                        if not post_id:
                            # Try to extract from a link
                            link = post_el.select_one('a[href*="post"]')
                            if link:
                                href = link.get('href', '')
                                id_match = re.search(r'post[/=]([a-zA-Z0-9_-]+)', href)
                                if id_match:
                                    post_id = id_match.group(1)
                        post['post_id'] = post_id if post_id else None
                        
                        # Content
                        text_el = post_el.select_one('span[class*="text"], p, div[class*="content"]')
                        if text_el:
                            text = text_el.get_text(strip=True)
                        else:
                            text = post_el.get_text(strip=True)
                        
                        if text and len(text) > 20:
                            # Remove date patterns from text
                            text = re.sub(r'\d+\s*(day|week|month|year)s?\s*ago', '', text, flags=re.IGNORECASE)
                            post['content'] = text[:300].strip()
                        
                        # Check for duplicate content
                        if post['content'] and post['content'] in seen_posts:
                            continue
                        if post['content']:
                            seen_posts.add(post['content'])
                        
                        # Media URL (image/video)
                        media_el = post_el.select_one('img[src*="googleusercontent"], img[src*="ggpht"], video source')
                        if media_el:
                            media_url = media_el.get('src', media_el.get('data-src', ''))
                            if media_url and 'data:' not in media_url:
                                post['media_url'] = media_url
                        
                        # Date
                        date_el = post_el.select_one('time, span[class*="date"], span[class*="time"]')
                        if date_el:
                            date_text = date_el.get('datetime', date_el.get_text(strip=True))
                            post['date'] = date_text[:30] if date_text else None
                        
                        # Fallback: look for relative date in text
                        if not post['date']:
                            full_text = post_el.get_text()
                            date_match = re.search(r'(\d+\s*(?:day|week|month|year)s?\s*ago)', full_text, re.IGNORECASE)
                            if date_match:
                                post['date'] = date_match.group(1)
                        
                        if post.get('content'):
                            posts.append(post)
                            
                    except:
                        continue
                
                # Stop if we have enough posts
                if len(posts) >= 5:
                    break
                        
        except Exception as e:
            log.debug(f"[Parser] Owner posts extraction error: {e}")
        
        return posts[:5]

    def _extract_photo_urls(self, soup) -> list:
        """
        Extract sample photo URLs from the place page.
        
        Returns:
            List of photo URLs (up to 5)
        """
        photo_urls = []
        
        try:
            # 1. Main Knowledge Panel / Header Image (Prioritize this)
            main_photo = None
            
            # Strategy A: Look for "See photos" container
            see_photos_btn = soup.select_one('button[aria-label*="See photos"], div[aria-label*="See photos"]')
            if see_photos_btn:
                # The image might be inside or a sibling/parent bg
                img = see_photos_btn.select_one('img')
                if img: main_photo = img
            
            # Strategy B: First button with a large image (standard Maps layout)
            if not main_photo:
                buttons = soup.select('button img')
                for img in buttons[:3]:
                    if img.get('src') and 'googleusercontent' in img.get('src'):
                        main_photo = img
                        break
            
            if main_photo:
                src = main_photo.get('src', '')
                if src and 'data:' not in src and len(src) > 50:
                     # Clean high-res URL if possible (remove size params)
                     clean_url = src.split('=')[0]
                     photo_urls.append(clean_url + '=s680-w680-h510') # Request reasonable size
                     seen_urls.add(clean_url)

            # 2. Look for photo gallery images
            img_selectors = [
                'button[aria-label*="Photo"] img[src]',
                'div[aria-label*="Photo"] img[src]',
                'div[dataset-id] img[src]',
                'img[src*="googleusercontent"]',
            ]
            
            seen_urls = set()
            for selector in img_selectors:
                for img in soup.select(selector)[:10]:
                    src = img.get('src', '')
                    # Filter out tiny icons and data URIs
                    if src and 'data:' not in src and len(src) > 50:
                        # Clean up URL parameters for consistency
                        base_url = src.split('=')[0] if '=' in src else src
                        if base_url not in seen_urls:
                            seen_urls.add(base_url)
                            photo_urls.append(src)
                            
                            if len(photo_urls) >= 5:
                                break
                                
                if len(photo_urls) >= 5:
                    break
                    
        except Exception as e:
            log.debug(f"[Parser] Photo URLs extraction error: {e}")
        
        return photo_urls

    def _extract_business_status(self, soup) -> str:
        """
        Extract business open/closed status.
        
        Returns:
            str: 'OPEN' | 'CLOSED' | 'TEMPORARILY_CLOSED' | 'PERMANENTLY_CLOSED' | None
        """
        try:
            full_text = soup.get_text().lower()
            
            # Check for permanently closed first
            if 'permanently closed' in full_text:
                return 'PERMANENTLY_CLOSED'
            
            # Check for temporarily closed
            if 'temporarily closed' in full_text:
                return 'TEMPORARILY_CLOSED'
            
            # Check for closed indicators
            closed_patterns = ['closed now', 'hours might differ', 'closed ⋅']
            for pattern in closed_patterns:
                if pattern in full_text:
                    return 'CLOSED'
            
            # Check for open indicators
            open_patterns = ['open now', 'open ⋅', 'opens at', 'open 24 hours']
            for pattern in open_patterns:
                if pattern in full_text:
                    return 'OPEN'
            
            # Also check aria-labels for status
            status_el = soup.select_one('[aria-label*="Open"], [aria-label*="Closed"]')
            if status_el:
                label = status_el.get('aria-label', '').lower()
                if 'open' in label:
                    return 'OPEN'
                elif 'closed' in label:
                    return 'CLOSED'
                    
        except Exception as e:
            log.debug(f"[Parser] Business status extraction error: {e}")
        
        return None

    def _extract_claimed_status(self, soup) -> bool:
        """
        Extract whether business is claimed/verified.
        
        Returns:
            bool: True = claimed, False = unclaimed, None = unknown
        """
        try:
            full_text = soup.get_text().lower()
            
            # Unclaimed indicators
            if 'claim this business' in full_text or 'own this business?' in full_text:
                return False
            
            # Look for "Claim this business" button
            claim_btn = soup.select_one('button[aria-label*="Claim"], a[href*="claim"]')
            if claim_btn:
                return False
            
            # Claimed/verified indicators
            if 'verified' in full_text:
                return True
            
            # Look for verified badge
            verified_badge = soup.select_one('[aria-label*="Verified"], div[class*="verified"]')
            if verified_badge:
                return True
            
            # If business has owner responses to reviews, it's likely claimed
            owner_response = soup.select_one('div[class*="owner-response"], span:contains("Response from")')
            if owner_response:
                return True
                
        except Exception as e:
            log.debug(f"[Parser] Claimed status extraction error: {e}")
        
        return None

    def _parse_address_components(self, address: str) -> dict:
        """
        Parse a full address string into components.
        
        Args:
            address: Full address string like "123 Main St, City, ST 12345, Country"
            
        Returns:
            dict with street, city, state, postal_code, country
        """
        components = {
            'street': None,
            'city': None,
            'state': None,
            'postal_code': None,
            'country': None
        }
        
        if not address:
            return components
        
        try:
            # Clean up address
            address = address.strip()
            
            # Split by comma
            parts = [p.strip() for p in address.split(',')]
            
            if len(parts) >= 1:
                components['street'] = parts[0]
            
            if len(parts) >= 2:
                components['city'] = parts[1]
            
            if len(parts) >= 3:
                # This part often contains "State ZIP" or just state
                state_zip = parts[2].strip()
                
                # Try to extract state and ZIP
                # Pattern: "CA 90210" or "California 90210" or just "CA"
                zip_match = re.search(r'(\d{5}(?:-\d{4})?)', state_zip)
                if zip_match:
                    components['postal_code'] = zip_match.group(1)
                    state_part = state_zip.replace(zip_match.group(1), '').strip()
                    if state_part:
                        components['state'] = state_part
                else:
                    components['state'] = state_zip
            
            if len(parts) >= 4:
                components['country'] = parts[3].strip()
            
            # Try to extract country codes like "USA", "India", "UK" from last part
            if not components['country'] and len(parts) >= 3:
                last_part = parts[-1].strip()
                # Common country patterns
                country_patterns = ['USA', 'US', 'United States', 'UK', 'United Kingdom', 
                                   'India', 'Canada', 'Australia', 'Germany', 'France']
                for country in country_patterns:
                    if country.lower() in last_part.lower():
                        components['country'] = country
                        break
                        
        except Exception as e:
            log.debug(f"[Parser] Address parsing error: {e}")
        
        return components

    def _extract_plus_code(self, soup) -> str:
        """
        Extract Plus Code (Open Location Code) from the page.
        
        Returns:
            str: Plus code like "7JVW52GV+XR" or None
        """
        try:
            # Plus codes often appear in a data attribute or text
            # Look for button/link with plus code
            plus_code_btn = soup.select_one('button[data-item-id*="plus_code"], button[aria-label*="Plus code"]')
            if plus_code_btn:
                label = plus_code_btn.get('aria-label', '')
                # Extract the code from label like "Plus code: 7JVW52GV+XR"
                match = re.search(r'([A-Z0-9]{4,8}\+[A-Z0-9]{2,3})', label)
                if match:
                    return match.group(1)
                # Also check button text
                text = plus_code_btn.get_text(strip=True)
                match = re.search(r'([A-Z0-9]{4,8}\+[A-Z0-9]{2,3})', text)
                if match:
                    return match.group(1)
            
            # Search in page text
            full_text = soup.get_text()
            # Plus code format: 4-8 chars + plus sign + 2-3 chars
            match = re.search(r'([A-Z0-9]{4,8}\+[A-Z0-9]{2,3})', full_text)
            if match:
                return match.group(1)
                
        except Exception as e:
            log.debug(f"[Parser] Plus code extraction error: {e}")
        
        return None

    def _extract_engagement_buttons(self, soup) -> dict:
        """
        Extract which engagement action buttons are available.
        
        Returns:
            dict with boolean flags for each button type
        """
        buttons = {
            'directions': False,
            'call': False,
            'website': False,
            'booking': False,
            'menu': False,
            'order_online': False
        }
        
        try:
            # Directions button
            if soup.select_one('button[data-item-id="directions"], button[aria-label*="Directions"], a[data-value="Directions"]'):
                buttons['directions'] = True
            
            # Call button
            if soup.select_one('button[data-item-id*="phone"], button[aria-label*="Call"], a[href^="tel:"]'):
                buttons['call'] = True
            
            # Website button
            if soup.select_one('a[data-item-id="authority"], button[aria-label*="Website"], a[data-value="Website"]'):
                buttons['website'] = True
            
            # Booking/Appointment button
            booking_selectors = [
                'a[href*="book"], a[href*="appointment"]',
                'button[aria-label*="Book"], button[aria-label*="Appointment"]',
                'button[data-item-id*="book"], button[data-item-id*="appointment"]'
            ]
            for selector in booking_selectors:
                if soup.select_one(selector):
                    buttons['booking'] = True
                    break
            
            # Menu button
            if soup.select_one('button[aria-label*="Menu"], a[data-item-id*="menu"], button[data-item-id*="menu"]'):
                buttons['menu'] = True
            
            # Order online button
            order_selectors = [
                'a[href*="order"], button[aria-label*="Order"]',
                'button[data-item-id*="order"], a[data-value*="Order"]'
            ]
            for selector in order_selectors:
                if soup.select_one(selector):
                    buttons['order_online'] = True
                    break
            
            # Also check for text-based indicators
            buttons_area = soup.select('button[jsaction], div[role="button"]')
            for btn in buttons_area[:20]:  # Check first 20 buttons
                text = btn.get_text(strip=True).lower()
                aria = btn.get('aria-label', '').lower()
                combined = text + ' ' + aria
                
                if 'direction' in combined:
                    buttons['directions'] = True
                if 'call' in combined or 'phone' in combined:
                    buttons['call'] = True
                if 'website' in combined:
                    buttons['website'] = True
                if 'book' in combined or 'appointment' in combined or 'reserve' in combined:
                    buttons['booking'] = True
                if 'menu' in combined:
                    buttons['menu'] = True
                if 'order' in combined:
                    buttons['order_online'] = True
                    
        except Exception as e:
            log.debug(f"[Parser] Engagement buttons extraction error: {e}")
        
        return buttons

    def _extract_video_count(self, soup) -> int:
        """
        Extract video count from the page.
        
        Returns:
            int: Number of videos or None
        """
        try:
            # Look for video tab or count
            video_patterns = [
                r'(\d+)\s*videos?',
                r'Videos?\s*\((\d+)\)',
            ]
            
            # Check tabs/buttons
            for el in soup.select('button[aria-label*="Video"], button[role="tab"]'):
                text = el.get('aria-label', '') + el.get_text()
                for pattern in video_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        return int(match.group(1))
            
            # Search in page text
            full_text = soup.get_text()[:3000]
            for pattern in video_patterns:
                match = re.search(pattern, full_text, re.IGNORECASE)
                if match:
                    count = int(match.group(1))
                    # Sanity check
                    if 0 < count < 10000:
                        return count
                        
        except Exception as e:
            log.debug(f"[Parser] Video count extraction error: {e}")
        
        return None

    def _extract_photo_details(self, soup) -> list:
        """
        Extract detailed photo information including categories.
        
        Returns:
            list of dicts with photo_id, url, category, uploaded_by, upload_date
        """
        photos = []
        
        try:
            # Photo categories from Google Maps
            category_mapping = {
                'all': 'other',
                'latest': 'other',
                'interior': 'interior',
                'exterior': 'exterior',
                'menu': 'menu',
                'food': 'menu',
                'team': 'team',
                'staff': 'team',
                'videos': 'video',
                'street view': 'exterior',
                'by owner': 'owner',
                'by customers': 'customer'
            }
            
            seen_urls = set()
            
            # Look for photos with metadata
            photo_selectors = [
                'button[data-photo-id] img',
                'div[data-photo-id] img',
                'button[aria-label*="Photo"] img',
                'img[src*="googleusercontent"][data-atf]'
            ]
            
            for selector in photo_selectors:
                for img in soup.select(selector)[:10]:
                    try:
                        photo = {
                            'photo_id': None,
                            'url': None,
                            'category': 'other',
                            'uploaded_by': None,
                            'upload_date': None
                        }
                        
                        # Get URL
                        src = img.get('src', '')
                        if not src or 'data:' in src or len(src) < 50:
                            continue
                        
                        base_url = src.split('=')[0] if '=' in src else src
                        if base_url in seen_urls:
                            continue
                        seen_urls.add(base_url)
                        
                        photo['url'] = src
                        
                        # Get photo ID from parent
                        parent = img.parent
                        if parent:
                            photo_id = parent.get('data-photo-id', '')
                            if photo_id:
                                photo['photo_id'] = photo_id
                        
                        # Try to determine category from context
                        aria_label = parent.get('aria-label', '').lower() if parent else ''
                        for keyword, category in category_mapping.items():
                            if keyword in aria_label:
                                photo['category'] = category
                                break
                        
                        # Check if uploaded by owner or customer
                        context_text = aria_label
                        if 'owner' in context_text or 'business' in context_text:
                            photo['uploaded_by'] = 'owner'
                        elif 'customer' in context_text or 'user' in context_text or 'visitor' in context_text:
                            photo['uploaded_by'] = 'customer'
                        
                        photos.append(photo)
                        
                        if len(photos) >= 10:
                            break
                            
                    except:
                        continue
                        
                if len(photos) >= 10:
                    break
                    
        except Exception as e:
            log.debug(f"[Parser] Photo details extraction error: {e}")
        
        return photos


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
