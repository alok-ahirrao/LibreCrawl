"""
Google SERP Parser
Extracts structured data from Google Search Results Pages.
"""
import re
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, unquote


class GoogleSerpParser:
    """
    Parses HTML content from Google Search Results.
    Extracts organic results, local pack, and SERP features.
    """
    
    # SERP Feature identifiers
    SERP_FEATURES = {
        'ads_top': False,
        'ads_bottom': False,
        'local_pack': False,
        'featured_snippet': False,
        'people_also_ask': False,
        'knowledge_panel': False,
        'image_pack': False,
        'video_carousel': False,
        'news_box': False,
        'shopping_results': False,
        'sitelinks': False,
        'reviews': False,
    }
    
    def parse_serp_results(self, html_content: str, target_domain: str = None) -> dict:
        """
        Parse Google SERP HTML and extract structured data.
        
        Args:
            html_content: Raw HTML from Google Search
            target_domain: Optional domain to find ranking for
            
        Returns:
            Dict with organic_results, local_pack, serp_features, target_rank, target_url
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        
        result = {
            'organic_results': [],
            'local_pack': [],
            'serp_features': dict(self.SERP_FEATURES),
            'target_rank': None,
            'target_url': None,
            'total_results': None,
        }
        
        # Extract total results count
        result['total_results'] = self._extract_total_results(soup)
        
        # Detect SERP features
        result['serp_features'] = self._detect_serp_features(soup)
        
        # Extract organic results
        result['organic_results'] = self._extract_organic_results(soup)
        
        # Check for Extended Local Pack (Local Finder)
        local_finder_html = None
        if '<!-- LOCAL_FINDER_HTML_START -->' in html_content:
            try:
                parts = html_content.split('<!-- LOCAL_FINDER_HTML_START -->')
                # Use the first part as the main SERP (organic results already parsed from it effectively, but let's be safe)
                # Actually, organic results parsed from 'soup' which was created from FULL html_content. 
                # This is fine, BS4 ignores the comment/extra data usually? 
                # Wait, if I parse FULL content, BS4 sees the appended HTML too.
                # Organic extraction restricts itself to #rso, so it should be fine.
                
                if len(parts) > 1:
                    local_finder_html = parts[1].split('<!-- LOCAL_FINDER_HTML_END -->')[0]
            except:
                pass

        # Check for Hotel Pack
        hotel_finder_html = None
        if '<!-- HOTELS_HTML_START -->' in html_content:
            try:
                parts = html_content.split('<!-- HOTELS_HTML_START -->')
                if len(parts) > 1:
                    hotel_finder_html = parts[1].split('<!-- HOTELS_HTML_END -->')[0]
            except:
                pass

        # Extract local pack
        if local_finder_html:
            local_soup = BeautifulSoup(local_finder_html, 'html.parser')
            extended_pack = self._extract_extended_local_pack(local_soup)
            if extended_pack:
                result['local_pack'] = extended_pack
                result['serp_features']['local_pack'] = True # Confirm it exists
        elif result['serp_features']['local_pack']:
            result['local_pack'] = self._extract_local_pack(soup)

        # Extract hotel results
        result['hotel_results'] = []
        result['shopping_results'] = []
        
        if hotel_finder_html:
            hotel_soup = BeautifulSoup(hotel_finder_html, 'html.parser')
            result['hotel_results'] = self._extract_hotel_pack(hotel_soup)
            if result['hotel_results']:
                result['serp_features']['hotel_pack'] = True

        # Check for Shopping HTML
        shopping_finder_html = None
        if '<!-- SHOPPING_HTML_START -->' in html_content:
            try:
                shopping_finder_html = html_content.split('<!-- SHOPPING_HTML_START -->')[1].split('<!-- SHOPPING_HTML_END -->')[0]
            except IndexError:
                pass
        
        if shopping_finder_html:
            shopping_soup = BeautifulSoup(shopping_finder_html, 'html.parser')
            result['shopping_results'] = self._extract_shopping_results(shopping_soup)
            if result['shopping_results']:
                 result['serp_features']['shopping_graph'] = True
        
        # Find target domain ranking
        if target_domain:
            target_domain_clean = target_domain.lower().replace('www.', '').strip()
            for res in result['organic_results']:
                url_domain = urlparse(res['url']).netloc.lower().replace('www.', '')
                if target_domain_clean in url_domain or url_domain in target_domain_clean:
                    result['target_rank'] = res['rank']
                    result['target_url'] = res['url']
                    break
        
        return result

    def _extract_hotel_pack(self, soup) -> list:
        """Extract hotel data from the Hotel Finder view."""
        hotel_results = []
        
        try:
            # Hotel Cards Strategy
            # Generic item selector: div[jscontroller] usually wraps interactive cards in hotel finder
            items = soup.select('c-wiz[jsrenderer] div[jscontroller]')
            
            if not items:
                # Fallback to a broader search for cards with images and text
                items = soup.select('div[role="listitem"]')
                
            if not items:
                 # Fallback: look for cards with prices
                 price_elems = soup.select('span:contains("₹"), span:contains("$")')
                 items = []
                 seen_parents = set()
                 for p in price_elems:
                     parent = p.parent
                     for _ in range(6):
                         if parent and parent not in seen_parents and len(parent.get_text()) > 50:
                             if parent.select_one('img'):
                                items.append(parent)
                                seen_parents.add(parent)
                                break
                         if parent: parent = parent.parent

            unique_hotels = set()
            
            for idx, item in enumerate(items, 1):
                try:
                    full_text = item.get_text()
                    
                    # Name Extraction
                    name = ''
                    # Try specific classes first
                    name_elem = item.select_one('.BgYkof') or item.select_one('.kCsHn') or item.select_one('h2') or item.select_one('h3')
                    
                    if name_elem:
                        name = name_elem.get_text(strip=True)
                    else:
                        # Fallback: Split text by newlines and take first valid line
                        lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                        for l in lines:
                            if len(l) > 3 and '₹' not in l and '$' not in l and 'review' not in l.lower():
                                name = l
                                break
                    
                    if not name or len(name) > 100: 
                        continue

                    # Filter out non-hotel UI elements
                    IGNORED_TERMS = [
                        'Skip to main content', 'Accessibility feedback', 'Travel', 'Explore', 'Flights', 'Hotels', 
                        'Holiday rentals', 'Change appearance', 'Enter a date', 'Adults', 'Children', 'All filters', 
                        'Sort by', 'When to visit', "What you'll pay", 'Loading...', 'Loading results', 
                        'Update list', 'Map', 'Satellite', 'Currency', 'Language'
                    ]
                    
                    if any(term.lower() in name.lower() for term in IGNORED_TERMS):
                        continue
                    
                    # Deduplication
                    if name in unique_hotels:
                        continue
                    unique_hotels.add(name)

                    # Price
                    price = ''
                    # Look for currency symbol explicitly in text nodes
                    import re
                    price_match = re.search(r'(₹|Rs\.|USD|\$)\s?[\d,]+', full_text)
                    if price_match:
                        price = price_match.group(0)
                        
                    # Rating & Reviews
                    rating = ''
                    reviews = ''
                    
                    # Look for aria-label "X stars Y reviews"
                    rating_span = item.select_one('span[aria-label*="star"]') or item.select_one('span[aria-label*="review"]')
                    if rating_span:
                        aria = rating_span.get('aria-label', '')
                        rating_match = re.search(r'(\d+(\.\d+)?) star', aria)
                        if rating_match:
                            rating = rating_match.group(1)
                            
                        reviews_match = re.search(r'([\d,]+) review', aria)
                        if reviews_match:
                            reviews = reviews_match.group(1)
                    
                    # Fallback for rating/reviews from text
                    if not rating:
                         rating_text_match = re.search(r'(\d\.\d)\s?/\s?5', full_text)
                         if rating_text_match:
                             rating = rating_text_match.group(1)
                             
                    if not reviews:
                        reviews_text_match = re.search(r'\(([\d,.]+)(?: reviews|k)?\)', full_text)
                        if reviews_text_match:
                            val = reviews_text_match.group(1)
                            if 'k' in full_text and '.' in val:
                                reviews = val + 'k' # Keep the k format
                            else:
                                reviews = val

                    # STRICT VALIDATION: If no price AND no rating, it's likely not a hotel card
                    if not price and not rating:
                        continue

                    # Image
                    image = ''
                    img_elem = item.select_one('div[role="img"]') 
                    if img_elem:
                        style = img_elem.get('style', '')
                        if 'url(' in style:
                            image = style.split('url(')[1].split(')')[0].replace('"', '')
                    
                    if not image:
                         img_tag = item.select_one('img')
                         if img_tag:
                             image = img_tag.get('src', '')

                    # Deal / Badge
                    deal = ''
                    if 'Deal' in full_text or 'Great deal' in full_text:
                        deal = 'Great Deal'

                    hotel_results.append({
                        'rank': len(hotel_results) + 1, # specific rank
                        'name': name,
                        'price': price,
                        'rating': rating,
                        'reviews': reviews,
                        'image': image,
                        'deal': deal
                    })
                            
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"[SerpParser] Error extracting hotel pack: {e}")
            
        return hotel_results
    
    def _extract_total_results(self, soup) -> str:
        """Extract 'About X results' text."""
        try:
            result_stats = soup.select_one('#result-stats')
            if result_stats:
                return result_stats.get_text(strip=True)
        except:
            pass
        return None
    
    def _detect_serp_features(self, soup) -> dict:
        """Detect which SERP features are present on the page."""
        features = dict(self.SERP_FEATURES)
        
    def _detect_serp_features(self, soup) -> dict:
        """Detect which SERP features are present on the page using modern selectors."""
        features = dict(self.SERP_FEATURES)
        
        try:
            # Helper to check text content
            text_content = soup.get_text().lower()
            html_str = str(soup)
            
            # --- ADS ---
            # Top/Bottom Ads
            ads_selectors = [
                 'div[id^="tads"]',     # Top ads container
                 'div[id^="bottomads"]', # Bottom ads container
                 'div.uEierd',          # Individual ad unit
                 'div[aria-label="Ads"]',
                 'span:contains("Sponsored")',
                 'div[data-text-ad]'
            ]
            for sel in ads_selectors:
                if soup.select_one(sel):
                    # Distinguish top vs bottom based on ID if possible, but generally mark ads present
                    if 'bottom' in str(sel) or 'bottom' in str(soup.select_one(sel).get('id', '')):
                         features['ads_bottom'] = True
                    else:
                         features['ads_top'] = True
            
            # --- LOCAL PACK ---
            # 3-Pack, Local Finder
            local_selectors = [
                'div.VkpGBb',           # Standard local result
                'div.istjKb',           # Local pack container
                'div[data-local-attribute]', # Local attributes
                'div#bg-place-station', # Local station
                'div.loc-local-pack-content',
                'div.rllt__details',    # Details in local pack
                'a[href*="maps.google.com/maps?"]', # Map links
                'div[data-hveid] div.dbg0pd' # Local business names
            ]
            if any(soup.select_one(s) for s in local_selectors):
                features['local_pack'] = True
            
            # --- FEATURED SNIPPET ---
            # Position Zero, Answer Box
            snippet_selectors = [
                'div.xpdopen',
                'div.M8OgIe',          # Featured snippet description
                'div.ifM9O',           # Featured snippet container
                'div[data-featured-snippet]',
                'h2:contains("Featured snippet")',
                'div.IZ6rdc',
                'block-component div.hp-xpdbox'
            ]
            if any(soup.select_one(s) for s in snippet_selectors):
                features['featured_snippet'] = True
            
            # --- PEOPLE ALSO ASK ---
            # Related Questions
            paa_selectors = [
                'div.related-question-pair',
                'div[jsname="Cpkphb"]', # Accordion container
                'div.wQiwMc',           # Question header
                'div[data-q]',          # Question data attribute
                'div.Wt5Tfe'            # PAA container
            ]
            if any(soup.select_one(s) for s in paa_selectors):
                features['people_also_ask'] = True
                
            # --- KNOWLEDGE PANEL ---
            # Right-side panel
            kp_selectors = [
                'div.kp-wholepage',
                'div#kp-wp-tab-overview',
                'div.knowledge-panel',
                'div[data-attrid="title"]',
                'div.B1u6xe'           # Knowledge panel header
            ]
            if any(soup.select_one(s) for s in kp_selectors):
                features['knowledge_panel'] = True

            # --- FORMS OF RICH RESULTS ---
            
            # Image Pack
            if soup.select_one('g-scrolling-carousel') or soup.select_one('div[data-attrid="images universal"]'):
                features['image_pack'] = True
                
            # Video Carousel
            if soup.select_one('video-voyager') or soup.select_one('div[data-attrid="videos universal"]'):
                features['video_carousel'] = True
                
            # News Box (Top Stories)
            if soup.select_one('div.F9rcV') or soup.select_one('g-section-with-header a[href*="news.google.com"]'):
                features['news_box'] = True
                
            # Shopping Results
            if soup.select_one('div.pla-unit-container') or soup.select_one('div[data-pla]'):
                features['shopping_results'] = True
                
            # Sitelinks (within organic results or standalone)
            if soup.select_one('div.HiHjCd') or soup.select_one('ul.lhsL7b') or soup.select_one('table.jmjoTe'):
                features['sitelinks'] = True
                
            # Reviews/Ratings (Star ratings in results)
            if soup.select_one('span.fG8Fp') or soup.select_one('span[aria-label*="rating"]'):
                features['reviews'] = True
                
        except Exception as e:
            print(f"[SerpParser] Error detecting features: {e}")
                
        except Exception as e:
            print(f"[SerpParser] Error detecting features: {e}")
        
        return features
    
    def _extract_organic_results(self, soup) -> list:
        """Extract organic search results using a link-first approach."""
        results = []
        seen_urls = set()
        rank = 0
        
        # Find the main search results container
        # [FIX] Check for combined_results first to handle multi-page crawls
        main_container = soup.select_one('#combined_results') or soup.select_one('#rso') or soup.select_one('#search') or soup
        
        # Find all external links that look like search results
        # These are typically in structure: parent has link + title + snippet
        all_links = main_container.select('a[href^="http"]')
        
        for link in all_links:
            try:
                url = link.get('href', '')
                
                # Skip Google internal links
                if not url or 'google.com' in url or 'google.co' in url:
                    continue
                    
                # Skip if already seen
                if url in seen_urls:
                    continue
                
                # Skip non-result links (images, small icons, etc.)
                # A result link typically has text content or contains an h3
                has_h3 = link.select_one('h3')
                link_text = link.get_text(strip=True)
                
                # Find parent container that represents the full result
                parent = link.parent
                for _ in range(5):  # Walk up to 5 levels
                    if parent is None:
                        break
                    parent_classes = parent.get('class', [])
                    if isinstance(parent_classes, list):
                        parent_class_str = ' '.join(parent_classes)
                    else:
                        parent_class_str = str(parent_classes)
                    
                    # Stop at common result containers
                    if any(c in parent_class_str for c in ['g', 'MjjYud', 'N54PNb', 'hlcw0c']):
                        break
                    parent = parent.parent
                
                if parent is None:
                    parent = link.parent
                
                # Check for ads
                is_ad = False
                parent_html = str(parent)[:1000].lower() # Increased window to catch indicators
                if 'ads' in parent_html or 'sponsored' in parent_html or 'data-text-ad' in parent_html:
                    is_ad = True
                    # Double check it's not a false positive by looking for specific ad elements
                    # Common ad indicators: 
                    # - span:contains("Sponsored")
                    # - div[data-text-ad]
                    # - class includes "uEierd" (common Google ad class)
                    
                    # If we aren't sure, we rely on the broad check
                
                # Extract title
                title = ''
                if has_h3:
                    title = has_h3.get_text(strip=True)
                else:
                    # Try to find h3 in parent
                    h3_in_parent = parent.select_one('h3') if parent else None
                    if h3_in_parent:
                        title = h3_in_parent.get_text(strip=True)
                    elif link_text and len(link_text) > 10:
                        title = link_text[:100]
                
                # Skip if no meaningful title
                if not title or len(title) < 5:
                    continue
                
                seen_urls.add(url)
                
                # Only increment rank for non-ads
                if not is_ad:
                    rank += 1
                
                # Extract snippet from parent
                snippet = self._extract_snippet(parent) if parent else ''
                
                # Extract displayed URL
                displayed_url = ''
                cite = parent.select_one('cite') if parent else None
                if cite:
                    displayed_url = cite.get_text(strip=True)
                else:
                    # Use domain from URL
                    from urllib.parse import urlparse
                    displayed_url = urlparse(url).netloc
                
                results.append({
                    'rank': rank if not is_ad else 0, # Ads get rank 0
                    'url': url,
                    'title': title,
                    'snippet': snippet,
                    'displayed_url': displayed_url,
                    'is_ad': is_ad
                })
                
            except Exception as e:
                continue
        
        return results
    
    def _is_ad_or_feature(self, item) -> bool:
        """Check if an item is an ad or special feature, not organic."""
        try:
            # Check for ad indicators
            item_text = str(item)
            if 'data-text-ad' in item_text:
                return True
            if 'sponsored' in item_text.lower():
                return True
            
            # Check for ad label
            ad_label = item.select_one('span:contains("Ad")')
            if ad_label:
                return True
            
            # Check classes
            classes = item.get('class', [])
            if isinstance(classes, list):
                class_str = ' '.join(classes).lower()
                if 'ad' in class_str or 'commercial' in class_str:
                    return True
                    
        except:
            pass
        
        return False
    
    def _extract_snippet(self, item) -> str:
        """Extract the snippet/description from a result."""
        try:
            # Try various snippet selectors
            snippet_selectors = [
                'div.VwiC3b',  # Common snippet class
                'div[data-sncf]',
                'span.aCOpRe',
                'div.IsZvec',
            ]
            
            for selector in snippet_selectors:
                snippet_elem = item.select_one(selector)
                if snippet_elem:
                    text = snippet_elem.get_text(strip=True)
                    if len(text) > 20:  # Reasonable snippet length
                        return text
            
            # Fallback: get text from item excluding title
            h3 = item.select_one('h3')
            if h3:
                h3.decompose()
            
            all_text = item.get_text(separator=' ', strip=True)
            # Return first 300 chars as snippet
            return all_text[:300] if len(all_text) > 300 else all_text
            
        except:
            pass
        
        return ''
    
    def _extract_displayed_url(self, item) -> str:
        """Extract the green displayed URL (breadcrumb)."""
        try:
            # Displayed URL is usually in a cite element or specific div
            cite = item.select_one('cite')
            if cite:
                return cite.get_text(strip=True)
            
            # Alternative: look for breadcrumb-style URL
            breadcrumb = item.select_one('div.TbwUpd')
            if breadcrumb:
                return breadcrumb.get_text(strip=True)
                
        except:
            pass
        
        return ''
    
    def _extract_local_pack(self, soup) -> list:
        """Extract businesses from the local pack / map results."""
        local_results = []
        
        try:
            # Local pack container selectors
            local_selectors = [
                'div.VkpGBb',  # Local result item
                'div[data-local-attribute]',
            ]
            
            for selector in local_selectors:
                items = soup.select(selector)
                
                for idx, item in enumerate(items[:3], 1):  # Usually 3 local results
                    try:
                        # Business name
                        name_elem = item.select_one('div.dbg0pd') or item.select_one('span.OSrXXb')
                        name = name_elem.get_text(strip=True) if name_elem else ''
                        
                        # Rating
                        rating_elem = item.select_one('span.yi40Hd')
                        rating = rating_elem.get_text(strip=True) if rating_elem else ''
                        
                        # Reviews count
                        reviews_elem = item.select_one('span.RDApEe')
                        reviews = reviews_elem.get_text(strip=True) if reviews_elem else ''
                        
                        # Category/type
                        category_elem = item.select_one('div.rllt__details span:first-child')
                        category = category_elem.get_text(strip=True) if category_elem else ''
                        
                        # [NEW] Detect if this is a sponsored/ad listing
                        is_ad = False
                        item_text = item.get_text().lower()
                        if 'sponsored' in item_text or 'ad' in item.get('class', []):
                            is_ad = True
                        # Also check for "Sponsored" in any span
                        sponsored_elem = item.find(lambda tag: tag.name == 'span' and 'Sponsored' in tag.get_text())
                        if sponsored_elem:
                            is_ad = True
                        
                        if name:
                            local_results.append({
                                'rank': idx,
                                'name': name,
                                'rating': rating,
                                'reviews': reviews,
                                'category': category,
                                'is_ad': is_ad,  # [NEW] Add ad flag
                            })
                            
                    except Exception as e:
                        continue
                
                if local_results:
                    break
                    
        except Exception as e:
            print(f"[SerpParser] Error extracting local pack: {e}")
        
        return local_results

    def _extract_extended_local_pack(self, soup) -> list:
        """Extract deeper business data from the Local Finder view (after clicking 'More places')."""
        local_results = []
        
        try:
            # Local Finder / Maps list items
            # Usually div[jscontroller="AtSb"] or div.rl_item inside a specific container
            items = soup.select('div[jscontroller="AtSb"]') or soup.select('div.rl_item')
            
            if not items:
                # Try finding by class if jscontroller is missing
                items = soup.select('div.VkpGBb')
            
            for idx, item in enumerate(items, 1):
                try:
                    # Business Name
                    name_elem = item.select_one('div.dbg0pd') or item.select_one('span.OSrXXb') or item.select_one('div[role="heading"]')
                    name = name_elem.get_text(strip=True) if name_elem else ''
                    
                    if not name:
                         continue

                    # Rating
                    rating_elem = item.select_one('span.yi40Hd') or item.select_one('span.MW4etd')
                    rating = rating_elem.get_text(strip=True) if rating_elem else ''
                    
                    # Reviews count
                    reviews_elem = item.select_one('span.RDApEe') or item.select_one('span.UY7F9')
                    reviews = reviews_elem.get_text(strip=True) if reviews_elem else ''
                    if reviews:
                        reviews = reviews.replace('(', '').replace(')', '').replace(',', '')
                    
                    # Category
                    category = ''
                    details_div = item.select_one('div.rllt__details')
                    if details_div:
                        # Category is often the first text node or span
                        cat_span = details_div.select_one('span')
                        if cat_span:
                            category = cat_span.get_text(strip=True)
                            
                    # Website URL
                    website = ''
                    # Look for explicit website action button
                    web_btn = item.select_one('a[aria-label="Website"]') or item.select_one('a:contains("Website")')
                    if web_btn:
                        website = web_btn.get('href', '')
                    else:
                        # Heuristic: Find first non-Google link
                        links = item.select('a[href^="http"]')
                        for link in links:
                            href = link.get('href', '')
                            # Skip Google Maps, Search, etc.
                            if 'google.com' in href or 'google.co' in href:
                                continue
                            
                            # Skip likely junk
                            if 'search?' in href:
                                continue
                                
                            # If it's an external link, it's likely the website
                            website = href
                            break
                            
                    # Address/Phone (sometimes extracted)
                    
                    local_results.append({
                        'rank': idx,
                        'name': name,
                        'rating': rating,
                        'reviews': reviews,
                        'category': category,
                        'website': website,
                        'type': 'extended'
                    })
                            
                except Exception as e:
                    continue
            
        except Exception as e:
            print(f"[SerpParser] Error extracting extended local pack: {e}")
            
        return local_results

    def _extract_shopping_results(self, soup) -> list:
        """Extract product data from the Shopping Tab view."""
        shopping_results = []
        
        try:
            # Shopping Grid Items
            # Selectors vary: .sh-dgr__content, .sh-np__click-target, or data-docid containers
            # [FIX] Added 'div.sh-dgr__grid-result' and others as common containers
            items = soup.select('div.sh-dgr__content, div[data-docid], div.sh-dgr__grid-result, div.KZmu8e, div.i0X6df')
            
            # --- HEURISTIC FALLBACK ---
            if not items:
                print("[SerpParser] No standard items found. Attempting heuristic fallback...")
                # Find all elements that look like a price
                import re
                price_pattern = re.compile(r'(\$|€|₹|Rs\.|USD)\s?[\d,]+\.?\d*')
                
                # Find elements with text matching price
                price_elems = soup.find_all(['span', 'div'], string=price_pattern)
                
                potential_parents = set()
                for p in price_elems:
                    # Walk up to find a container that has an image
                    parent = p.parent
                    for _ in range(6): # Walk up 6 levels
                        if parent:
                            # Must have image AND significant text checks
                            if parent.select_one('img'):
                                txt = parent.get_text(strip=True)
                                if len(txt) > 20 and len(txt) < 500: # manageable text length
                                    potential_parents.add(parent)
                                    # Don't break, allow finding larger containers
                            parent = parent.parent
                        else:
                            break
                            
                # Filter: keep only "distinct" cards (remove parents if children are already candidates)
                # Sort by text length, keep smallest valid containers
                candidates = sorted(list(potential_parents), key=lambda x: len(x.get_text(strip=True)))
                
                # Deduplicate: if A is in B, keep A (smaller)
                final_candidates = []
                for cand in candidates:
                     is_parent_of_existing = False
                     for existing in final_candidates:
                         if existing in cand.descendants:
                             is_parent_of_existing = True
                             break
                     if not is_parent_of_existing:
                         final_candidates.append(cand)
                         
                items = final_candidates
                print(f"[SerpParser] Heuristic found {len(items)} candidates (filtered from {len(potential_parents)})")

            print(f"[SerpParser] Processing {len(items)} items...")
            
            seen_titles = set()
            
            for idx, item in enumerate(items, 1):
                try:
                    full_text = item.get_text(strip=True)
                    
                    # Title Extraction
                    title = ''
                    # Try specific headings first
                    title_elem = item.select_one('h3') or item.select_one('.tAxDx') or item.select_one('.IuHnof') or item.select_one('.ei2ZRb')
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                    else:
                        # Heuristic: Match text that looks like a title (start of string, reasonable length)
                        # Split by newline or common separators
                        parts = [p.strip() for p in item.get_text(separator='\n').split('\n') if p.strip()]
                        for part in parts:
                            if len(part) > 10 and len(part) < 100 and '$' not in part and '₹' not in part:
                                title = part
                                break
                                
                    if not title or len(title) < 3:
                        continue
                        
                    if title in seen_titles:
                        continue
                    seen_titles.add(title)
                    
                    # Price
                    price = ''
                    import re
                    price_matches = re.findall(r'(₹|Rs\.|USD|\$)\s?[\d,]+', full_text)
                    if price_matches:
                        # Try to find the price element specifically
                        price_elem = item.select_one('span.a8Pemb') or item.select_one('span[aria-hidden="true"]')
                        if price_elem:
                            price = price_elem.get_text(strip=True)
                        else:
                            # Find the matching string in text
                             # We assume the first price match is the main price if multiple exist
                             for match in price_matches:
                                 if match in full_text:
                                     price = match
                                     break

                    if not price:
                        continue # Skip items without price
                            
                    # Merchant / Source
                    source = ''
                    source_elem = item.select_one('.aULzUe') or item.select_one('.IuHnof')
                    if source_elem:
                        source = source_elem.get_text(strip=True)
                    else:
                        # Heuristic: Find known merchants
                        for merch in ['Amazon', 'Flipkart', 'Myntra', 'Meesho', 'Croma', 'Reliance Digital', 'eBay', 'Target', 'Best Buy', 'Walmart', 'Google']:
                            if merch in full_text:
                                source = merch
                                break
                    
                    # Rating & Reviews
                    rating = ''
                    reviews = ''
                    # ... (keep existing logic) ...
                    rating_span = item.select_one('div[aria-label*="star"]') or item.select_one('span[aria-label*="star"]')
                    if rating_span:
                        aria = rating_span.get('aria-label', '')
                        rating_match = re.search(r'(\d+(\.\d+)?) star', aria)
                        if rating_match:
                            rating = rating_match.group(1)
                        reviews_match = re.search(r'([\d,]+) review', aria)
                        if reviews_match:
                            reviews = reviews_match.group(1)

                    # Image
                    image = ''
                    img_tag = item.select_one('div[role="img"] img') or item.select_one('img')
                    if img_tag:
                        image = img_tag.get('src', '')
                        if 'data:image' in image:
                             if img_tag.get('data-src'): image = img_tag.get('data-src')
                             elif img_tag.get('data-url'): image = img_tag.get('data-url')
                    
                    if not image:
                         # Try finding image in style
                         style_div = item.select_one('div[style*="url("]')
                         if style_div:
                             style = style_div.get('style', '')
                             if 'url(' in style:
                                 image = style.split('url(')[1].split(')')[0].replace('"', '')

                    # Delivery
                    delivery = ''
                    if 'Free delivery' in full_text:
                        delivery = 'Free delivery'

                    result_item = {
                        'rank': len(shopping_results) + 1,
                        'title': title,
                        'price': price,
                        'source': source,
                        'rating': rating,
                        'reviews': reviews,
                        'image': image,
                        'delivery': delivery
                    }
                    shopping_results.append(result_item)
                    
                    if idx <= 3:
                        print(f"[SerpParser] Debug Item {idx}: {title[:30]}... | {price} | img={bool(image)}")
                    
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error parsing shopping results: {e}")
            
        return shopping_results[:50]
