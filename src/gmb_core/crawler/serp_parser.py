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
        'ai_overview': False,  # [NEW] AI Overview (SGE)
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
            'ai_overview': None, # [NEW] Structured AI Overview data
        }
        
        # Extract total results count
        result['total_results'] = self._extract_total_results(soup)
        
        # Detect SERP features
        result['serp_features'] = self._detect_serp_features(soup)

        # Extract AI Overview content if detected
        if result['serp_features']['ai_overview']:
            result['ai_overview'] = self._extract_ai_overview(soup)
        
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
        """Detect which SERP features are present on the page using modern selectors."""
        features = dict(self.SERP_FEATURES)
        
        try:
            # Helper to check text content
            text_content = soup.get_text().lower()
            html_str = str(soup)
            
            # --- AI OVERVIEW (SGE) ---
            # Experimental / New Selectors
            # --- AI OVERVIEW (SGE) ---
            # Check for AI Overview using robust text search and class names
            
            # 1. Check for common classes/attributes
            ai_present = False
            if soup.select_one('div.GenerativeAI') or soup.select_one('div[data-attrid="ai_overview"]'):
                ai_present = True
            
            # 2. Check for text "AI Overview" or "Generative AI" in headers/spans if NOT yet found
            if not ai_present:
                 # Look for "AI Overview" in headings or spans
                 if soup.find(lambda tag: tag.name in ['h1', 'h2', 'span', 'div'] and tag.get_text(strip=True) == "AI Overview"):
                     ai_present = True
                 elif soup.find(lambda tag: "Generative AI" in tag.get_text() and tag.name in ['span', 'div']):
                     ai_present = True

            if ai_present:
                features['ai_overview'] = True
            
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
        
        return features

    def _extract_ai_overview(self, soup) -> dict:
        """Extract content from AI Overview (SGE)."""
        ai_data = {
            'present': True,
            'text': '',
            'citations': [],
            'has_code': False
        }
        
        try:
            # Try to find the container
            # SGE containers are dynamic, but often have 'Generative' in class or specific structure
            container = None
            
            # 1. Look for 'AI Overview' header container
            header = soup.find(lambda tag: tag.name in ['h1', 'h2', 'span', 'div'] and "AI Overview" == tag.get_text(strip=True))
            if header:
                # Walk up to find the main block
                container = header.find_parent('div', class_=lambda c: c and ('M8OgIe' in c or 'Generative' in c)) or header.parent.parent
            
            if not container:
                # 2. Look for wrapper with data-attrid
                container = soup.select_one('div[data-attrid="ai_overview"]')
                
            if not container:
                # 3. Fallback: detection found "Generative AI" somewhere else? find that wrapper
                gen_ai_label = soup.find(lambda tag: "Generative AI" in tag.get_text() and tag.name in ['span', 'div'])
                if gen_ai_label:
                     container = gen_ai_label.find_parent('div', class_=lambda c: c and ('M8OgIe' in c)) or gen_ai_label.parent.parent

            if not container:
                # If we detected it but can't find container, mark as present but empty (user sees "Triggered" but no info)
                # Or we can return nothing? 
                # Let's trust detection, but if no container, we can't show text.
                return ai_data
                
            # Extract Text
            # Usually in paragraph blocks or div with specific class
            text_blocks = container.select('div[data-attrid="ai_overview"] > div') or container.select('p') or container.select('div.M8OgIe')
            full_text = []
            for block in text_blocks:
                text = block.get_text(strip=True)
                if len(text) > 20 and "AI Overview" not in text and "Generative AI" not in text: 
                    full_text.append(text)
            
            ai_data['text'] = ' '.join(full_text[:5]) # First few paragraphs
            
            # Extract Citations (Links in the AI block)
            links = container.select('a[href^="http"]')
            for link in links:
                href = link.get('href')
                # Skip Google links
                if 'google.com' in href: continue
                
                title = link.get_text(strip=True)
                domain = urlparse(href).netloc
                
                ai_data['citations'].append({
                    'url': href,
                    'title': title,
                    'domain': domain
                })
            
            # Deduplicate citations
            unique_citations = []
            seen_urls = set()
            for c in ai_data['citations']:
                if c['url'] not in seen_urls:
                    seen_urls.add(c['url'])
                    unique_citations.append(c)
            ai_data['citations'] = unique_citations
            
            # Check for code blocks
            if container.select('code') or container.select('pre'):
                ai_data['has_code'] = True
                
        except Exception as e:
            print(f"[SerpParser] Error extracting AI overview: {e}")
            
        return ai_data
    
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
        """Extract businesses from the local pack / map results with detailed info."""
        local_results = []
        
        try:
            # Local pack container selectors - try multiple approaches
            local_selectors = [
                'div.VkpGBb',  # Local result item
                'div[data-local-attribute]',
                'div.rllt__wrapped',  # Wrapped local result
            ]
            
            items = []
            for selector in local_selectors:
                items = soup.select(selector)
                if items:
                    break
            
            # If still no items, try a broader search
            if not items:
                # Look for containers with business names
                items = soup.select('div.dbg0pd, span.OSrXXb')
                items = [item.find_parent('div', class_=lambda x: x and 'VkpGBb' in str(x)) or item.parent.parent for item in items if item]
            
            seen_names = set()
            
            for idx, item in enumerate(items[:20], 1):  # Limit to 20 results
                try:
                    if item is None:
                        continue
                    
                    # === BUSINESS NAME ===
                    name_elem = item.select_one('div.dbg0pd') or item.select_one('span.OSrXXb') or item.select_one('div[role="heading"]')
                    name = name_elem.get_text(strip=True) if name_elem else ''
                    
                    # === CLEAN NAME ===
                    if name.endswith("My Ad Center"):
                        name = name.replace("My Ad Center", "").strip()
                    if name.endswith("Sponsored"):
                        name = name.replace("Sponsored", "").strip()
                    
                    if not name or name in seen_names:
                        continue
                    seen_names.add(name)
                    
                    # === RATING ===
                    rating = ''
                    rating_elem = item.select_one('span.yi40Hd') or item.select_one('span.MW4etd')
                    if rating_elem:
                        rating = rating_elem.get_text(strip=True)
                    else:
                        # Try aria-label
                        star_elem = item.select_one('span[aria-label*="star"]')
                        if star_elem:
                            import re
                            match = re.search(r'(\d+\.?\d*)\s*star', star_elem.get('aria-label', ''))
                            if match:
                                rating = match.group(1)
                    
                    # === REVIEWS COUNT ===
                    reviews = ''
                    reviews_elem = item.select_one('span.RDApEe') or item.select_one('span.UY7F9')
                    if reviews_elem:
                        reviews = reviews_elem.get_text(strip=True)
                        reviews = reviews.replace('(', '').replace(')', '').replace(',', '')
                    else:
                        # Try aria-label
                        review_elem = item.select_one('span[aria-label*="review"]')
                        if review_elem:
                            import re
                            match = re.search(r'([\d,]+)\s*review', review_elem.get('aria-label', ''))
                            if match:
                                reviews = match.group(1).replace(',', '')
                    
                    # === CATEGORY ===
                    category = ''
                    details_div = item.select_one('div.rllt__details') or item.select_one('div.rllt__wrapped')
                    if details_div:
                        # Category is often the first span or text
                        spans = details_div.select('span')
                        for span in spans:
                            text = span.get_text(strip=True)
                            # Skip ratings, hours, etc
                            if text and not text.startswith('(') and '·' not in text and len(text) < 50:
                                if not any(c.isdigit() for c in text[:3]):  # Skip if starts with numbers (like phone)
                                    category = text
                                    break
                    
                    # === ADDRESS (Short) ===
                    address = ''
                    # Look for address patterns in full text if specific element fails
                    full_text = item.get_text(separator=' | ')
                    clean_text = item.get_text(separator=' ').replace('\n', ' ')
                    
                    # Try to find address element first (specific class)
                    address_elem = item.select_one('span.AcEdkd') or item.select_one('div.rllt__details span:nth-of-type(2)')
                    if address_elem:
                        address = address_elem.get_text(strip=True)
                    else:
                        # Fallback: Parse from full text
                        parts = full_text.split('|')
                        for part in parts:
                            part = part.strip()
                            if not part or part == name or part == category: continue
                            
                            # Address patterns: contains comma OR ends with Rd/St/Ave etc OR has numbers
                            if ',' in part or re.search(r'\b(Rd|St|Ave|Dr|Blvd|Ln|Way|Cir|Pl|Hwy)\b', part, re.I):
                                if len(part) < 100 and "Open" not in part and "Closed" not in part:
                                    # Ensure it's not a review count or similar
                                    if not re.search(r'\d+\s*(review|year)', part, re.I):
                                        address = part
                                        break
                    
                    # === PHONE ===
                    phone = ''
                    phone_elem = item.select_one('span[data-dtype="d3ph"]') or item.select_one('a[href^="tel:"]')
                    if phone_elem:
                        phone = phone_elem.get_text(strip=True)
                    else:
                        # Extract phone from text using regex
                        phone_matches = re.findall(r'(?:(?:\+|00)91[\s.-]?)?(?:0?[6-9]\d{4}[\s.-]?\d{5}|(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4})', clean_text)
                        if not phone_matches:
                             phone_matches = re.findall(r'\b0\d{5}\s\d{5}\b', clean_text)
                        
                        if phone_matches:
                            phone = phone_matches[0]
                    
                    # === TIMINGS (Open/Closed status) ===
                    timings = ''
                    status_match = re.search(r'\b(Open|Closed|Closes soon)\b(?:[ ·•-]*)(?:Closes|Opens)?\s*(?:24 hours|\d{1,2}(?::\d{2})?\s*(?:am|pm|AM|PM|noon|midnight)?)', clean_text)
                    if status_match:
                        timings = status_match.group(0).replace('·', '').strip()
                        timings = re.sub(r'\s+', ' ', timings)
                    
                    # === HIGHLIGHTS/FEATURES ===
                    highlights = []
                    # Look for special features
                    feature_patterns = [
                        r'(In-store shopping|Dine-in|Takeout|Delivery|Curbside pickup)',
                        r'(Free Wi-Fi|WiFi|Wheelchair accessible)',
                        r'(MDS|Gold Medalist|Specialist|Board Certified)',
                        r'(\d+\+?\s*(?:years|implants|patients|reviews|locations))',
                        r'\b(Available for new patients)\b',
                    ]
                    for pattern in feature_patterns:
                        matches = re.findall(pattern, full_text, re.I)
                        highlights.extend(matches)
                    
                    # Quotes for reviews
                    quote_matches = re.findall(r'"([^"]{10,60})"', clean_text)
                    for q in quote_matches:
                        if q not in highlights:
                             highlights.append(f'"{q}"')
                    
                    # Also check for service tags/chips
                    tags = item.select('span.hGz87c, span.LrzXr, span.YhemCb')
                    for tag in tags:
                        tag_text = tag.get_text(strip=True)
                        if tag_text and len(tag_text) < 50:
                            highlights.append(tag_text)
                    
                    # Dedupe highlights
                    highlights = list(dict.fromkeys(highlights))[:5]  # Keep max 5
                    
                    # === WEBSITE URL ===
                    website = ''
                    web_link = item.select_one('a[href^="http"]:not([href*="google."])')
                    if web_link:
                        website = web_link.get('href', '')
                    
                    # === IS AD ===
                    is_ad = False
                    item_html = str(item).lower()
                    if 'sponsored' in item_html or 'data-text-ad' in item_html:
                        is_ad = True
                    sponsored_elem = item.find(lambda tag: tag.name == 'span' and 'Sponsored' in tag.get_text())
                    if sponsored_elem:
                        is_ad = True
                    
                    local_results.append({
                        'rank': len(local_results) + 1,
                        'name': name,
                        'rating': rating,
                        'reviews': reviews,
                        'category': category,
                        'address': address,
                        'phone': phone,
                        'timings': timings,
                        'highlights': ', '.join(highlights) if highlights else '',
                        'website': website,
                        'is_ad': is_ad,
                    })
                            
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"[SerpParser] Error extracting local pack: {e}")
        
        return local_results

    def _extract_extended_local_pack(self, soup) -> list:
        """Extract deeper business data from the Local Finder view (after clicking 'More places')."""
        local_results = []
        
        try:
            # Local Finder / Maps list items
            items = soup.select('div[jscontroller="AtSb"]') or soup.select('div.rl_item')
            
            if not items:
                items = soup.select('div.VkpGBb')
            
            seen_names = set()
            
            for idx, item in enumerate(items, 1):
                try:
                    # Business Name
                    name_elem = item.select_one('div.dbg0pd') or item.select_one('span.OSrXXb') or item.select_one('div[role="heading"]')
                    name = name_elem.get_text(strip=True) if name_elem else ''
                    
                    # Full text for parsing additional fields - use pipe separator for better delimiting
                    full_text = item.get_text(separator=' | ')
                    clean_text = item.get_text(separator=' ').replace('\n', ' ')
                    
                    # === CLEAN NAME ===
                    # User reported "NameMy Ad Center". Clean this specific artifact.
                    if name.endswith("My Ad Center"):
                        name = name.replace("My Ad Center", "").strip()
                    if name.endswith("Sponsored"):
                        name = name.replace("Sponsored", "").strip()

                    if not name or name in seen_names:
                         continue
                    seen_names.add(name)

                    # Rating
                    rating = ''
                    rating_elem = item.select_one('span.yi40Hd') or item.select_one('span.MW4etd')
                    if rating_elem:
                        rating = rating_elem.get_text(strip=True)
                    
                    # Reviews count
                    reviews = ''
                    reviews_elem = item.select_one('span.RDApEe') or item.select_one('span.UY7F9')
                    if reviews_elem:
                        reviews = reviews_elem.get_text(strip=True)
                        reviews = reviews.replace('(', '').replace(')', '').replace(',', '')
                    
                    # Category
                    category = ''
                    details_div = item.select_one('div.rllt__details')
                    if details_div:
                        cat_span = details_div.select_one('span')
                        if cat_span:
                            category = cat_span.get_text(strip=True)

                    # === PHONE ===
                    phone = ''
                    # Priority 1: Link with tel: - Extract from HREF not text
                    phone_link = item.select_one('a[href^="tel:"]')
                    if phone_link:
                        href = phone_link.get('href', '')
                        if href.startswith('tel:'):
                            phone = href.replace('tel:', '').strip()
                        else:
                            phone = phone_link.get_text(strip=True)
                    
                    if not phone or len(phone) < 7: # If phone is "Call" or empty
                        # Priority 2: Text regex - Relaxed & Localized
                        import re
                        # Clean text for phone extraction
                        # 1. Indian/International: +91 97134 35111, 097134 35111
                        # 2. US: (617) 555-0199, 617-555-0199
                        
                        # Match Indian patterns first
                        phone_matches = re.findall(r'(?:(?:\+|00)91[\s.-]?)?(?:0?[6-9]\d{4}[\s.-]?\d{5})', clean_text)
                        
                        if not phone_matches:
                            # Match US/Standard patterns
                            # (ddd) ddd-dddd or ddd-ddd-dddd
                            phone_matches = re.findall(r'(?:\(\d{3}\)|\d{3})[\s.-]\d{3}[\s.-]\d{4}', clean_text)
                        
                        if not phone_matches:
                             # Fallback: specific "0xxxxx xxxxx" 
                             phone_matches = re.findall(r'\b0\d{5}\s\d{5}\b', clean_text)

                        if phone_matches:
                            phone = phone_matches[0]

                    # === TIMINGS ===
                    timings = ''
                    # Pattern: "Open · Closes 9 pm", "Open 24 hours"
                    # We match "Open" or "Closed" or "Closes"
                    status_match = re.search(r'\b(Open|Closed|Closes soon|Opens soon)\b(?:.*?)((?:Closes|Opens)\s.*?(?:AM|PM|am|pm|24 hours|\d{1,2}(?::\d{2})?)?)', clean_text)
                    if status_match:
                         # Use the whole match but clean it
                         raw = status_match.group(0)
                         # Validate it looks like time
                         if re.search(r'\d|Open|Closed', raw):
                              timings = raw.replace('·', '').replace('  ', ' ').strip()
                              if len(timings) > 40: timings = timings[:40] + "..."

                    # === ADDRESS ===
                    address = ''
                    parts = full_text.split('|')
                    for part in parts:
                        part = part.strip()
                        if not part or part == name or part == category or (phone and phone in part and len(part) < len(phone)+5): continue
                        if part == rating or part == reviews: continue
                        if timings and part in timings: continue
                        
                        # Check if part contains the phone we found? "Address · Phone"
                        if phone and (phone in part or re.search(r'\d{3}[-]\d{4}', part)):
                             # Split by dot/bullet
                             subparts = re.split(r'[·•]', part)
                             for sp in subparts:
                                 sp = sp.strip()
                                 # Take the part that is NOT the phone and looks like address
                                 if len(sp) > 5 and not re.search(r'\d{3}[-]\d{4}', sp) and phone not in sp:
                                     # Not "Open", not "Reviews"
                                     if "Open" not in sp and "review" not in sp.lower():
                                         address = sp
                                         break
                             if address: break

                        # Heuristic-based
                        street_suffixes = r'\b(St|Rd|Ave|Blvd|Ln|Dr|Way|Ct|Pl|Terr|Circle|Sq|Pkwy|Hwy|Street|Road|Avenue|Lane|Drive|Marg|Chowk|Nagar|Colony|Society|Complex|Plaza|Heights|Apartment|Floor|Suite|Ste)\b'
                        if ',' in part or re.search(street_suffixes, part, re.I):
                             if "review" not in part.lower() and " mi" not in part and len(part) < 120 and "Open" not in part:
                                 address = part
                                 break

                    # === HIGHLIGHTS ===
                    highlights = []
                    # 1. Known attributes regex
                    feature_patterns = [
                        r'\b(In-store shopping)\b',
                        r'\b(In-store pickup)\b', 
                        r'\b(Curbside pickup)\b',
                        r'\b(Delivery)\b',
                        r'\b(Dine-in)\b',
                        r'\b(Takeaway)\b',
                        r'\b(No-contact delivery)\b',
                        r'\b(Online appointments)\b',
                        r'\b(On-site services)\b',
                        r'\b(Available for new patients)\b',
                    ]
                    for pattern in feature_patterns:
                        if re.search(pattern, clean_text, re.I):
                             match = re.search(pattern, clean_text, re.I).group(1)
                             if match not in highlights:
                                 highlights.append(match)
                    
                    # 2. Quotes (reviews/highlights often in quotes)
                    quote_matches = re.findall(r'"([^"]{10,60})"', clean_text)
                    for q in quote_matches:
                        if q not in highlights:
                             highlights.append(f'"{q}"')

                    # Service tags (chips) - usually in specific spans
                    tags = item.select('span.hGz87c, span.LrzXr, div.I9NHl')
                    for tag in tags:
                        tag_text = tag.get_text(strip=True)
                        if tag_text and len(tag_text) < 40 and tag_text not in highlights and tag_text != category:
                             # Filter out junk
                             if "Open" not in tag_text and "Closes" not in tag_text and "review" not in tag_text:
                                highlights.append(tag_text)
                            
                    highlights = list(dict.fromkeys(highlights))[:4]
                            
                    # Website URL
                    website = ''
                    web_btn = item.select_one('a[aria-label="Website"]') or item.select_one('a:contains("Website")')
                    if web_btn:
                        website = web_btn.get('href', '')
                    else:
                        links = item.select('a[href^="http"]')
                        for link in links:
                            href = link.get('href', '')
                            if 'google.com' not in href and 'google.co' not in href and 'search?' not in href:
                                website = href
                                break
                    
                    local_results.append({
                        'rank': len(local_results) + 1,
                        'name': name,
                        'rating': rating,
                        'reviews': reviews,
                        'category': category,
                        'address': address,
                        'phone': phone,
                        'timings': timings,
                        'highlights': ', '.join(highlights) if highlights else '',
                        'website': website,
                        'is_ad': False,
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
