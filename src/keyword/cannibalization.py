"""
Keyword Cannibalization Detection (COMPLETELY REFINED)
Identifies TRUE keyword cannibalization using proper SEO principles.

CORE PRINCIPLE: 
Google asks "Which ONE page best satisfies this search query?"
NOT "Do these pages share words?"

TRUE CANNIBALIZATION = Same primary keyword target + Same intent + Same purpose
SEMANTIC OVERLAP = Expected and beneficial (not a problem)
"""

import asyncio
import logging
from typing import Optional, List, Dict, Set, Tuple
from collections import defaultdict
from urllib.parse import urlparse, urljoin
import requests
import re
import time
from bs4 import BeautifulSoup

from .keyword_analyzer import KeywordDensityAnalyzer
from .ai_service import GeminiKeywordAI

logger = logging.getLogger(__name__)


class KeywordCannibalizationDetector:
    """
    Detects TRUE keyword cannibalization with strict SEO-correct rules.
    
    New Rules:
    1. EXCLUDE all system/utility/profile pages completely
    2. STRICT NAP filtering (phone, address, zip codes)
    3. PRIMARY KEYWORD focus only (not every word on page)
    4. PAGE TYPE awareness (Service vs Blog vs Profile vs Review)
    5. INTENT + PURPOSE matching required for flagging
    6. SMART ACTIONS (never suggest harmful 301s)
    """
    
    # Comprehensive exclusion patterns
    EXCLUDED_URL_PATTERNS = [
        # System pages (NEVER analyze these)
        r'/wp-content/', r'/wp-includes/', r'/wp-admin/', r'/feed/',
        r'/login', r'/register', r'/signup', r'/cart', r'/checkout', 
        r'/account', r'/dashboard', r'/profile-settings',
        
        # Legal/Utility (NEVER 301 these)
        r'/privacy', r'/terms', r'/disclaimer', r'/policy', r'/legal',
        r'/cookie', r'/gdpr', r'/accessibility',
        
        # System responses
        r'/thank-you', r'/confirmation', r'/success', r'/error',
        r'/404', r'/500', r'/maintenance',
        
        # Contact/Forms (Different purpose, not content)
        r'/contact', r'/appointment', r'/book-now', r'/schedule',
        r'/inquiry', r'/request', r'/consultation', r'/get-started',
        
        # Reviews/Testimonials (Reputation, not service targeting)
        r'/reviews/?$', r'/testimonials/?$', r'/feedback/?$',
        r'/case-studies/?$', r'/patient-stories/?$',
        
        # Resource centers/indexes (Archive pages, not content)
        r'/learning-center/?$', r'/news-events/?$', r'/blog/?$',
        r'/resources/?$', r'/library/?$', r'/archive/?$',
        
        # Design/Development
        r'/design', r'/style-guide', r'/pattern-library', r'/demo',
        
        # Archives (Category/Tag pages, not content)
        r'/category/', r'/tag/', r'/author/', r'/date/',
        r'/blog/page/', r'/page/\d+/',
        
        # Date-based URLs (Archive pages)
        r'/20\d{2}/', r'/\d{4}/\d{2}/',
        r'/(january|february|march|april|may|june|july|august|september|october|november|december)/',
        
        # Parameters
        r'format=', r'feed=', r'utm_', r'ref=', r'share=', r'print=',
        
        # Media
        r'\.(jpg|jpeg|png|gif|pdf|doc|docx|zip)$',
    ]
    
    # NAP (Name, Address, Phone) patterns - these are EXPECTED on multiple pages
    NAP_PATTERNS = [
        # Phone numbers (any format)
        r'^\d{3}[\s\-\.]?\d{3,4}[\s\-\.]?\d{4}$',  # 978-851-7890, 978 851 7890
        r'^\d{3}\s*\d{3,4}$',  # 978 851, partial phone
        r'^\(\d{3}\)\s*\d{3}[\s\-]?\d{4}$',  # (978) 851-7890
        r'^1?\s*\d{3}[\s\-\.]?\d{3}[\s\-\.]?\d{4}$',  # With or without country code
        
        # Addresses
        r'^\d+\s+[a-z]+\s+(st|street|ave|avenue|rd|road|blvd|boulevard|lane|ln|drive|dr|way|ct|court|place|pl)\.?$',
        r'^\d+\s+main\s*(st|street)?$',  # "1438 main", "1438 main street"
        
        # Zip codes
        r'^\d{5}(-\d{4})?$',  # 01876, 01876-1234
        
        # City, State combinations
        r'^[a-z\s]+,\s*[a-z]{2}\s*\d{5}$',  # "Tewksbury, MA 01876"
        
        # Business hours
        r'^(mon|tue|wed|thu|fri|sat|sun)[a-z]*[\s\-:]\d',
        
        # Pure numbers (years, counts, etc.)
        r'^\d{1,4}$',  # 2024, 978, etc.
        
        # Common NAP components
        r'^(tel|fax|ph|phone|call|email|hours):?\s*',
    ]
    
    # Common branded terms that shouldn't be flagged as keywords
    BRANDED_NOISE = [
        'dental', 'dentist', 'dentistry', 'care', 'office', 'practice',
        'doctor', 'dr', 'clinic', 'center', 'associates', 'group',
        'family', 'general', 'cosmetic', 'pediatric', 'orthodontic',
        # These are too generic - only flag if part of specific phrase
    ]

    def __init__(self, ai_service: Optional[GeminiKeywordAI] = None):
        """Initialize the detector."""
        self.ai_service = ai_service or GeminiKeywordAI()
        self.analyzer = KeywordDensityAnalyzer(self.ai_service)
        
    async def close(self):
        """Close resources."""
        await self.analyzer.close()
    
    def _is_excluded_url(self, url: str) -> bool:
        """
        Check if URL should be excluded from cannibalization analysis.
        Returns True if this is a system/utility/profile page.
        """
        parsed = urlparse(url)
        path = parsed.path.lower()
        query = parsed.query.lower()
        
        full_str = path + ('?' + query if query else '')
        
        for pattern in self.EXCLUDED_URL_PATTERNS:
            if re.search(pattern, full_str, re.IGNORECASE):
                logger.debug(f"Excluding {url} - matched pattern: {pattern}")
                return True
        return False

    def _is_nap_or_noise(self, keyword: str) -> bool:
        """
        Check if keyword is NAP data, branded noise, or not a real keyword.
        
        NAP data is EXPECTED on multiple pages and should NEVER be flagged.
        """
        kw = keyword.lower().strip()
        
        # Empty or too short
        if len(kw) < 3:
            return True
        
        # Single generic branded word
        if kw in self.BRANDED_NOISE:
            return True
        
        # Check against all NAP patterns
        for pattern in self.NAP_PATTERNS:
            if re.match(pattern, kw, re.IGNORECASE):
                logger.debug(f"Filtering NAP/noise: {keyword}")
                return True
        
        # Additional checks
        # Pure numbers
        if kw.replace(' ', '').replace('-', '').replace('.', '').isdigit():
            return True
        
        # Common stop words that got through
        stop_words = {'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
                     'of', 'with', 'by', 'from', 'an', 'as', 'is', 'was', 'are'}
        if kw in stop_words:
            return True
            
        return False

    def _detect_page_type(self, url: str, title: str = "", h1: str = "") -> str:
        """
        Detect page type based on URL structure and content.
        
        Returns:
            'Service': Service/treatment pages (main content pages)
            'Blog': Blog posts/articles (informational content)
            'Profile': Team/doctor profile pages (E-E-A-T)
            'Review': Review/testimonial pages (reputation)
            'Location': Location/contact pages (NAP)
            'System': System/utility pages
            'Homepage': Homepage
        """
        path = urlparse(url).path.lower()
        title_lower = title.lower()
        h1_lower = h1.lower()
        
        # Homepage
        if path in ['/', '', '/index.html', '/home']:
            return 'Homepage'
        
        # System pages (shouldn't reach here due to exclusions, but safety check)
        system_indicators = ['/privacy', '/terms', '/policy', '/thank-you', 
                           '/contact', '/appointment', '/book']
        if any(ind in path for ind in system_indicators):
            return 'System'
        
        # Team/Doctor profiles (E-E-A-T signals, NOT service competition)
        profile_indicators = [
            '/doctor/', '/dr-', '/dentist/', '/team/', '/our-team/',
            '/meet-', '/about-dr', '/specialist/', '/our-doctors/',
            '/staff/', '/hygienist/', '/orthodontist/'
        ]
        if any(ind in path for ind in profile_indicators):
            return 'Profile'
        
        # Check content for profile indicators
        profile_content = ['meet dr', 'about dr', 'dr. ', 'biography', 
                         'education', 'credentials', 'experience']
        if any(ind in title_lower or ind in h1_lower for ind in profile_content):
            return 'Profile'
        
        # Blog/Articles (informational content, can share keywords with services)
        blog_indicators = [
            '/blog/', '/news/', '/articles/', '/post/', '/insights/',
            '/tips/', '/guide/', '/how-to/', '/learn/', '/education/',
            '/faq/', '/questions/', '/learning-center/'
        ]
        if any(ind in path for ind in blog_indicators):
            return 'Blog'
        
        # Check for date-based URLs (blog posts)
        if re.search(r'/\d{4}/\d{2}/', path):
            return 'Blog'
        
        # Reviews/Testimonials (reputation signals, NOT service targeting)
        review_indicators = ['/review', '/testimonial', '/case-stud', 
                           '/patient-stor', '/success-stor']
        if any(ind in path for ind in review_indicators):
            return 'Review'
        
        # Location pages (NAP data, NOT service targeting)
        location_indicators = ['/location', '/directions', '/find-us', 
                             '/office', '/address']
        if any(ind in path for ind in location_indicators):
            return 'Location'
        
        # Service pages (main treatment/service pages)
        service_indicators = [
            '/service/', '/treatment/', '/procedure/', '/care/',
            '/implant', '/crown', '/veneer', '/whitening',
            '/orthodontic', '/cosmetic', '/restorative',
            '/emergency', '/extraction', '/root-canal'
        ]
        if any(ind in path for ind in service_indicators):
            return 'Service'
        
        # Default: If it's a content page that wasn't excluded, treat as Service
        # This is safer than marking everything as "Other"
        return 'Service'

    def _is_primary_keyword_target(self, keyword: str, page_data: Dict) -> bool:
        """
        Determine if a keyword is a PRIMARY target for this page.
        
        A keyword is primary if:
        - It appears in Title AND/OR H1
        - It has high density (>1.5%) OR high prominence score
        - It's not just incidental mention
        
        This prevents flagging every word on a page as a "keyword target".
        """
        title = page_data.get('title', '').lower()
        h1 = page_data.get('h1', '').lower()
        
        kw_lower = keyword.lower()
        
        # Must be in title or H1 to be considered primary
        in_title = kw_lower in title
        in_h1 = kw_lower in h1
        
        if not (in_title or in_h1):
            return False
        
        # Check density and prominence from keyword data
        for kw_obj in page_data.get('keywords', []):
            if kw_obj['keyword'].lower() == kw_lower:
                density = kw_obj.get('density', 0)
                prominence = kw_obj.get('prominence_score', 0)
                
                # High density or prominence indicates primary focus
                if density > 1.5 or prominence > 50:
                    return True
                
                # In both title AND H1 = definitely primary
                if in_title and in_h1:
                    return True
        
        return False

    def _filter_keyword(self, keyword: str) -> bool:
        """
        Filter keywords based on strict rules.
        
        Requirements:
        - Must be 2-5 words (phrases, not single words)
        - Must not be NAP or noise
        - Must be meaningful (not just "dental" or "care")
        """
        words = keyword.split()
        word_count = len(words)
        
        # Must be a phrase (2-5 words)
        if word_count < 2 or word_count > 5:
            return False
        
        # Filter NAP and noise
        if self._is_nap_or_noise(keyword):
            return False
        
        # Must not be all branded/generic words
        kw_lower = keyword.lower()
        words_lower = kw_lower.split()
        
        # If all words are in branded noise, skip
        if all(word in self.BRANDED_NOISE for word in words_lower):
            return False
        
        return True

    def fetch_sitemap_urls(self, sitemap_url: str, max_urls: int = 50) -> List[str]:
        """Fetch URLs from a sitemap, applying strict exclusion rules."""
        urls = []
        visited_sitemaps = set()
        
        def _fetch_recursive(url: str):
            if len(urls) >= max_urls:
                return
            if url in visited_sitemaps:
                return
            
            visited_sitemaps.add(url)
            
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                response = requests.get(url, headers=headers, timeout=15)
                
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'lxml-xml')
                    
                    # Handle sitemap index
                    sitemap_locs = soup.find_all('sitemap')
                    if sitemap_locs:
                        for sm in sitemap_locs:
                            if len(urls) >= max_urls: break
                            loc = sm.find('loc')
                            if loc:
                                _fetch_recursive(loc.text.strip())
                    else:
                        # Regular sitemap
                        url_locs = soup.find_all('url')
                        for url_entry in url_locs:
                            if len(urls) >= max_urls: break
                            loc = url_entry.find('loc')
                            if loc:
                                url_text = loc.text.strip()
                                # Apply strict exclusions
                                if not self._is_excluded_url(url_text):
                                    urls.append(url_text)
            except Exception as e:
                logger.error(f"Failed to fetch sitemap {url}: {e}")

        # Start recursion
        _fetch_recursive(sitemap_url)
        return urls[:max_urls]
    
    async def discover_site_urls(self, domain: str, max_urls: int = 30) -> List[str]:
        """Discover URLs for a domain via sitemap."""
        if not domain.startswith(('http://', 'https://')):
            domain = f'https://{domain}'
        
        parsed = urlparse(domain)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        sitemap_locations = [
            f"{base_url}/sitemap.xml",
            f"{base_url}/sitemap_index.xml",
            f"{base_url}/wp-sitemap.xml",
            f"{base_url}/page-sitemap.xml",
            f"{base_url}/post-sitemap.xml",
        ]
        
        all_urls = []
        visited_urls = set()
        
        # Helper to fetch sitemaps in thread
        loop = asyncio.get_running_loop()
        
        for sitemap_url in sitemap_locations:
            if len(all_urls) >= max_urls:
                break
                
            # Calculate remaining quota
            remaining = max_urls - len(all_urls)
            logger.info(f"Checking sitemap: {sitemap_url} (Need {remaining} more)")
            
            # Fetch from this sitemap (run in thread since fetch_sitemap_urls is sync)
            found_urls = await loop.run_in_executor(
                None, 
                lambda: self.fetch_sitemap_urls(sitemap_url, max_urls=remaining)
            )
            
            # Add unique URLs
            new_count = 0
            for url in found_urls:
                if url not in visited_urls:
                    visited_urls.add(url)
                    all_urls.append(url)
                    new_count += 1
                    if len(all_urls) >= max_urls:
                        break
            
            if new_count > 0:
                logger.info(f"Added {new_count} new URLs from {sitemap_url}")

        if not all_urls:
            logger.warning(f"No sitemap found for {base_url}, using homepage only")
            all_urls = [base_url]
            visited_urls.add(base_url)
        
        # SPIDER FALLBACK: If sitemaps didn't yield enough URLs, spider internal links
        if len(all_urls) < max_urls:
            needed = max_urls - len(all_urls)
            logger.info(f"Sitemaps yielded {len(all_urls)} pages. Spidering for {needed} more...")
            
            # Use a queue for BFS (start with what we have)
            # Limit queue to prevent infinite loops, but ensure diversity
            queue = list(all_urls)  # Start with ALL discovered pages, not just first 50

            queue_idx = 0
            
            # Sem for limiting concurrent spidering
            sem = asyncio.Semaphore(20)

            async def _spider_single(url):
                async with sem:
                    try:
                        logger.debug(f"Spidering {url} for more links...")
                        return await self._fetch_links_from_page_async(url, base_url)
                    except Exception as e:
                        logger.debug(f"Spider failed on {url}: {e}")
                        return []

            while len(all_urls) < max_urls and queue_idx < len(queue):
                # Process in batches of 20
                batch_size = 20
                batch_urls = []
                
                while len(batch_urls) < batch_size and queue_idx < len(queue):
                    current_url = queue[queue_idx]
                    queue_idx += 1
                    
                    # Check if we should even crawl this page for links (skip images/files)
                    if any(current_url.lower().endswith(ext) for ext in ['.jpg','.png','.pdf','.xml']):
                        continue
                    
                    batch_urls.append(current_url)
                
                if not batch_urls:
                    break
                
                logger.info(f"Spidering batch of {len(batch_urls)} urls...")
                tasks = [_spider_single(u) for u in batch_urls]
                results = await asyncio.gather(*tasks)
                
                # Process results
                for new_links in results:
                    for link in new_links:
                        if link not in visited_urls:
                            visited_urls.add(link)
                            all_urls.append(link)
                            queue.append(link)
                            
                            if len(all_urls) >= max_urls:
                                break
                    if len(all_urls) >= max_urls:
                        break
                                
                # Respect rate limits slightly (batch level)
                await asyncio.sleep(0.1)
                    
        logger.info(f"Total discovered URLs (Sitemap + Spider): {len(all_urls)}")
        return all_urls[:max_urls]

    
    
    async def _fetch_links_from_page_async(self, url: str, base_domain: str) -> List[str]:
        """Fetch internal links from a page asynchronously."""
        
        def _sync_fetch():
            links = []
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                response = requests.get(url, headers=headers, timeout=10)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Normalize base domain for comparison (handle www vs non-www)
                    base_parsed = urlparse(base_domain)
                    base_host = base_parsed.netloc.replace('www.', '')
                    
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        
                        # Skip basic non-http links
                        if href.startswith(('javascript:', 'mailto:', 'tel:', '#')):
                            continue

                        full_url = urljoin(url, href)
                        
                        # Clean URL (remove fragment)
                        full_url = full_url.split('#')[0]
                        
                        # Check internal (relaxed check for www)
                        parsed_full = urlparse(full_url)
                        if parsed_full.netloc.replace('www.', '') != base_host:
                            continue
                            
                        # Clean trailing slash consistency
                        if full_url.endswith('/'):
                            full_url = full_url[:-1]
                            
                        # Checks
                        if full_url == base_domain: continue
                        if self._is_excluded_url(full_url): continue
                        
                        if full_url not in links:
                            links.append(full_url)
            except Exception as e:
                logger.debug(f"Spider error on {url}: {e}")
            return links

        # Run blocking request in executor
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _sync_fetch)
    
    async def analyze_pages(self, urls: List[str]) -> Dict[str, Dict]:
        """
        Analyze multiple pages for keyword targeting concurrently.
        Returns page_data with type detection.
        """
        page_data = {}
        
        # Limit concurrency to avoid blocking or overwhelming server
        sem = asyncio.Semaphore(10)
        
        async def _analyze_single(url):
            async with sem:
                try:
                    result = await self.analyzer.analyze_page(url, use_ai=False, top_n=30)
                    
                    if not result.get('error'):
                        title = result.get('title', '')
                        h1 = result.get('h1', '')
                        
                        data = {
                            'title': title,
                            'h1': h1,
                            'keywords': result.get('keywords', []),
                            'total_words': result.get('total_words', 0),
                            'type': self._detect_page_type(url, title, h1)
                        }
                        
                        logger.info(f"Analyzed {url} - Type: {data['type']}")
                        return url, data
                    else:
                        logger.warning(f"Failed to analyze {url}: {result.get('error')}")
                        return url, None
                        
                except Exception as e:
                    logger.error(f"Error analyzing {url}: {e}")
                    return url, None

        # Run all analysis tasks concurrently
        tasks = [_analyze_single(url) for url in urls]
        results = await asyncio.gather(*tasks)
        
        # Collect successful results
        for url, data in results:
            if data:
                page_data[url] = data
        
        return page_data
    
    def _should_flag_cannibalization(
        self, 
        pages: List[Dict], 
        keyword: str, 
        intent: str
    ) -> Tuple[bool, str, str]:
        """
        Determine if this is TRUE cannibalization requiring action.
        
        Returns: (should_flag, severity, recommendation)
        
        Rules:
        1. Service vs Service (same intent) = HIGH (True cannibalization)
        2. Service vs Blog = LOW (Opportunity for internal linking)
        3. Service vs Profile/Review = IGNORE (Different purposes)
        4. Multiple Blogs = MEDIUM (Content consolidation opportunity)
        5. Anything with System/Location pages = IGNORE
        """
        # Group by page type
        by_type = defaultdict(list)
        for page in pages:
            by_type[page['type']].append(page)
        
        service_count = len(by_type['Service'])
        blog_count = len(by_type['Blog'])
        profile_count = len(by_type['Profile'])
        review_count = len(by_type['Review'])
        homepage_count = len(by_type['Homepage'])
        
        # RULE 1: Multiple Service pages targeting same keyword = TRUE CANNIBALIZATION
        if service_count >= 2:
            return (
                True,
                'high',
                'TRUE CANNIBALIZATION: Merge content into one comprehensive page or use 301 redirect from secondary to primary service page. This is competing for the same search query.'
            )
        
        # RULE 2: Service + Blog = OPPORTUNITY (not a problem, but can optimize)
        if service_count >= 1 and blog_count >= 1:
            return (
                True,
                'low',
                'LINKING OPPORTUNITY: Blog content should link to the main service page. This is beneficial semantic overlap, not cannibalization.'
            )
        
        # RULE 3: Service + Profile/Review = EXPECTED (E-E-A-T signals)
        if service_count >= 1 and (profile_count >= 1 or review_count >= 1):
            return (
                False,
                'none',
                'NO ACTION: Profile and review pages mentioning this keyword support the main service page (E-E-A-T). This is expected and beneficial.'
            )
        
        # RULE 4: Multiple Blog posts = CONTENT CONSOLIDATION
        if blog_count >= 2 and service_count == 0:
            return (
                True,
                'medium',
                'CONTENT OPPORTUNITY: Consider creating a comprehensive guide that covers all aspects, or ensure clear topic differentiation and internal linking between articles.'
            )
        
        # RULE 5: Homepage + Service = NORMAL (homepage often mentions main services)
        if homepage_count >= 1 and service_count >= 1:
            return (
                False,
                'none',
                'NO ACTION: Homepage naturally mentions main services. This is expected.'
            )
        
        # RULE 6: Only profiles/reviews/system = NOT CANNIBALIZATION
        if service_count == 0 and blog_count == 0:
            return (
                False,
                'none',
                'NO ACTION: No content pages competing. This keyword appears only in profiles, reviews, or system pages.'
            )
        
        # Default: Don't flag
        return (False, 'none', 'NO ACTION: No true cannibalization detected.')
    
    async def analyze_domain(
        self,
        domain: str,
        urls: List[str] = None,
        max_pages: int = 30,
        min_density: float = 0.5
    ) -> Dict:
        """
        Complete cannibalization analysis with strict SEO-correct rules.
        
        This identifies TRUE cannibalization where multiple pages compete for
        the SAME PRIMARY KEYWORD with the SAME INTENT and PURPOSE.
        
        It IGNORES:
        - Semantic overlap (expected and beneficial)
        - NAP data (required on multiple pages)
        - System/utility pages
        - Profile pages (E-E-A-T)
        - Review pages (reputation)
        """
        logger.info(f"Starting cannibalization analysis for {domain}")
        
        # 1. Discover or use provided URLs
        if not urls:
            urls = await self.discover_site_urls(domain, max_urls=max_pages)
        
        # 2. Filter out excluded URLs
        valid_urls = [u for u in urls if not self._is_excluded_url(u)]
        logger.info(f"Analyzing {len(valid_urls)} pages (filtered from {len(urls)} total)")
        
        if not valid_urls:
            return {
                'domain': domain,
                'error': 'No valid content pages found after filtering',
                'pages_analyzed': 0
            }
        
        # 3. Analyze all pages
        page_data = await self.analyze_pages(valid_urls[:max_pages])
        
        if not page_data:
            return {
                'domain': domain,
                'error': 'No pages could be analyzed',
                'pages_analyzed': 0
            }
        
        logger.info(f"Successfully analyzed {len(page_data)} pages")
        
        # 4. Extract PRIMARY keywords only (not every word on page)
        all_keywords = set()
        keyword_to_pages = defaultdict(list)
        
        for url, data in page_data.items():
            for kw_obj in data.get('keywords', []):
                kw_text = kw_obj['keyword'].lower()
                
                # Filter: Must be a real keyword (2-5 words, not NAP)
                if not self._filter_keyword(kw_text):
                    continue
                
                # Filter: Must be PRIMARY target (in title/H1)
                if not self._is_primary_keyword_target(kw_text, data):
                    continue
                
                all_keywords.add(kw_text)
                
                keyword_to_pages[kw_text].append({
                    'url': url,
                    'title': data.get('title', ''),
                    'h1': data.get('h1', ''),
                    'type': data.get('type'),
                    'prominence_score': kw_obj.get('prominence_score', 0),
                    'density': kw_obj.get('density', 0),
                    'in_title': kw_obj.get('in_title', False),
                    'in_h1': kw_obj.get('in_headings', False)
                })
        
        logger.info(f"Found {len(all_keywords)} unique primary keywords across all pages")
        
        # 5. Classify intent for all keywords
        if all_keywords:
            classified_intents = await self.ai_service.classify_intent(list(all_keywords))
        else:
            classified_intents = {}
        
        # Map keywords to intent
        kw_to_intent = {}
        for intent, kws in classified_intents.items():
            for kw in kws:
                # Simplify intent categories
                if intent in ['commercial', 'transactional', 'local']:
                    final_intent = 'Transactional'
                elif intent == 'navigational':
                    final_intent = 'Navigational'
                else:
                    final_intent = 'Informational'
                kw_to_intent[kw] = final_intent
        
        # 6. Detect TRUE cannibalization with strict rules
        issues = []
        
        for keyword, pages in keyword_to_pages.items():
            # Only flag if 2+ pages target this keyword
            if len(pages) < 2:
                continue
            
            intent = kw_to_intent.get(keyword, 'Unknown')
            
            # Check if this is true cannibalization
            should_flag, severity, recommendation = self._should_flag_cannibalization(
                pages, keyword, intent
            )
            
            if not should_flag:
                continue
            
            # Sort pages by prominence (identify primary vs competing)
            pages_sorted = sorted(
                pages, 
                key=lambda x: (x['prominence_score'], x['density']), 
                reverse=True
            )
            
            primary_page = pages_sorted[0]
            competing_pages = pages_sorted[1:]
            
            # Calculate risk score based on severity and page count
            if severity == 'high':
                risk_score = min(90, 70 + (len(pages) * 5))
            elif severity == 'medium':
                risk_score = min(70, 40 + (len(pages) * 5))
            else:  # low
                risk_score = min(50, 20 + (len(pages) * 5))
            
            # Build page type summary
            type_counts = defaultdict(int)
            for page in pages:
                type_counts[page['type']] += 1
            
            type_summary = ', '.join([f"{count} {ptype}" for ptype, count in type_counts.items()])
            
            # Generate risk factors for frontend display
            risk_factors = []
            if len(pages) >= 3:
                risk_factors.append(f"{len(pages)} pages competing")
            if severity == 'high':
                risk_factors.append("Critical Service vs Service conflict")
            elif severity == 'medium':
                risk_factors.append("Multiple Blog posts overlap")
            
            # Check optimization overlap
            highly_optimized_count = sum(1 for p in pages if p['prominence_score'] > 60)
            if highly_optimized_count >= 2:
                risk_factors.append("Multiple pages highly optimized")

            issues.append({
                'keyword': keyword,
                'search_intent': intent,
                'severity': severity,
                'risk_score': risk_score,
                'page_count': len(pages),
                'page_types': type_summary,
                'risk_factors': risk_factors, # [RESTORED]
                'primary_page': {
                    'url': primary_page['url'],
                    'title': primary_page['title'],
                    'type': primary_page['type'],
                    'prominence': round(primary_page['prominence_score'], 2)
                },
                'competing_pages': [
                    {
                        'url': p['url'],
                        'title': p['title'],
                        'type': p['type'],
                        'prominence': round(p['prominence_score'], 2)
                    } for p in competing_pages
                ],
                'recommendation': recommendation,
                'explanation': self._generate_explanation(keyword, pages, intent)
            })
        
        # Sort by risk score
        issues.sort(key=lambda x: x['risk_score'], reverse=True)
        
        # 7. Generate summary
        high_issues = [i for i in issues if i['severity'] == 'high']
        medium_issues = [i for i in issues if i['severity'] == 'medium']
        low_issues = [i for i in issues if i['severity'] == 'low']
        
        return {
            'domain': domain,
            'pages_analyzed': len(page_data),
            'primary_keywords_found': len(all_keywords),
            'total_keywords_indexed': len(all_keywords), # Added for frontend compatibility
            'cannibalization_issues': { # Renamed from cannibalization_summary for frontend compatibility
                'total_issues': len(issues),
                'high_severity': len(high_issues),
                'medium_severity': len(medium_issues),
                'low_severity': len(low_issues)
            },
            'issues': issues[:30],  # Return top 30
            'high_priority_issues': high_issues[:10],  # Top 10 critical
            'summary': self._generate_summary(issues, len(page_data)),
            'page_type_distribution': self._get_type_distribution(page_data)
        }
    
    def _generate_explanation(self, keyword: str, pages: List[Dict], intent: str) -> str:
        """Generate clear explanation of why this is flagged."""
        page_types = [p['type'] for p in pages]
        type_counts = defaultdict(int)
        for pt in page_types:
            type_counts[pt] += 1
        
        service_count = type_counts['Service']
        blog_count = type_counts['Blog']
        
        if service_count >= 2:
            return f"Multiple service pages ({service_count}) are targeting '{keyword}' as a primary keyword. These pages are competing for the same search query and should be consolidated."
        elif service_count >= 1 and blog_count >= 1:
            return f"You have {service_count} service page(s) and {blog_count} blog post(s) targeting '{keyword}'. The blog should link to the service page to support it."
        elif blog_count >= 2:
            return f"Multiple blog posts ({blog_count}) cover '{keyword}'. Consider creating one comprehensive guide or ensure clear topic differentiation."
        else:
            return f"{len(pages)} pages mention '{keyword}' as a primary target."
    
    def _get_type_distribution(self, page_data: Dict) -> Dict[str, int]:
        """Get distribution of page types analyzed."""
        type_counts = defaultdict(int)
        for data in page_data.values():
            type_counts[data['type']] += 1
        return dict(type_counts)
    
    def _generate_summary(self, issues: List[Dict], pages_analyzed: int) -> str:
        """Generate human-readable summary."""
        if not issues:
            return (
                f"✅ Excellent! No true keyword cannibalization detected across "
                f"{pages_analyzed} pages. Your content is well-organized with "
                f"clear primary keyword targeting."
            )
        
        high = len([i for i in issues if i['severity'] == 'high'])
        medium = len([i for i in issues if i['severity'] == 'medium'])
        low = len([i for i in issues if i['severity'] == 'low'])
        
        summary_parts = []
        
        if high > 0:
            summary_parts.append(
                f"🔴 {high} CRITICAL issue(s): Multiple service pages competing "
                f"for the same primary keywords. These need immediate consolidation."
            )
        
        if medium > 0:
            summary_parts.append(
                f"🟡 {medium} content opportunity(ies): Blog posts could be "
                f"consolidated into comprehensive guides."
            )
        
        if low > 0:
            summary_parts.append(
                f"🟢 {low} linking opportunity(ies): Blog content should link "
                f"to related service pages."
            )
        
        return " | ".join(summary_parts)


# Example usage
if __name__ == "__main__":
    async def main():
        """Example usage of the refined detector."""
        detector = KeywordCannibalizationDetector()
        
        try:
            # Analyze a domain
            result = await detector.analyze_domain(
                domain="example.com",
                max_pages=30
            )
            
            print(f"\n{'='*60}")
            print(f"CANNIBALIZATION ANALYSIS RESULTS")
            print(f"{'='*60}\n")
            
            print(f"Domain: {result['domain']}")
            print(f"Pages Analyzed: {result['pages_analyzed']}")
            print(f"Primary Keywords Found: {result['primary_keywords_found']}")
            
            print(f"\n{result['summary']}\n")
            
            print(f"Page Type Distribution:")
            for ptype, count in result['page_type_distribution'].items():
                print(f"  {ptype}: {count}")
            
            if result['high_priority_issues']:
                print(f"\n{'='*60}")
                print(f"HIGH PRIORITY ISSUES (Requires Action)")
                print(f"{'='*60}\n")
                
                for i, issue in enumerate(result['high_priority_issues'], 1):
                    print(f"{i}. Keyword: '{issue['keyword']}'")
                    print(f"   Intent: {issue['search_intent']}")
                    print(f"   Risk Score: {issue['risk_score']}/100")
                    print(f"   Pages Affected: {issue['page_count']} ({issue['page_types']})")
                    print(f"   \n   Primary Page:")
                    print(f"   - {issue['primary_page']['title']}")
                    print(f"   - {issue['primary_page']['url']}")
                    print(f"   \n   Competing Pages:")
                    for comp in issue['competing_pages']:
                        print(f"   - {comp['title']} ({comp['type']})")
                        print(f"     {comp['url']}")
                    print(f"   \n   ✅ RECOMMENDATION:")
                    print(f"   {issue['recommendation']}")
                    print(f"   \n   💡 EXPLANATION:")
                    print(f"   {issue['explanation']}")
                    print(f"\n{'-'*60}\n")
            
        finally:
            await detector.close()

    asyncio.run(main())
