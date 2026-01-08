"""
Keyword Data Service
Provides high-traffic keyword discovery using Google Trends, Autocomplete, and AI.
"""

import re
import json
import logging
import requests
from typing import Optional, List, Dict
from urllib.parse import quote_plus
import time

logger = logging.getLogger(__name__)

# Try to import trendspyg (modern alternative, primary choice)
TRENDSPYG_AVAILABLE = False
download_google_trends_rss = None
download_google_trends_csv = None

try:
    from trendspyg import download_google_trends_rss as _download_rss
    download_google_trends_rss = _download_rss
    TRENDSPYG_AVAILABLE = True
    logger.info("trendspyg loaded successfully (primary trends source)")
except ImportError:
    logger.warning("trendspyg not installed. Run: pip install trendspyg")

# Try to import trendspyg CSV downloader for more comprehensive data
try:
    from trendspyg import download_google_trends_csv as _download_csv
    download_google_trends_csv = _download_csv
except ImportError:
    pass

# Try to import pytrends as fallback
PYTRENDS_AVAILABLE = False
TrendReq = None

try:
    from pytrends.request import TrendReq as _TrendReq
    TrendReq = _TrendReq
    PYTRENDS_AVAILABLE = True
    logger.info("pytrends available as fallback")
except ImportError:
    if not TRENDSPYG_AVAILABLE:
        logger.warning("No trends library available. Install: pip install trendspyg")


class KeywordDataService:
    """
    Service for fetching keyword data from external sources.
    
    Features:
    - Google Trends integration (trending/rising keywords) via trendspyg or pytrends
    - Google Autocomplete suggestions
    - Relative popularity scores
    - Keyword enrichment with traffic indicators
    """
    
    def __init__(self, language: str = 'en', geo: str = ''):
        """
        Initialize the keyword data service.
        
        Args:
            language: Language code (e.g., 'en', 'hi')
            geo: Geographic location (e.g., 'US', 'IN')
        """
        self.language = language
        self.geo = geo
        self.pytrends = None
        self.pytrends_available = False
        self.trendspyg_available = TRENDSPYG_AVAILABLE
        
        # Use trendspyg as primary if available
        if self.trendspyg_available:
            logger.info(f"Using trendspyg for Google Trends (geo: {geo or 'worldwide'})")
        
        # Initialize pytrends as fallback
        if PYTRENDS_AVAILABLE and TrendReq:
            try:
                # Configure with retries and timeout to avoid 400/429 errors
                self.pytrends = TrendReq(
                    hl=language, 
                    tz=0,  # Use UTC to avoid timezone issues
                    timeout=(10, 25),  # Connection and read timeouts
                    retries=2,
                    backoff_factor=0.5
                )
                self.pytrends_available = True
                logger.info("PyTrends initialized as fallback")
            except Exception as e:
                logger.warning(f"PyTrends initialization failed: {e}")
                self.pytrends_available = False
    
    def get_autocomplete_suggestions(
        self, 
        keyword: str, 
        language: str = None,
        country: str = None
    ) -> List[Dict]:
        """
        Get Google Autocomplete suggestions for a keyword.
        
        Args:
            keyword: The seed keyword
            language: Language code (overrides default)
            country: Country code (overrides default)
            
        Returns:
            List of suggestion dicts with keyword and type
        """
        lang = language or self.language
        geo = country or self.geo
        
        try:
            # Use Google's public autocomplete API
            url = "https://suggestqueries.google.com/complete/search"
            params = {
                'client': 'firefox',
                'q': keyword,
                'hl': lang,
            }
            if geo:
                params['gl'] = geo
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                suggestions = data[1] if len(data) > 1 else []
                
                return [
                    {
                        'keyword': s,
                        'type': self._classify_suggestion_type(s, keyword),
                        'source': 'autocomplete'
                    }
                    for s in suggestions[:15]  # Limit to 15
                ]
            
        except Exception as e:
            logger.error(f"Autocomplete fetch failed: {e}")
        
        return []
    
    def _classify_suggestion_type(self, suggestion: str, seed: str) -> str:
        """Classify the type of autocomplete suggestion."""
        suggestion_lower = suggestion.lower()
        seed_lower = seed.lower()
        
        # Question keywords
        if any(q in suggestion_lower for q in ['how', 'what', 'why', 'when', 'where', 'which', 'who']):
            return 'question'
        
        # Long-tail (more than seed + 2 words)
        seed_words = len(seed_lower.split())
        sugg_words = len(suggestion_lower.split())
        if sugg_words >= seed_words + 2:
            return 'long-tail'
        
        # Transactional intent
        if any(t in suggestion_lower for t in ['buy', 'price', 'cost', 'near me', 'best', 'cheap', 'discount']):
            return 'transactional'
        
        # Comparison
        if any(c in suggestion_lower for c in ['vs', 'versus', 'compare', 'or', 'difference']):
            return 'comparison'
        
        return 'related'
    
    def get_trending_keywords(
        self, 
        seed_keywords: List[str],
        geo: str = None,
        timeframe: str = 'today 12-m'
    ) -> Dict:
        """
        Get trending and related keywords from Google Trends.
        
        Uses trendspyg as primary source (faster, more reliable) with pytrends fallback.
        
        Args:
            seed_keywords: List of seed keywords (max 5)
            geo: Geographic region (overrides default)
            timeframe: Time range ('today 12-m', 'today 3-m', 'now 7-d')
            
        Returns:
            Dict with trending keywords, related queries, and interest data
        """
        region = geo or self.geo
        keywords = seed_keywords[:5]
        
        result = {
            'related_queries': [],
            'rising_queries': [],
            'interest_over_time': [],
            'suggestions': [],
            'trending_now': []  # Real-time general trends from trendspyg (not seed-specific)
        }
        
        # ========== OPTIONAL: Get general trending topics via trendspyg ==========
        # Note: trendspyg RSS returns GENERAL trending topics, NOT seed-keyword-related data
        # We store these separately in 'trending_now' and don't mix with seed-specific results
        if self.trendspyg_available and download_google_trends_rss:
            try:
                trendspyg_geo = region.upper() if region else 'US'
                logger.info(f"Fetching general trends via trendspyg RSS (geo: {trendspyg_geo})")
                trending = download_google_trends_rss(geo=trendspyg_geo)
                
                if trending:
                    for trend in trending[:15]:  # Top 15 general trends
                        trend_keyword = trend.get('trend', '')
                        traffic = trend.get('traffic', '0+')
                        
                        # Only add to trending_now (general trends, NOT seed-specific)
                        result['trending_now'].append({
                            'keyword': trend_keyword,
                            'traffic': traffic,
                            'source': 'trendspyg_rss',
                            'type': 'trending',
                            'traffic_potential': self._traffic_to_potential(traffic),
                            'news_articles': len(trend.get('news_articles', [])),
                            'explore_link': trend.get('explore_link', '')
                        })
                    
                    logger.info(f"trendspyg: {len(trending)} general trending topics (stored separately)")
                    
            except Exception as e:
                logger.debug(f"trendspyg RSS fetch failed: {e}")
        
        # ========== PRIMARY: Always use pytrends for SEED-KEYWORD-SPECIFIC data ==========
        # pytrends provides related queries, rising queries, and interest data FOR THE SEED KEYWORDS
        if PYTRENDS_AVAILABLE and TrendReq:
            try:
                logger.info(f"Using pytrends for seed-keyword related data: {keywords}")
                
                # Create a fresh client
                pytrends = TrendReq(
                    hl='en-US', 
                    tz=360, 
                    timeout=(10, 25), 
                    retries=0, 
                    backoff_factor=0.1
                )
                
                # Helper to build payload with fallbacks
                def fetch_trends(pt_client, tf, g):
                    pt_client.build_payload(
                        kw_list=keywords,
                        timeframe=tf,
                        geo=g
                    )
                    return pt_client
                
                # Robust Multi-Stage Fallback Strategy (Silent)
                payload_success = False
                try:
                    fetch_trends(pytrends, timeframe, region)
                    payload_success = True
                except Exception:
                    try:
                        fetch_trends(pytrends, 'now 7-d', region)
                        payload_success = True
                    except Exception:
                        try:
                            fetch_trends(pytrends, timeframe, '')
                            payload_success = True
                        except Exception:
                            try:
                                if len(keywords) > 1:
                                    pytrends.build_payload(
                                        kw_list=[keywords[0]],
                                        timeframe=timeframe,
                                        geo=region
                                    )
                                    payload_success = True
                            except Exception:
                                logger.info("pytrends fallback exhausted")
                
                if payload_success:
                    # Get related queries
                    try:
                        related = pytrends.related_queries()
                        for kw in keywords:
                            if kw in related and related[kw]:
                                if related[kw].get('top') is not None:
                                    top_df = related[kw]['top']
                                    for _, row in top_df.head(10).iterrows():
                                        result['related_queries'].append({
                                            'keyword': row['query'],
                                            'score': int(row['value']),
                                            'source_keyword': kw,
                                            'type': 'top',
                                            'traffic_potential': self._score_to_traffic(row['value'])
                                        })
                                
                                if related[kw].get('rising') is not None:
                                    rising_df = related[kw]['rising']
                                    for _, row in rising_df.head(10).iterrows():
                                        value = row['value']
                                        if isinstance(value, str):
                                            score = 1000 if 'Breakout' in value else 0
                                        else:
                                            score = int(value)
                                        
                                        result['rising_queries'].append({
                                            'keyword': row['query'],
                                            'growth': value if isinstance(value, str) else f"+{value}%",
                                            'source_keyword': kw,
                                            'type': 'rising',
                                            'is_breakout': isinstance(value, str) and 'Breakout' in value,
                                            'traffic_potential': 'high' if score > 500 else 'medium'
                                        })
                    except Exception as e:
                        logger.debug(f"pytrends related queries failed: {e}")
                    
                    # Get interest over time
                    try:
                        interest = pytrends.interest_over_time()
                        if not interest.empty:
                            for kw in keywords:
                                if kw in interest.columns:
                                    recent = interest[kw].tail(4).tolist()
                                    avg = sum(recent) / len(recent) if recent else 0
                                    trend = 'rising' if len(recent) > 1 and recent[-1] > recent[0] else 'falling'
                                    
                                    result['interest_over_time'].append({
                                        'keyword': kw,
                                        'current_interest': recent[-1] if recent else 0,
                                        'average_interest': round(avg, 1),
                                        'trend': trend,
                                        'traffic_potential': self._score_to_traffic(avg)
                                    })
                    except Exception as e:
                        logger.debug(f"pytrends interest failed: {e}")
                    
                    # Get suggestions
                    try:
                        for kw in keywords:
                            suggestions = pytrends.suggestions(kw)
                            for s in suggestions[:5]:
                                result['suggestions'].append({
                                    'keyword': s.get('title', ''),
                                    'type': s.get('type', 'query'),
                                    'source_keyword': kw
                                })
                    except Exception as e:
                        logger.debug(f"pytrends suggestions failed: {e}")
                        
            except Exception as e:
                logger.debug(f"pytrends fallback failed: {e}")
        
        # No trends library available
        if not self.trendspyg_available and not PYTRENDS_AVAILABLE:
            logger.info("No trends library available, skipping trends fetch")
            result['error'] = "No trends library available. Install: pip install trendspyg"
        
        return result
    
    def _traffic_to_potential(self, traffic: str) -> str:
        """Convert trendspyg traffic string to potential label."""
        traffic_str = str(traffic).upper().replace(',', '').replace('+', '')
        
        # Handle M/K notation (e.g., '2M+', '500K+')
        if 'M' in traffic_str:
            return 'very_high'
        elif '500K' in traffic_str or '200K' in traffic_str:
            return 'high'
        elif '100K' in traffic_str or '50K' in traffic_str:
            return 'medium'
        elif 'K' in traffic_str:
            return 'low'
        
        # Handle numeric format (e.g., '1000+', '500+', '100+')
        try:
            # Extract numeric value
            import re
            match = re.search(r'(\d+)', traffic_str)
            if match:
                val = int(match.group(1))
                if val >= 10000:
                    return 'very_high'
                elif val >= 1000:
                    return 'high'
                elif val >= 500:
                    return 'medium'
                elif val >= 100:
                    return 'low'
        except:
            pass
        
        return 'medium'
    
    def _score_to_traffic(self, score: float) -> str:
        """Convert popularity score to traffic potential label."""
        if score >= 75:
            return 'very_high'
        elif score >= 50:
            return 'high'
        elif score >= 25:
            return 'medium'
        elif score >= 10:
            return 'low'
        return 'very_low'
    
    def generate_long_tail_keywords(
        self, 
        seed_keyword: str,
        include_alphabet_soup: bool = True,
        include_patterns: bool = True,
        include_modifiers: bool = True
    ) -> List[Dict]:
        """
        Generate long-tail keyword variations for a seed keyword.
        
        Uses multiple techniques:
        1. Alphabet soup (append a-z to seed)
        2. Pattern-based (how to, best, near me, etc.)
        3. Intent modifiers (buy, price, review, etc.)
        
        Args:
            seed_keyword: The base keyword to expand
            include_alphabet_soup: Generate a-z variations
            include_patterns: Use common search patterns
            include_modifiers: Add intent modifiers
            
        Returns:
            List of long-tail keyword suggestions with metadata
        """
        long_tails = []
        seed = seed_keyword.strip().lower()
        
        # 1. Alphabet Soup Technique - Get autocomplete for seed + each letter
        if include_alphabet_soup:
            for letter in 'abcdefghijklmnopqrstuvwxyz':
                suggestions = self.get_autocomplete_suggestions(f"{seed} {letter}")
                for s in suggestions[:3]:  # Top 3 per letter
                    if s['keyword'].lower() != seed:
                        long_tails.append({
                            'keyword': s['keyword'],
                            'type': 'long-tail',
                            'source': 'alphabet_soup',
                            'traffic_potential': 'medium',
                            'competition': 'low'
                        })
                time.sleep(0.1)  # Rate limiting
        
        # 2. Pattern-Based Variations
        if include_patterns:
            patterns = [
                f"how to {seed}",
                f"how much does {seed} cost",
                f"best {seed}",
                f"best {seed} for beginners",
                f"{seed} near me",
                f"{seed} in [city]",  # Template
                f"what is {seed}",
                f"why {seed}",
                f"{seed} vs",
                f"{seed} alternatives",
                f"{seed} reviews",
                f"{seed} guide",
                f"{seed} tutorial",
                f"{seed} tips",
                f"{seed} examples",
                f"cheap {seed}",
                f"affordable {seed}",
                f"{seed} for sale",
                f"{seed} services",
                f"professional {seed}",
                f"{seed} before and after",
                f"{seed} pros and cons"
            ]
            
            for pattern in patterns:
                if '[city]' in pattern:
                    continue  # Skip template patterns
                long_tails.append({
                    'keyword': pattern,
                    'type': 'long-tail',
                    'source': 'pattern',
                    'traffic_potential': 'medium',
                    'competition': 'low'
                })
        
        # 3. Intent Modifiers
        if include_modifiers:
            # Transactional modifiers
            transactional = ['buy', 'price', 'cost', 'discount', 'coupon', 'deal', 'cheap', 'affordable', 'free']
            for mod in transactional:
                long_tails.append({
                    'keyword': f"{seed} {mod}",
                    'type': 'transactional',
                    'source': 'modifier',
                    'traffic_potential': 'high',
                    'competition': 'medium',
                    'intent': 'transactional'
                })
            
            # Informational modifiers
            informational = ['meaning', 'definition', 'explained', 'benefits', 'advantages', 'disadvantages', 'facts']
            for mod in informational:
                long_tails.append({
                    'keyword': f"{seed} {mod}",
                    'type': 'informational',
                    'source': 'modifier',
                    'traffic_potential': 'medium',
                    'competition': 'low',
                    'intent': 'informational'
                })
            
            # Comparison modifiers
            comparison = ['vs', 'versus', 'compared to', 'or', 'difference between']
            for mod in comparison:
                long_tails.append({
                    'keyword': f"{seed} {mod}",
                    'type': 'comparison',
                    'source': 'modifier',
                    'traffic_potential': 'medium',
                    'competition': 'medium',
                    'intent': 'comparison'
                })
        
        # Deduplicate
        seen = set()
        unique = []
        for kw in long_tails:
            key = kw['keyword'].lower()
            if key not in seen:
                seen.add(key)
                unique.append(kw)
        
        return unique
    
    def get_people_also_ask(self, keyword: str) -> List[Dict]:
        """
        Get 'People Also Ask' questions from Google SERP.
        
        Args:
            keyword: The search keyword
            
        Returns:
            List of related questions with topics
        """
        questions = []
        
        try:
            url = f"https://www.google.com/search?q={quote_plus(keyword)}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find PAA questions (multiple selectors for robustness)
                paa_selectors = [
                    'div[data-q]',  # PAA question attribute
                    'div.related-question-pair',
                    'div[jsname="Cpkphb"]'
                ]
                
                for selector in paa_selectors:
                    elements = soup.select(selector)
                    for el in elements:
                        # Try to extract question text
                        question = el.get('data-q') or el.get_text(strip=True)
                        if question and '?' in question and len(question) < 200:
                            questions.append({
                                'keyword': question,
                                'type': 'question',
                                'source': 'people_also_ask',
                                'traffic_potential': 'medium',
                                'content_opportunity': True
                            })
                
                # Also look for text that looks like questions
                all_text = soup.get_text()
                import re
                q_pattern = r'(?:How|What|Why|When|Where|Which|Who|Can|Do|Does|Is|Are|Should|Will|Would)[^.?!]*\?'
                matches = re.findall(q_pattern, all_text)
                for match in matches[:10]:
                    if len(match) > 20 and len(match) < 150:
                        questions.append({
                            'keyword': match.strip(),
                            'type': 'question',
                            'source': 'serp_questions',
                            'traffic_potential': 'medium',
                            'content_opportunity': True
                        })
                
                # Deduplicate
                seen = set()
                unique = []
                for q in questions:
                    key = q['keyword'].lower()[:50]
                    if key not in seen:
                        seen.add(key)
                        unique.append(q)
                        if len(unique) >= 15:
                            break
                
                return unique
                
        except Exception as e:
            logger.error(f"People Also Ask fetch failed: {e}")
        
        return []
    
    def get_related_searches(self, keyword: str) -> List[Dict]:
        """
        Get related searches using Google's 'People also search for' data.
        Scrapes Google search results for related keywords.
        
        Args:
            keyword: The seed keyword
            
        Returns:
            List of related search terms
        """
        related = []
        
        try:
            url = f"https://www.google.com/search?q={quote_plus(keyword)}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept-Language': 'en-US,en;q=0.9'
            }
            
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find 'Related searches' section
                related_divs = soup.find_all('div', {'class': 'BNeawe'})
                for div in related_divs:
                    text = div.get_text().strip()
                    if text and len(text) > 3 and len(text) < 100:
                        if text.lower() != keyword.lower():
                            related.append({
                                'keyword': text,
                                'type': 'related',
                                'source': 'google_related'
                            })
                
                # Limit and deduplicate
                seen = set()
                unique_related = []
                for r in related:
                    if r['keyword'].lower() not in seen:
                        seen.add(r['keyword'].lower())
                        unique_related.append(r)
                        if len(unique_related) >= 10:
                            break
                
                return unique_related
                
        except Exception as e:
            logger.error(f"Related searches fetch failed: {e}")
        
        return []
    
    def discover_keywords(
        self,
        seed_keywords: List[str],
        geo: str = None,
        include_trends: bool = True,
        include_autocomplete: bool = True,
        include_questions: bool = True
    ) -> Dict:
        """
        Comprehensive keyword discovery combining all sources.
        
        Args:
            seed_keywords: Initial keywords to expand
            geo: Geographic region
            include_trends: Include Google Trends data
            include_autocomplete: Include autocomplete suggestions
            include_questions: Generate question-based keywords
            
        Returns:
            Comprehensive keyword discovery results
        """
        region = geo or self.geo
        
        result = {
            'seed_keywords': seed_keywords,
            'geo': region,
            'discovered_keywords': [],
            'trending_keywords': [],
            'autocomplete_suggestions': [],
            'questions': [],
            'total_discovered': 0
        }
        
        all_keywords = set()
        
        # Get autocomplete for each seed keyword
        if include_autocomplete:
            for seed in seed_keywords[:5]:  # Limit to 5 seeds
                suggestions = self.get_autocomplete_suggestions(seed, country=region)
                for s in suggestions:
                    if s['keyword'].lower() not in all_keywords:
                        all_keywords.add(s['keyword'].lower())
                        result['autocomplete_suggestions'].append(s)
                        
                        # Also add question-type keywords to questions array
                        if s.get('type') == 'question':
                            result['questions'].append({
                                'keyword': s['keyword'],
                                'type': 'question',
                                'source': 'autocomplete',
                                'traffic_potential': 'medium'
                            })
                
                # Small delay to avoid rate limiting
                time.sleep(0.2)
        
        # Get trending keywords
        if include_trends:
            try:
                trends = self.get_trending_keywords(seed_keywords, geo=region)
                
                # Add related queries
                for q in trends.get('related_queries', []):
                    kw = q['keyword']
                    if kw.lower() not in all_keywords:
                        all_keywords.add(kw.lower())
                        result['trending_keywords'].append(q)
                
                # Add rising queries with high priority
                for q in trends.get('rising_queries', []):
                    kw = q['keyword']
                    if kw.lower() not in all_keywords:
                        all_keywords.add(kw.lower())
                        q['priority'] = 'high'  # Rising queries are high priority
                        result['trending_keywords'].append(q)
                
                # Add interest data
                result['interest_data'] = trends.get('interest_over_time', [])
            except Exception as e:
                logger.warning(f"Trends fetch failed, continuing with other sources: {e}")
        
        # Generate additional question keywords using question prefixes
        if include_questions:
            question_prefixes = ['how to', 'what is', 'why', 'where to', 'best']
            
            for seed in seed_keywords[:2]:  # Limit to first 2 seeds for speed
                for prefix in question_prefixes[:3]:  # Limit prefixes for speed
                    question = f"{prefix} {seed}"
                    try:
                        suggestions = self.get_autocomplete_suggestions(question, country=region)
                        
                        for s in suggestions[:3]:
                            if s['keyword'].lower() not in all_keywords:
                                all_keywords.add(s['keyword'].lower())
                                s['type'] = 'question' if prefix in ['how to', 'what is', 'why', 'where to'] else 'intent'
                                result['questions'].append({
                                    'keyword': s['keyword'],
                                    'type': s['type'],
                                    'source': 'autocomplete',
                                    'traffic_potential': 'medium'
                                })
                        
                        time.sleep(0.15)
                    except Exception as e:
                        logger.warning(f"Question autocomplete failed for '{question}': {e}")
        
        # Combine all discovered keywords
        all_discovered = []
        
        # Add trending with highest priority
        for kw in result['trending_keywords']:
            all_discovered.append({
                'keyword': kw['keyword'],
                'type': kw.get('type', 'trending'),
                'traffic_potential': kw.get('traffic_potential', 'medium'),
                'source': 'google_trends',
                'priority': 1 if kw.get('is_breakout') else 2,
                'is_breakout': kw.get('is_breakout', False),
                'growth': kw.get('growth', '')
            })
        
        # Add questions with medium priority
        for kw in result['questions']:
            if kw['keyword'].lower() not in {d['keyword'].lower() for d in all_discovered}:
                all_discovered.append({
                    'keyword': kw['keyword'],
                    'type': kw.get('type', 'question'),
                    'traffic_potential': 'medium',
                    'source': 'autocomplete',
                    'priority': 3
                })
        
        # Add autocomplete with lower priority
        for kw in result['autocomplete_suggestions']:
            if kw['keyword'].lower() not in {d['keyword'].lower() for d in all_discovered}:
                all_discovered.append({
                    'keyword': kw['keyword'],
                    'type': kw.get('type', 'related'),
                    'traffic_potential': 'medium',
                    'source': 'autocomplete',
                    'priority': 4
                })
        
        # Sort by priority
        all_discovered.sort(key=lambda x: x['priority'])
        
        # Remove duplicates while preserving order
        seen = set()
        unique_discovered = []
        for kw in all_discovered:
            keyword_lower = kw['keyword'].lower()
            if keyword_lower not in seen:
                seen.add(keyword_lower)
                unique_discovered.append(kw)
        
        result['discovered_keywords'] = unique_discovered[:100]  # Limit to 100
        result['total_discovered'] = len(unique_discovered)
        
        return result
    
    def enrich_keywords(
        self,
        keywords: List[str],
        geo: str = None
    ) -> List[Dict]:
        """
        Enrich a list of keywords with traffic and trend data.
        
        Args:
            keywords: List of keywords to enrich
            geo: Geographic region
            
        Returns:
            List of enriched keyword dicts
        """
        if not keywords:
            return []
        
        region = geo or self.geo
        enriched = []
        
        # Get trends for batches of 5
        for i in range(0, len(keywords), 5):
            batch = keywords[i:i+5]
            trends = self.get_trending_keywords(batch, geo=region)
            
            interest_map = {}
            for item in trends.get('interest_over_time', []):
                interest_map[item['keyword'].lower()] = item
            
            for kw in batch:
                kw_lower = kw.lower()
                interest = interest_map.get(kw_lower, {})
                
                enriched.append({
                    'keyword': kw,
                    'current_interest': interest.get('current_interest', 0),
                    'average_interest': interest.get('average_interest', 0),
                    'trend': interest.get('trend', 'unknown'),
                    'traffic_potential': interest.get('traffic_potential', 'unknown')
                })
            
            # Rate limiting
            if i + 5 < len(keywords):
                time.sleep(0.5)
        
        return enriched

    # =========================================================================
    # ENHANCED KEYWORD DISCOVERY METHODS
    # =========================================================================
    
    def get_niche_specific_keywords(
        self, 
        seed_keyword: str, 
        niche: str = '',
        location: str = ''
    ) -> List[Dict]:
        """
        Generate niche-specific keyword variations based on industry patterns.
        
        Args:
            seed_keyword: The base keyword
            niche: Business niche (e.g., 'dental', 'legal', 'real estate')
            location: Optional location to include
            
        Returns:
            List of niche-specific keyword suggestions
        """
        keywords = []
        seed = seed_keyword.strip().lower()
        niche_lower = niche.lower() if niche else ''
        
        # Industry-specific patterns by niche
        niche_patterns = {
            'dental': {
                'services': ['cleaning', 'whitening', 'implants', 'crowns', 'veneers', 'fillings', 'extraction', 'root canal', 'braces', 'invisalign'],
                'modifiers': ['pediatric', 'cosmetic', 'emergency', 'family', 'affordable', 'best', 'top rated'],
                'questions': ['does insurance cover', 'how much does', 'what is the cost of', 'is it painful', 'how long does', 'recovery time for'],
                'local': ['dentist near me', 'dental clinic', 'dental office', 'dental care'],
            },
            'legal': {
                'services': ['attorney', 'lawyer', 'law firm', 'legal services', 'consultation', 'representation'],
                'modifiers': ['free consultation', 'affordable', 'experienced', 'top rated', 'best', 'local'],
                'questions': ['do i need', 'how to find', 'when to hire', 'cost of hiring'],
                'local': ['attorney near me', 'law firm', 'legal help'],
            },
            'insurance': {
                'services': ['quotes', 'coverage', 'plans', 'policy', 'premium', 'deductible'],
                'modifiers': ['cheap', 'affordable', 'best', 'low cost', 'monthly', 'annual'],
                'questions': ['how much is', 'what does cover', 'do i need', 'how to get', 'how to save on'],
                'local': ['agents near me', 'brokers', 'companies'],
            },
            'real estate': {
                'services': ['homes for sale', 'houses', 'apartments', 'condos', 'rentals', 'listings'],
                'modifiers': ['affordable', 'luxury', 'cheap', 'new', 'foreclosure', 'investment'],
                'questions': ['how to buy', 'is it a good time to', 'what to look for when buying'],
                'local': ['realtor near me', 'real estate agent', 'property listings'],
            },
            'healthcare': {
                'services': ['doctor', 'clinic', 'hospital', 'treatment', 'diagnosis', 'checkup'],
                'modifiers': ['best', 'top rated', 'affordable', 'emergency', 'specialist'],
                'questions': ['symptoms of', 'how to treat', 'when to see a doctor for', 'is it serious'],
                'local': ['doctor near me', 'clinic', 'medical center'],
            },
            'default': {
                'services': ['services', 'solutions', 'options', 'providers', 'companies'],
                'modifiers': ['best', 'top', 'affordable', 'cheap', 'professional', 'local', 'near me'],
                'questions': ['how to', 'what is', 'why', 'when to', 'cost of', 'price of'],
                'local': ['near me', 'in area', 'local'],
            }
        }
        
        # Get patterns for the niche
        patterns = niche_patterns.get(niche_lower, niche_patterns['default'])
        
        # Generate service-specific keywords
        for service in patterns.get('services', [])[:8]:
            keywords.append({
                'keyword': f"{seed} {service}",
                'type': 'niche_service',
                'source': 'niche_pattern',
                'traffic_potential': 'high',
                'intent': 'transactional',
                'niche': niche
            })
        
        # Generate modifier-based keywords
        for modifier in patterns.get('modifiers', [])[:6]:
            keywords.append({
                'keyword': f"{modifier} {seed}",
                'type': 'niche_modifier',
                'source': 'niche_pattern',
                'traffic_potential': 'high',
                'intent': 'transactional',
                'niche': niche
            })
        
        # Generate question-based keywords
        for question in patterns.get('questions', [])[:5]:
            keywords.append({
                'keyword': f"{question} {seed}",
                'type': 'question',
                'source': 'niche_pattern',
                'traffic_potential': 'medium',
                'intent': 'informational',
                'niche': niche
            })
        
        # Add location-based keywords
        if location:
            loc = location.strip()
            for local_pattern in patterns.get('local', [])[:3]:
                keywords.append({
                    'keyword': f"{seed} {local_pattern} {loc}",
                    'type': 'local',
                    'source': 'niche_location',
                    'traffic_potential': 'high',
                    'intent': 'transactional',
                    'location': loc
                })
            
            # Direct location combinations
            keywords.extend([
                {'keyword': f"{seed} in {loc}", 'type': 'local', 'source': 'location', 'traffic_potential': 'high', 'intent': 'transactional'},
                {'keyword': f"{seed} {loc}", 'type': 'local', 'source': 'location', 'traffic_potential': 'high', 'intent': 'transactional'},
                {'keyword': f"best {seed} in {loc}", 'type': 'local', 'source': 'location', 'traffic_potential': 'high', 'intent': 'transactional'},
                {'keyword': f"cheap {seed} {loc}", 'type': 'local', 'source': 'location', 'traffic_potential': 'medium', 'intent': 'transactional'},
                {'keyword': f"{seed} near {loc}", 'type': 'local', 'source': 'location', 'traffic_potential': 'high', 'intent': 'transactional'},
            ])
        
        return keywords
    
    def get_competitor_template_keywords(self, seed_keywords: List[str]) -> List[Dict]:
        """
        Generate keywords that competitors commonly target.
        
        Args:
            seed_keywords: List of seed keywords
            
        Returns:
            List of competitor-style keyword suggestions
        """
        keywords = []
        
        # Common competitor keyword patterns
        competitor_patterns = [
            # Comparison patterns
            ("{seed} vs {alt}", "comparison", "high"),
            ("{seed} alternative", "comparison", "high"),
            ("{seed} alternatives", "comparison", "high"),
            ("best {seed} alternatives", "comparison", "high"),
            ("{seed} competitors", "comparison", "medium"),
            
            # Review patterns
            ("{seed} review", "review", "high"),
            ("{seed} reviews", "review", "high"),
            ("honest {seed} review", "review", "medium"),
            ("{seed} rating", "review", "medium"),
            ("is {seed} worth it", "review", "high"),
            
            # Buying patterns
            ("buy {seed}", "transactional", "very_high"),
            ("order {seed}", "transactional", "high"),
            ("{seed} pricing", "transactional", "very_high"),
            ("{seed} plans", "transactional", "high"),
            ("{seed} subscription", "transactional", "medium"),
            ("{seed} free trial", "transactional", "high"),
            ("{seed} discount code", "transactional", "high"),
            ("{seed} promo code", "transactional", "high"),
            
            # Problem/Solution patterns
            ("{seed} not working", "support", "medium"),
            ("{seed} issues", "support", "medium"),
            ("how to fix {seed}", "support", "medium"),
            ("{seed} troubleshooting", "support", "low"),
            
            # Year-based patterns (high value)
            ("best {seed} 2026", "current", "very_high"),
            ("{seed} 2026", "current", "high"),
            ("top {seed} 2026", "current", "high"),
        ]
        
        for seed in seed_keywords[:3]:  # Limit to first 3 seeds
            for pattern, intent, potential in competitor_patterns:
                keyword = pattern.format(seed=seed.strip(), alt="competitor")
                keywords.append({
                    'keyword': keyword,
                    'type': intent,
                    'source': 'competitor_template',
                    'traffic_potential': potential,
                    'intent': intent
                })
        
        return keywords
    
    def calculate_keyword_score(self, keyword_data: Dict) -> int:
        """
        Calculate opportunity score for a keyword (0-100).
        
        Args:
            keyword_data: Dict with keyword metadata
            
        Returns:
            Opportunity score 0-100
        """
        score = 50  # Base score
        
        # Traffic potential scoring
        potential = keyword_data.get('traffic_potential', 'medium')
        potential_scores = {
            'very_high': 25,
            'high': 20,
            'medium': 10,
            'low': 5,
            'very_low': 0
        }
        score += potential_scores.get(potential, 10)
        
        # Intent scoring (transactional > commercial > informational)
        intent = keyword_data.get('intent', '').lower()
        intent_scores = {
            'transactional': 15,
            'commercial': 12,
            'comparison': 10,
            'navigational': 5,
            'informational': 3,
        }
        score += intent_scores.get(intent, 5)
        
        # Source quality scoring
        source = keyword_data.get('source', '')
        source_scores = {
            'google_trends': 10,
            'trendspyg_rss': 8,
            'autocomplete': 8,
            'niche_pattern': 7,
            'competitor_template': 6,
            'people_also_ask': 7,
            'pattern': 5,
            'modifier': 4,
        }
        score += source_scores.get(source, 3)
        
        # Breakout bonus
        if keyword_data.get('is_breakout'):
            score += 15
        
        # Local intent bonus
        if keyword_data.get('type') == 'local' or 'near me' in keyword_data.get('keyword', '').lower():
            score += 10
        
        # Cap at 100
        return min(100, max(0, score))
    
    def discover_keywords_enhanced(
        self,
        seed_keywords: List[str],
        geo: str = None,
        niche: str = '',
        location: str = '',
        include_trends: bool = True,
        include_autocomplete: bool = True,
        include_questions: bool = True,
        include_niche_patterns: bool = True,
        include_competitor_templates: bool = True
    ) -> Dict:
        """
        Enhanced comprehensive keyword discovery combining ALL sources.
        
        This is a more robust version that includes:
        - Google Trends (rising, related, interest data)
        - Google Autocomplete suggestions
        - People Also Ask questions
        - Niche-specific patterns
        - Competitor keyword templates
        - Location-based variations
        - Opportunity scoring
        
        Args:
            seed_keywords: Initial keywords to expand
            geo: Geographic region
            niche: Business niche for specialized patterns
            location: Location for local SEO keywords
            include_trends: Include Google Trends data
            include_autocomplete: Include autocomplete suggestions
            include_questions: Generate question-based keywords
            include_niche_patterns: Include niche-specific patterns
            include_competitor_templates: Include competitor keyword patterns
            
        Returns:
            Comprehensive keyword discovery results with scoring
        """
        region = geo or self.geo
        
        result = {
            'seed_keywords': seed_keywords,
            'geo': region,
            'niche': niche,
            'location': location,
            'discovered_keywords': [],
            'trending_keywords': [],
            'autocomplete_suggestions': [],
            'questions': [],
            'niche_keywords': [],
            'local_keywords': [],
            'competitor_keywords': [],
            'long_tail_keywords': [],
            'interest_data': [],
            'total_discovered': 0,
            'sources_used': []
        }
        
        all_keywords = set()
        all_discovered = []
        
        # =====================================================================
        # 1. AUTOCOMPLETE SUGGESTIONS
        # =====================================================================
        if include_autocomplete:
            result['sources_used'].append('google_autocomplete')
            for seed in seed_keywords[:5]:
                try:
                    suggestions = self.get_autocomplete_suggestions(seed, country=region)
                    for s in suggestions:
                        kw_lower = s['keyword'].lower()
                        if kw_lower not in all_keywords:
                            all_keywords.add(kw_lower)
                            s['opportunity_score'] = self.calculate_keyword_score(s)
                            result['autocomplete_suggestions'].append(s)
                            all_discovered.append(s)
                            
                            if s.get('type') == 'question':
                                result['questions'].append(s)
                    time.sleep(0.15)
                except Exception as e:
                    logger.debug(f"Autocomplete failed for {seed}: {e}")
        
        # =====================================================================
        # 2. GOOGLE TRENDS DATA
        # =====================================================================
        if include_trends:
            result['sources_used'].append('google_trends')
            try:
                trends = self.get_trending_keywords(seed_keywords, geo=region)
                
                # Process related queries
                for q in trends.get('related_queries', []):
                    kw = q['keyword']
                    kw_lower = kw.lower()
                    if kw_lower not in all_keywords:
                        all_keywords.add(kw_lower)
                        q['opportunity_score'] = self.calculate_keyword_score(q)
                        result['trending_keywords'].append(q)
                        all_discovered.append(q)
                
                # Process rising queries (high value!)
                for q in trends.get('rising_queries', []):
                    kw = q['keyword']
                    kw_lower = kw.lower()
                    if kw_lower not in all_keywords:
                        all_keywords.add(kw_lower)
                        q['priority'] = 'high'
                        q['opportunity_score'] = self.calculate_keyword_score(q)
                        result['trending_keywords'].append(q)
                        all_discovered.append(q)
                
                # Store interest data
                result['interest_data'] = trends.get('interest_over_time', [])
                
            except Exception as e:
                logger.warning(f"Trends fetch failed: {e}")
        
        # =====================================================================
        # 3. QUESTION-BASED KEYWORDS
        # =====================================================================
        if include_questions:
            result['sources_used'].append('questions')
            question_prefixes = [
                'how to', 'what is', 'why', 'where to', 'best', 
                'when to', 'can i', 'should i', 'does', 'is it worth'
            ]
            
            for seed in seed_keywords[:3]:
                for prefix in question_prefixes[:6]:
                    question = f"{prefix} {seed}"
                    try:
                        suggestions = self.get_autocomplete_suggestions(question, country=region)
                        for s in suggestions[:2]:
                            kw_lower = s['keyword'].lower()
                            if kw_lower not in all_keywords:
                                all_keywords.add(kw_lower)
                                s['type'] = 'question'
                                s['intent'] = 'informational'
                                s['opportunity_score'] = self.calculate_keyword_score(s)
                                result['questions'].append(s)
                                all_discovered.append(s)
                        time.sleep(0.1)
                    except Exception as e:
                        logger.debug(f"Question autocomplete failed: {e}")
            
            # Also try People Also Ask
            for seed in seed_keywords[:2]:
                try:
                    paa = self.get_people_also_ask(seed)
                    for q in paa:
                        kw_lower = q['keyword'].lower()
                        if kw_lower not in all_keywords:
                            all_keywords.add(kw_lower)
                            q['opportunity_score'] = self.calculate_keyword_score(q)
                            result['questions'].append(q)
                            all_discovered.append(q)
                except Exception as e:
                    logger.debug(f"PAA failed for {seed}: {e}")
        
        # =====================================================================
        # 4. NICHE-SPECIFIC PATTERNS
        # =====================================================================
        if include_niche_patterns and niche:
            result['sources_used'].append('niche_patterns')
            for seed in seed_keywords[:3]:
                try:
                    niche_kws = self.get_niche_specific_keywords(seed, niche=niche, location=location)
                    for kw in niche_kws:
                        kw_lower = kw['keyword'].lower()
                        if kw_lower not in all_keywords:
                            all_keywords.add(kw_lower)
                            kw['opportunity_score'] = self.calculate_keyword_score(kw)
                            result['niche_keywords'].append(kw)
                            all_discovered.append(kw)
                            
                            if kw.get('type') == 'local':
                                result['local_keywords'].append(kw)
                except Exception as e:
                    logger.debug(f"Niche patterns failed: {e}")
        
        # =====================================================================
        # 5. LOCATION-BASED KEYWORDS
        # =====================================================================
        if location:
            result['sources_used'].append('location_based')
            loc = location.strip()
            
            for seed in seed_keywords[:3]:
                local_patterns = [
                    f"{seed} in {loc}",
                    f"{seed} {loc}",
                    f"best {seed} in {loc}",
                    f"cheap {seed} {loc}",
                    f"affordable {seed} in {loc}",
                    f"{seed} near {loc}",
                    f"top rated {seed} {loc}",
                    f"{seed} services {loc}",
                    f"find {seed} {loc}",
                    f"{loc} {seed} reviews",
                ]
                
                for pattern in local_patterns:
                    kw_lower = pattern.lower()
                    if kw_lower not in all_keywords:
                        all_keywords.add(kw_lower)
                        kw_data = {
                            'keyword': pattern,
                            'type': 'local',
                            'source': 'location_pattern',
                            'traffic_potential': 'high',
                            'intent': 'transactional',
                            'location': loc
                        }
                        kw_data['opportunity_score'] = self.calculate_keyword_score(kw_data)
                        result['local_keywords'].append(kw_data)
                        all_discovered.append(kw_data)
        
        # =====================================================================
        # 6. COMPETITOR KEYWORD TEMPLATES
        # =====================================================================
        if include_competitor_templates:
            result['sources_used'].append('competitor_templates')
            try:
                comp_kws = self.get_competitor_template_keywords(seed_keywords)
                for kw in comp_kws:
                    kw_lower = kw['keyword'].lower()
                    if kw_lower not in all_keywords:
                        all_keywords.add(kw_lower)
                        kw['opportunity_score'] = self.calculate_keyword_score(kw)
                        result['competitor_keywords'].append(kw)
                        all_discovered.append(kw)
            except Exception as e:
                logger.debug(f"Competitor templates failed: {e}")
        
        # =====================================================================
        # 7. LONG-TAIL KEYWORDS
        # =====================================================================
        result['sources_used'].append('long_tail')
        for seed in seed_keywords[:2]:
            try:
                long_tails = self.generate_long_tail_keywords(
                    seed_keyword=seed,
                    include_alphabet_soup=False,  # Skip for speed
                    include_patterns=True,
                    include_modifiers=True
                )
                for kw in long_tails:
                    kw_lower = kw['keyword'].lower()
                    if kw_lower not in all_keywords:
                        all_keywords.add(kw_lower)
                        kw['opportunity_score'] = self.calculate_keyword_score(kw)
                        result['long_tail_keywords'].append(kw)
                        all_discovered.append(kw)
            except Exception as e:
                logger.debug(f"Long-tail generation failed: {e}")
        
        # =====================================================================
        # SCORING AND SORTING
        # =====================================================================
        # Sort all discovered by opportunity score
        all_discovered.sort(key=lambda x: x.get('opportunity_score', 0), reverse=True)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_discovered = []
        for kw in all_discovered:
            kw_lower = kw['keyword'].lower()
            if kw_lower not in seen:
                seen.add(kw_lower)
                unique_discovered.append(kw)
        
        result['discovered_keywords'] = unique_discovered[:150]  # Top 150
        result['total_discovered'] = len(unique_discovered)
        
        # Add summary stats
        result['stats'] = {
            'total_unique': len(unique_discovered),
            'from_trends': len(result['trending_keywords']),
            'from_autocomplete': len(result['autocomplete_suggestions']),
            'questions': len(result['questions']),
            'niche_specific': len(result['niche_keywords']),
            'local_keywords': len(result['local_keywords']),
            'competitor_style': len(result['competitor_keywords']),
            'long_tail': len(result['long_tail_keywords']),
            'high_opportunity': len([k for k in unique_discovered if k.get('opportunity_score', 0) >= 70]),
            'medium_opportunity': len([k for k in unique_discovered if 50 <= k.get('opportunity_score', 0) < 70]),
        }
        
        logger.info(f"Enhanced discovery complete: {result['stats']}")
        
        return result

