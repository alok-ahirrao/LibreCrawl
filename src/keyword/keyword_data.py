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

# Try to import pytrends
PYTRENDS_AVAILABLE = False
TrendReq = None

try:
    from pytrends.request import TrendReq as _TrendReq
    TrendReq = _TrendReq
    PYTRENDS_AVAILABLE = True
except ImportError:
    logger.warning("pytrends not installed. Run: pip install pytrends")


class KeywordDataService:
    """
    Service for fetching keyword data from external sources.
    
    Features:
    - Google Trends integration (trending/rising keywords)
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
                logger.info("PyTrends initialized successfully")
            except Exception as e:
                logger.warning(f"PyTrends initialization failed (will use autocomplete only): {e}")
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
        
        Args:
            seed_keywords: List of seed keywords (max 5)
            geo: Geographic region (overrides default)
            timeframe: Time range ('today 12-m', 'today 3-m', 'now 7-d')
            
        Returns:
            Dict with trending keywords, related queries, and interest data
        """
        # Check if pytrends is properly initialized
        if not self.pytrends_available or not self.pytrends:
            logger.info("PyTrends not available, skipping trends fetch")
            return {
                'related_queries': [],
                'rising_queries': [],
                'interest_over_time': []
            }
        
        region = geo or self.geo
        
        # Limit to 5 keywords (pytrends limit)
        keywords = seed_keywords[:5]
        
        result = {
            'related_queries': [],
            'rising_queries': [],
            'interest_over_time': [],
            'suggestions': []
        }
        
        try:
            # Build payload
            self.pytrends.build_payload(
                kw_list=keywords,
                timeframe=timeframe,
                geo=region
            )
            
            # Get related queries
            try:
                related = self.pytrends.related_queries()
                for kw in keywords:
                    if kw in related and related[kw]:
                        # Top queries (consistently popular)
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
                        
                        # Rising queries (growing in popularity)
                        if related[kw].get('rising') is not None:
                            rising_df = related[kw]['rising']
                            for _, row in rising_df.head(10).iterrows():
                                value = row['value']
                                # Handle 'Breakout' values
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
                logger.warning(f"Related queries failed: {e}")
            
            # Get interest over time
            try:
                interest = self.pytrends.interest_over_time()
                if not interest.empty:
                    # Get last 4 data points for trend indication
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
                logger.warning(f"Interest over time failed: {e}")
            
            # Get suggestions for each keyword
            try:
                for kw in keywords:
                    suggestions = self.pytrends.suggestions(kw)
                    for s in suggestions[:5]:
                        result['suggestions'].append({
                            'keyword': s.get('title', ''),
                            'type': s.get('type', 'query'),
                            'source_keyword': kw
                        })
            except Exception as e:
                logger.warning(f"Suggestions failed: {e}")
                
        except Exception as e:
            if "400" in str(e) or "429" in str(e):
                logger.warning(f"Trends rate limited/blocked: {e}")
            else:
                logger.warning(f"Trends fetch failed: {e}")
            result['error'] = str(e)
        
        return result
    
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
