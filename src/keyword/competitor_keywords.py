"""
Competitor Keyword Research
Analyzes competitor websites to find keyword gaps, shared keywords, and opportunities.
Uses SERP scraping combined with AI for intelligent analysis.
"""

import asyncio
import logging
from typing import Optional, List, Dict
from urllib.parse import urlparse
import re

from .ai_service import GeminiKeywordAI
from .keyword_analyzer import KeywordDensityAnalyzer

logger = logging.getLogger(__name__)


class CompetitorKeywordResearcher:
    """
    Researches competitor keywords and identifies opportunities.
    
    Features:
    - Extract keywords from competitor pages
    - Find keyword gaps (they rank, you don't)
    - Find shared keywords (overlap)
    - Find missing opportunities
    - AI-powered difficulty estimation
    - AI-generated strategy recommendations
    """
    
    def __init__(
        self, 
        ai_service: Optional[GeminiKeywordAI] = None,
        serp_scraper = None
    ):
        """
        Initialize the researcher.
        
        Args:
            ai_service: Optional GeminiKeywordAI instance
            serp_scraper: Optional SERP scraper (will use existing geo_driver if available)
        """
        self.ai_service = ai_service or GeminiKeywordAI()
        self.analyzer = KeywordDensityAnalyzer(self.ai_service)
        self.serp_scraper = serp_scraper
    
    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL."""
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        parsed = urlparse(url)
        domain = parsed.netloc
        # Remove www prefix
        if domain.startswith('www.'):
            domain = domain[4:]
        return domain
    
    async def extract_page_keywords(
        self, 
        url: str, 
        top_n: int = 30
    ) -> dict:
        """
        Extract keywords from a specific page.
        
        Args:
            url: Page URL to analyze
            top_n: Number of top keywords to extract
            
        Returns:
            Dict with page keywords and metadata
        """
        try:
            result = await self.analyzer.analyze_page(url, use_ai=False, top_n=top_n)
            return {
                'url': url,
                'domain': self._extract_domain(url),
                'keywords': result.get('keywords', []),
                'title': result.get('title', ''),
                'total_words': result.get('total_words', 0),
                'error': result.get('error')
            }
        except Exception as e:
            logger.error(f"Failed to extract keywords from {url}: {e}")
            return {
                'url': url,
                'domain': self._extract_domain(url),
                'keywords': [],
                'error': str(e)
            }
    
    async def extract_domain_keywords(
        self,
        domain: str,
        pages: List[str] = None,
        max_pages: int = 5
    ) -> dict:
        """
        Extract keywords from multiple pages of a domain.
        
        Args:
            domain: Domain to analyze
            pages: Optional list of specific page URLs
            max_pages: Maximum pages to analyze if pages not specified
            
        Returns:
            Aggregated keywords for the domain
        """
        domain = self._extract_domain(domain)
        
        # If no specific pages provided, analyze homepage
        if not pages:
            pages = [f'https://{domain}/']
        
        all_keywords = {}
        page_results = []
        
        for page_url in pages[:max_pages]:
            result = await self.extract_page_keywords(page_url)
            page_results.append(result)
            
            # Aggregate keywords
            for kw in result.get('keywords', []):
                keyword = kw['keyword']
                if keyword in all_keywords:
                    all_keywords[keyword]['frequency'] += kw['frequency']
                    all_keywords[keyword]['pages'].append(page_url)
                else:
                    all_keywords[keyword] = {
                        'keyword': keyword,
                        'frequency': kw['frequency'],
                        'density': kw['density'],
                        'type': kw.get('type', 'word'),
                        'pages': [page_url]
                    }
        
        # Sort by frequency
        sorted_keywords = sorted(
            all_keywords.values(),
            key=lambda x: x['frequency'],
            reverse=True
        )
        
        return {
            'domain': domain,
            'total_pages_analyzed': len(page_results),
            'keywords': sorted_keywords,
            'page_results': page_results
        }
    
    def calculate_keyword_gap(
        self,
        your_keywords: List[dict],
        competitor_keywords: List[dict]
    ) -> List[dict]:
        """
        Find keywords competitor has that you don't.
        
        Args:
            your_keywords: Your keyword list
            competitor_keywords: Competitor's keyword list
            
        Returns:
            List of gap keywords (competitor has, you don't)
        """
        your_set = {kw['keyword'].lower() for kw in your_keywords}
        
        gaps = []
        for kw in competitor_keywords:
            if kw['keyword'].lower() not in your_set:
                gaps.append({
                    **kw,
                    'gap_type': 'missing'
                })
        
        return gaps
    
    def calculate_shared_keywords(
        self,
        your_keywords: List[dict],
        competitor_keywords: List[dict]
    ) -> List[dict]:
        """
        Find keywords both you and competitor have.
        
        Args:
            your_keywords: Your keyword list
            competitor_keywords: Competitor's keyword list
            
        Returns:
            List of shared keywords with comparison
        """
        your_dict = {kw['keyword'].lower(): kw for kw in your_keywords}
        comp_dict = {kw['keyword'].lower(): kw for kw in competitor_keywords}
        
        shared = []
        for keyword in your_dict:
            if keyword in comp_dict:
                your_kw = your_dict[keyword]
                comp_kw = comp_dict[keyword]
                
                shared.append({
                    'keyword': keyword,
                    'your_frequency': your_kw['frequency'],
                    'your_density': your_kw['density'],
                    'competitor_frequency': comp_kw['frequency'],
                    'competitor_density': comp_kw['density'],
                    'density_diff': round(your_kw['density'] - comp_kw['density'], 2)
                })
        
        return shared
    
    def calculate_opportunities(
        self,
        your_keywords: List[dict],
        competitor_keywords: List[dict],
        min_competitor_frequency: int = 3
    ) -> List[dict]:
        """
        Find high-value keywords you're missing.
        
        Args:
            your_keywords: Your keyword list
            competitor_keywords: Competitor's keyword list
            min_competitor_frequency: Minimum frequency in competitor's content
            
        Returns:
            Prioritized list of keyword opportunities
        """
        gaps = self.calculate_keyword_gap(your_keywords, competitor_keywords)
        
        # Filter and score opportunities
        opportunities = []
        for kw in gaps:
            if kw['frequency'] >= min_competitor_frequency:
                # Simple scoring: frequency * density weight
                score = kw['frequency'] * (1 + kw['density'] / 10)
                opportunities.append({
                    **kw,
                    'opportunity_score': round(score, 2)
                })
        
        # Sort by opportunity score
        opportunities.sort(key=lambda x: x['opportunity_score'], reverse=True)
        return opportunities
    
    async def research_competitor(
        self,
        your_url: str,
        competitor_url: str,
        use_ai: bool = True
    ) -> dict:
        """
        Perform full competitor keyword research.
        
        Args:
            your_url: Your website/page URL
            competitor_url: Competitor's website/page URL
            use_ai: Whether to use AI enhancement
            
        Returns:
            Complete competitor analysis
        """
        # Extract keywords from both
        your_result = await self.extract_page_keywords(your_url)
        competitor_result = await self.extract_page_keywords(competitor_url)
        
        your_keywords = your_result.get('keywords', [])
        competitor_keywords = competitor_result.get('keywords', [])
        
        # Calculate gaps, shared, and opportunities
        gaps = self.calculate_keyword_gap(your_keywords, competitor_keywords)
        shared = self.calculate_shared_keywords(your_keywords, competitor_keywords)
        opportunities = self.calculate_opportunities(your_keywords, competitor_keywords)
        
        result = {
            'your_domain': your_result.get('domain'),
            'competitor_domain': competitor_result.get('domain'),
            'your_keyword_count': len(your_keywords),
            'competitor_keyword_count': len(competitor_keywords),
            'gap_count': len(gaps),
            'shared_count': len(shared),
            'opportunity_count': len(opportunities),
            'gap_keywords': gaps[:30],  # Top 30
            'shared_keywords': shared[:20],  # Top 20
            'opportunities': opportunities[:20],  # Top 20
            'your_top_keywords': your_keywords[:15],
            'competitor_top_keywords': competitor_keywords[:15],
            'ai_analysis': None
        }
        
        # AI enhancement
        if use_ai and self.ai_service.is_available():
            try:
                # Get AI content gap analysis
                gap_analysis = await self.ai_service.analyze_content_gap(
                    your_keywords=[kw['keyword'] for kw in your_keywords],
                    competitor_keywords=[kw['keyword'] for kw in competitor_keywords],
                    your_domain=your_result.get('domain', ''),
                    competitor_domain=competitor_result.get('domain', '')
                )
                
                # Get AI recommendations
                recommendations = await self.ai_service.generate_recommendations(
                    analysis_data={
                        'gaps': len(gaps),
                        'shared': len(shared),
                        'opportunities': [opp['keyword'] for opp in opportunities[:10]],
                        'your_top': [kw['keyword'] for kw in your_keywords[:10]],
                        'competitor_top': [kw['keyword'] for kw in competitor_keywords[:10]]
                    }
                )
                
                result['ai_analysis'] = {
                    'gap_analysis': gap_analysis,
                    'recommendations': recommendations
                }
                
            except Exception as e:
                logger.error(f"AI analysis failed: {e}")
                result['ai_analysis'] = {'error': str(e)}
        
        return result
    
    async def research_multiple_competitors(
        self,
        your_url: str,
        competitor_urls: List[str],
        use_ai: bool = True
    ) -> dict:
        """
        Research multiple competitors at once.
        
        Args:
            your_url: Your website/page URL
            competitor_urls: List of competitor URLs
            use_ai: Whether to use AI enhancement
            
        Returns:
            Combined competitor analysis
        """
        # Get your keywords
        your_result = await self.extract_page_keywords(your_url)
        your_keywords = your_result.get('keywords', [])
        
        # Analyze each competitor
        competitor_analyses = []
        all_competitor_keywords = []
        
        for comp_url in competitor_urls:
            comp_result = await self.extract_page_keywords(comp_url)
            comp_keywords = comp_result.get('keywords', [])
            
            gaps = self.calculate_keyword_gap(your_keywords, comp_keywords)
            shared = self.calculate_shared_keywords(your_keywords, comp_keywords)
            
            competitor_analyses.append({
                'url': comp_url,
                'domain': comp_result.get('domain'),
                'keyword_count': len(comp_keywords),
                'gap_count': len(gaps),
                'shared_count': len(shared),
                'top_gaps': gaps[:10],
                'top_shared': shared[:10],
                'top_keywords': comp_keywords[:10]
            })
            
            all_competitor_keywords.extend(comp_keywords)
        
        # Find keywords all competitors share
        keyword_presence = {}
        for kw in all_competitor_keywords:
            key = kw['keyword'].lower()
            if key not in keyword_presence:
                keyword_presence[key] = {'keyword': kw['keyword'], 'count': 0, 'competitors': []}
            keyword_presence[key]['count'] += 1
        
        # Keywords appearing in multiple competitors
        common_competitor_keywords = [
            kw for kw in keyword_presence.values()
            if kw['count'] >= 2
        ]
        common_competitor_keywords.sort(key=lambda x: x['count'], reverse=True)
        
        # Find universal gaps (keywords ALL competitors have, you don't)
        your_set = {kw['keyword'].lower() for kw in your_keywords}
        universal_gaps = [
            kw for kw in common_competitor_keywords
            if kw['keyword'].lower() not in your_set
        ]
        
        result = {
            'your_domain': your_result.get('domain'),
            'your_keyword_count': len(your_keywords),
            'competitor_count': len(competitor_urls),
            'competitors': competitor_analyses,
            'common_competitor_keywords': common_competitor_keywords[:20],
            'universal_gaps': universal_gaps[:20],
            'your_top_keywords': your_keywords[:15],
            'ai_analysis': None
        }
        
        # AI analysis
        if use_ai and self.ai_service.is_available():
            try:
                # Expand opportunities with AI
                if universal_gaps:
                    expanded = await self.ai_service.expand_keywords(
                        [g['keyword'] for g in universal_gaps[:10]],
                        count=15
                    )
                    result['ai_expanded_keywords'] = expanded.get('keywords', [])
                
                # Classify all gap keywords by intent
                all_gaps = [g['keyword'] for g in universal_gaps[:30]]
                if all_gaps:
                    intents = await self.ai_service.classify_intent(all_gaps)
                    result['ai_intent_classification'] = intents
                
                # Generate strategy
                recommendations = await self.ai_service.generate_recommendations({
                    'total_competitors': len(competitor_urls),
                    'universal_gaps': len(universal_gaps),
                    'your_keywords': len(your_keywords),
                    'top_opportunities': [g['keyword'] for g in universal_gaps[:10]]
                })
                result['ai_strategy'] = recommendations
                
            except Exception as e:
                logger.error(f"AI analysis failed: {e}")
                result['ai_analysis'] = {'error': str(e)}
        
        return result
    
    async def estimate_keyword_difficulty(
        self,
        keyword: str,
        serp_data: dict = None
    ) -> dict:
        """
        Estimate difficulty for ranking on a keyword.
        
        Args:
            keyword: The keyword to analyze
            serp_data: Optional SERP data (if available)
            
        Returns:
            Difficulty estimation
        """
        if not self.ai_service.is_available():
            return {
                'keyword': keyword,
                'difficulty': 50,
                'level': 'unknown',
                'error': 'AI not available for difficulty estimation'
            }
        
        # If no SERP data provided, create minimal data
        if not serp_data:
            serp_data = {
                'top_results': [],
                'has_featured_snippet': False,
                'has_local_pack': False,
                'ad_count': 0,
                'authority_sites': []
            }
        
        return await self.ai_service.estimate_difficulty(keyword, serp_data)
    
    async def close(self):
        """Close resources."""
        await self.analyzer.close()
