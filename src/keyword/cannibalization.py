"""
Keyword Cannibalization Detection
Identifies when multiple pages on a site compete for the same keywords.
"""

import asyncio
import logging
from typing import Optional, List, Dict, Set
from collections import defaultdict
from urllib.parse import urlparse, urljoin
import requests
from bs4 import BeautifulSoup

from .keyword_analyzer import KeywordDensityAnalyzer
from .ai_service import GeminiKeywordAI

logger = logging.getLogger(__name__)


class KeywordCannibalizationDetector:
    """
    Detects keyword cannibalization across a website.
    
    Cannibalization occurs when multiple pages target the same keyword,
    causing them to compete against each other in search rankings.
    
    Features:
    - Crawl site pages (via sitemap or provided URLs)
    - Build keyword-to-page index
    - Identify overlapping keywords
    - Score cannibalization risk
    - Provide consolidation recommendations
    """
    
    def __init__(self, ai_service: Optional[GeminiKeywordAI] = None):
        """
        Initialize the detector.
        
        Args:
            ai_service: Optional GeminiKeywordAI instance for enhanced analysis
        """
        self.ai_service = ai_service or GeminiKeywordAI()
        self.analyzer = KeywordDensityAnalyzer(self.ai_service)
        
    async def close(self):
        """Close resources."""
        await self.analyzer.close()
    
    def fetch_sitemap_urls(self, sitemap_url: str, max_urls: int = 50) -> List[str]:
        """
        Fetch URLs from a sitemap.
        
        Args:
            sitemap_url: URL to sitemap.xml
            max_urls: Maximum URLs to return
            
        Returns:
            List of page URLs from sitemap
        """
        urls = []
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(sitemap_url, headers=headers, timeout=15)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'lxml-xml')
                
                # Handle sitemap index (links to other sitemaps)
                sitemap_locs = soup.find_all('sitemap')
                if sitemap_locs:
                    # It's a sitemap index, get first sub-sitemap
                    for sm in sitemap_locs[:3]:  # Limit sub-sitemaps
                        loc = sm.find('loc')
                        if loc:
                            sub_urls = self.fetch_sitemap_urls(loc.text.strip(), max_urls=max_urls // 3)
                            urls.extend(sub_urls)
                            if len(urls) >= max_urls:
                                break
                else:
                    # Regular sitemap with URLs
                    url_locs = soup.find_all('url')
                    for url_entry in url_locs:
                        loc = url_entry.find('loc')
                        if loc:
                            urls.append(loc.text.strip())
                            if len(urls) >= max_urls:
                                break
                                
        except Exception as e:
            logger.error(f"Failed to fetch sitemap {sitemap_url}: {e}")
        
        return urls[:max_urls]
    
    def discover_site_urls(self, domain: str, max_urls: int = 30) -> List[str]:
        """
        Discover URLs for a domain by trying common sitemap locations.
        
        Args:
            domain: Domain to discover URLs for
            max_urls: Maximum URLs to return
            
        Returns:
            List of discovered URLs
        """
        # Normalize domain
        if not domain.startswith(('http://', 'https://')):
            domain = f'https://{domain}'
        
        parsed = urlparse(domain)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Try common sitemap locations
        sitemap_locations = [
            f"{base_url}/sitemap.xml",
            f"{base_url}/sitemap_index.xml",
            f"{base_url}/sitemap-index.xml",
            f"{base_url}/wp-sitemap.xml",
            f"{base_url}/page-sitemap.xml",
        ]
        
        for sitemap_url in sitemap_locations:
            urls = self.fetch_sitemap_urls(sitemap_url, max_urls)
            if urls:
                logger.info(f"Found {len(urls)} URLs from {sitemap_url}")
                return urls
        
        # Fallback: Return just the homepage
        logger.warning(f"No sitemap found for {base_url}, using homepage only")
        return [base_url]
    
    async def analyze_pages(
        self, 
        urls: List[str], 
        top_n_keywords: int = 20
    ) -> Dict[str, Dict]:
        """
        Analyze multiple pages for their keywords.
        
        Args:
            urls: List of URLs to analyze
            top_n_keywords: Top keywords to extract per page
            
        Returns:
            Dict mapping URL to keyword analysis
        """
        page_keywords = {}
        
        for url in urls:
            try:
                result = await self.analyzer.analyze_page(
                    url, 
                    use_ai=False,  # Skip AI to speed up
                    top_n=top_n_keywords
                )
                
                if not result.get('error'):
                    page_keywords[url] = {
                        'title': result.get('title', ''),
                        'h1': result.get('h1', ''),
                        'keywords': result.get('keywords', []),
                        'total_words': result.get('total_words', 0)
                    }
                else:
                    logger.warning(f"Failed to analyze {url}: {result.get('error')}")
                    
            except Exception as e:
                logger.error(f"Error analyzing {url}: {e}")
        
        return page_keywords
    
    def build_keyword_index(
        self, 
        page_keywords: Dict[str, Dict],
        min_density: float = 0.5
    ) -> Dict[str, List[Dict]]:
        """
        Build an index of keywords to pages that target them.
        
        Args:
            page_keywords: Dict of URL -> keyword analysis
            min_density: Minimum keyword density to consider as "targeting"
            
        Returns:
            Dict mapping keyword -> list of page info dicts
        """
        keyword_index = defaultdict(list)
        
        for url, data in page_keywords.items():
            for kw in data.get('keywords', []):
                # Only include keywords with significant density
                if kw.get('density', 0) >= min_density:
                    keyword_index[kw['keyword'].lower()].append({
                        'url': url,
                        'title': data.get('title', ''),
                        'density': kw['density'],
                        'frequency': kw['frequency'],
                        'in_title': kw.get('in_title', False),
                        'in_headings': kw.get('in_headings', False),
                        'prominence_score': kw.get('prominence_score', 0)
                    })
        
        return dict(keyword_index)
    
    def detect_cannibalization(
        self, 
        keyword_index: Dict[str, List[Dict]],
        min_pages: int = 2
    ) -> List[Dict]:
        """
        Detect cannibalization issues from keyword index.
        
        Args:
            keyword_index: Keyword to pages mapping
            min_pages: Minimum pages to consider as cannibalization
            
        Returns:
            List of cannibalization issues with recommendations
        """
        issues = []
        
        for keyword, pages in keyword_index.items():
            if len(pages) >= min_pages:
                # Sort pages by prominence (higher = more optimized)
                pages_sorted = sorted(
                    pages, 
                    key=lambda x: (x['prominence_score'], x['density']), 
                    reverse=True
                )
                
                # Calculate risk score
                # Higher if: more pages, similar prominence, both in titles
                primary = pages_sorted[0]
                secondary = pages_sorted[1:]
                
                # Check if multiple pages have keyword in title/headings
                title_count = sum(1 for p in pages if p['in_title'])
                heading_count = sum(1 for p in pages if p['in_headings'])
                
                # Risk factors
                risk_score = 0
                risk_factors = []
                
                if len(pages) >= 3:
                    risk_score += 30
                    risk_factors.append(f"{len(pages)} pages competing")
                elif len(pages) == 2:
                    risk_score += 15
                    risk_factors.append("2 pages competing")
                
                if title_count >= 2:
                    risk_score += 40
                    risk_factors.append(f"{title_count} pages have keyword in title")
                
                if heading_count >= 2:
                    risk_score += 20
                    risk_factors.append(f"{heading_count} pages have keyword in headings")
                
                # Similar prominence = higher competition
                if secondary and abs(primary['prominence_score'] - secondary[0]['prominence_score']) < 20:
                    risk_score += 10
                    risk_factors.append("Pages have similar optimization level")
                
                # Determine recommendation
                if risk_score >= 50:
                    severity = 'high'
                    recommendation = 'Consolidate pages or differentiate content significantly'
                elif risk_score >= 30:
                    severity = 'medium'
                    recommendation = 'Review content overlap and consider 301 redirect or noindex'
                else:
                    severity = 'low'
                    recommendation = 'Minor overlap - ensure primary page has stronger optimization'
                
                issues.append({
                    'keyword': keyword,
                    'severity': severity,
                    'risk_score': min(100, risk_score),
                    'page_count': len(pages),
                    'primary_page': {
                        'url': primary['url'],
                        'title': primary['title'],
                        'prominence_score': primary['prominence_score']
                    },
                    'competing_pages': [
                        {
                            'url': p['url'],
                            'title': p['title'],
                            'prominence_score': p['prominence_score']
                        } for p in secondary[:4]  # Limit to 4 secondary pages
                    ],
                    'risk_factors': risk_factors,
                    'recommendation': recommendation
                })
        
        # Sort by risk score
        issues.sort(key=lambda x: x['risk_score'], reverse=True)
        
        return issues
    
    async def analyze_domain(
        self,
        domain: str,
        urls: List[str] = None,
        max_pages: int = 30,
        min_density: float = 0.5
    ) -> Dict:
        """
        Full cannibalization analysis for a domain.
        
        Args:
            domain: Domain to analyze
            urls: Optional specific URLs to analyze (overrides sitemap discovery)
            max_pages: Maximum pages to analyze
            min_density: Minimum keyword density to consider
            
        Returns:
            Complete cannibalization report
        """
        # Discover URLs if not provided
        if not urls:
            urls = self.discover_site_urls(domain, max_urls=max_pages)
        
        logger.info(f"Analyzing {len(urls)} pages for cannibalization")
        
        # Analyze all pages
        page_keywords = await self.analyze_pages(urls[:max_pages])
        
        if not page_keywords:
            return {
                'domain': domain,
                'error': 'Failed to analyze any pages',
                'pages_analyzed': 0
            }
        
        # Build keyword index
        keyword_index = self.build_keyword_index(page_keywords, min_density)
        
        # Detect issues
        issues = self.detect_cannibalization(keyword_index)
        
        # Categorize by severity
        high_severity = [i for i in issues if i['severity'] == 'high']
        medium_severity = [i for i in issues if i['severity'] == 'medium']
        low_severity = [i for i in issues if i['severity'] == 'low']
        
        return {
            'domain': domain,
            'pages_analyzed': len(page_keywords),
            'total_keywords_indexed': len(keyword_index),
            'cannibalization_issues': {
                'total': len(issues),
                'high_severity': len(high_severity),
                'medium_severity': len(medium_severity),
                'low_severity': len(low_severity)
            },
            'issues': issues[:30],  # Top 30 issues
            'high_priority_issues': high_severity[:10],
            'summary': self._generate_summary(issues, len(page_keywords))
        }
    
    def _generate_summary(self, issues: List[Dict], pages_analyzed: int) -> str:
        """Generate a human-readable summary of cannibalization analysis."""
        if not issues:
            return f"No significant cannibalization issues detected across {pages_analyzed} pages. Your site structure appears healthy."
        
        high = len([i for i in issues if i['severity'] == 'high'])
        medium = len([i for i in issues if i['severity'] == 'medium'])
        
        if high >= 5:
            return f"Critical: Found {high} high-severity cannibalization issues across {pages_analyzed} pages. Immediate action required to consolidate competing pages."
        elif high >= 2:
            return f"Warning: Found {high} high-severity and {medium} medium-severity issues. Review and consolidate pages targeting the same keywords."
        elif medium >= 5:
            return f"Moderate: Found {medium} medium-severity issues. Consider content differentiation or strategic redirects."
        else:
            return f"Minor: Found {len(issues)} potential overlaps across {pages_analyzed} pages. Low priority but worth reviewing."
