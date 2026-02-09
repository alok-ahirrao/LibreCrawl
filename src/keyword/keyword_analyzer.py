"""
Keyword Density Analyzer
Analyzes web pages for keyword frequency, density, and provides AI-enhanced insights.
"""

import re
import asyncio
import requests
from collections import Counter
from typing import Optional, List, Dict
from urllib.parse import urlparse
import logging

from bs4 import BeautifulSoup

from .ai_service import GeminiKeywordAI

logger = logging.getLogger(__name__)


# Common English stop words to filter out
STOP_WORDS = {
    # Articles
    'a', 'an', 'the',
    # Pronouns
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 
    'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 
    'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 
    'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 
    'these', 'those',
    # Verbs
    'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 
    'had', 'having', 'do', 'does', 'did', 'doing', 'would', 'should', 'could', 
    'ought', 'will', 'shall', 'can', 'may', 'might', 'must',
    # Prepositions
    'about', 'above', 'across', 'after', 'against', 'along', 'among', 'around',
    'at', 'before', 'behind', 'below', 'beneath', 'beside', 'between', 'beyond',
    'by', 'down', 'during', 'except', 'for', 'from', 'in', 'inside', 'into',
    'like', 'near', 'of', 'off', 'on', 'onto', 'out', 'outside', 'over',
    'past', 'since', 'through', 'throughout', 'till', 'to', 'toward', 'under',
    'underneath', 'until', 'up', 'upon', 'with', 'within', 'without',
    # Conjunctions
    'and', 'but', 'or', 'nor', 'for', 'yet', 'so', 'although', 'because',
    'since', 'unless', 'while', 'if', 'then', 'else', 'when', 'where', 'why',
    'how', 'than', 'whether', 'either', 'neither', 'both', 'each', 'few',
    'more', 'most', 'other', 'some', 'such', 'no', 'not', 'only', 'same',
    'as', 'also', 'just', 'even', 'still', 'already', 'always', 'never',
    # Common words
    'all', 'any', 'every', 'here', 'there', 'very', 'too', 'well', 'now',
    'get', 'got', 'go', 'goes', 'going', 'gone', 'come', 'comes', 'coming',
    'came', 'make', 'made', 'take', 'took', 'taken', 'give', 'gave', 'given',
    'know', 'knew', 'known', 'think', 'thought', 'see', 'saw', 'seen',
    'want', 'wanted', 'use', 'used', 'using', 'find', 'found', 'say', 'said',
    'let', 'put', 'keep', 'kept', 'tell', 'told', 'ask', 'asked', 'try',
    'tried', 'need', 'needed', 'feel', 'felt', 'become', 'became', 'leave',
    'left', 'call', 'called', 'first', 'last', 'long', 'great', 'little',
    'own', 'old', 'right', 'big', 'high', 'different', 'small', 'large',
    'next', 'early', 'young', 'important', 'public', 'bad', 'new', 'good',
    # Numbers
    'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
    # Web-specific
    'click', 'read', 'learn', 'view', 'share', 'follow', 'like', 'comment',
    'subscribe', 'menu', 'home', 'page', 'site', 'website', 'link', 'links',
    'copyright', 'privacy', 'policy', 'terms', 'conditions', 'contact', 'us',
}

# Hindi stop words (common ones)
HINDI_STOP_WORDS = {
    'का', 'की', 'के', 'है', 'हैं', 'था', 'थी', 'थे', 'को', 'से', 'में', 'पर',
    'और', 'या', 'एक', 'यह', 'वह', 'इस', 'उस', 'जो', 'कि', 'लिए', 'साथ',
    'अपने', 'होता', 'होती', 'होते', 'करना', 'करता', 'करती', 'करते',
}

ALL_STOP_WORDS = STOP_WORDS | HINDI_STOP_WORDS


class KeywordDensityAnalyzer:
    """
    Analyzes keyword density for web pages.
    
    Features:
    - Fetch and parse web pages
    - Extract visible text content
    - Count words and calculate frequencies
    - Calculate keyword density percentages
    - AI-enhanced semantic grouping and recommendations
    """
    
    def __init__(self, ai_service: Optional[GeminiKeywordAI] = None):
        """
        Initialize the analyzer.
        
        Args:
            ai_service: Optional GeminiKeywordAI instance for AI features
        """
        self.ai_service = ai_service or GeminiKeywordAI()
    
    async def close(self):
        """Close resources (no-op since we use sync requests now)."""
        pass
    
    async def fetch_page(self, url: str) -> str:
        """
        Fetch HTML content from a URL asynchronously.
        
        Args:
            url: The URL to fetch
            
        Returns:
            HTML content as string
        """
        # Ensure URL has protocol
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        def _sync_fetch():
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0'
            }
            
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            
            session = requests.Session()
            retry = Retry(
                total=3,
                read=3,
                connect=3,
                backoff_factor=0.5,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            
            try:
                response = session.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    return response.text
                else:
                    raise Exception(f"Failed to fetch URL: HTTP {response.status_code}")
            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                raise

        # Run blocking request in executor
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, _sync_fetch)
    
    def extract_text(self, html: str) -> dict:
        """
        Extract visible text content from HTML.
        
        Args:
            html: HTML content
            
        Returns:
            Dict with extracted text and metadata
        """
        soup = BeautifulSoup(html, 'lxml')
        
        # Extract title
        title = ""
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
        
        # Extract meta description
        meta_desc = ""
        meta_tag = soup.find('meta', attrs={'name': 'description'})
        if meta_tag:
            meta_desc = meta_tag.get('content', '')
        
        # Extract H1
        h1_text = ""
        h1_tag = soup.find('h1')
        if h1_tag:
            h1_text = h1_tag.get_text(strip=True)
        
        # Extract all headings
        headings = []
        for level in range(1, 7):
            for h in soup.find_all(f'h{level}'):
                headings.append({
                    'level': level,
                    'text': h.get_text(strip=True)
                })
        
        # Remove non-content elements
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 
                         'noscript', 'iframe', 'svg', 'form', 'button']):
            tag.decompose()
        
        # Remove common non-content classes/ids
        for selector in ['.nav', '.navigation', '.menu', '.footer', '.header',
                         '.sidebar', '.widget', '.ad', '.advertisement', 
                         '.cookie', '.popup', '#nav', '#menu', '#footer', '#header']:
            for el in soup.select(selector):
                el.decompose()
        
        # Get main content (prioritize main/article tags)
        main_content = soup.find('main') or soup.find('article') or soup.find('body')
        
        if main_content:
            body_text = main_content.get_text(separator=' ', strip=True)
        else:
            body_text = soup.get_text(separator=' ', strip=True)
        
        # Clean up whitespace
        body_text = re.sub(r'\s+', ' ', body_text).strip()
        
        return {
            'title': title,
            'meta_description': meta_desc,
            'h1': h1_text,
            'headings': headings,
            'body_text': body_text
        }
    
    def count_words(self, text: str) -> int:
        """
        Count total words in text.
        
        Args:
            text: Text content
            
        Returns:
            Total word count
        """
        words = re.findall(r'\b\w+\b', text.lower())
        return len(words)
    
    def extract_keywords(
        self, 
        text: str, 
        min_length: int = 3,
        top_n: int = 50,
        include_phrases: bool = True
    ) -> List[dict]:
        """
        Extract keywords and their frequencies from text.
        
        Args:
            text: Text content to analyze
            min_length: Minimum word length to include
            top_n: Number of top keywords to return
            include_phrases: Whether to include 2-3 word phrases
            
        Returns:
            List of {keyword, frequency, density} dicts
        """
        # Extract single words
        words = re.findall(r'\b\w+\b', text.lower())
        total_words = len(words)
        
        if total_words == 0:
            return []
        
        # Filter stop words and short words
        filtered_words = [
            w for w in words 
            if w not in ALL_STOP_WORDS 
            and len(w) >= min_length
            and not w.isdigit()
        ]
        
        # Count single word frequencies
        word_counts = Counter(filtered_words)
        
        # Extract 2-word and 3-word phrases if enabled
        phrase_counts = Counter()
        if include_phrases and len(words) > 2:
            # 2-word phrases
            for i in range(len(words) - 1):
                phrase = f"{words[i]} {words[i+1]}"
                # Skip if either word is a stop word or too short
                if (words[i] not in ALL_STOP_WORDS and 
                    words[i+1] not in ALL_STOP_WORDS and
                    len(words[i]) >= min_length and 
                    len(words[i+1]) >= min_length):
                    phrase_counts[phrase] += 1
            
            # 3-word phrases
            for i in range(len(words) - 2):
                phrase = f"{words[i]} {words[i+1]} {words[i+2]}"
                # At least first and last words should be meaningful
                if (words[i] not in ALL_STOP_WORDS and 
                    words[i+2] not in ALL_STOP_WORDS and
                    len(words[i]) >= min_length and 
                    len(words[i+2]) >= min_length):
                    phrase_counts[phrase] += 1
        
        # Combine and filter (minimum 2 occurrences for phrases)
        all_keywords = []
        
        # Add single words
        for word, count in word_counts.most_common(top_n * 2):
            density = (count / total_words) * 100
            all_keywords.append({
                'keyword': word,
                'frequency': count,
                'density': round(density, 2),
                'type': 'word'
            })
        
        # Add phrases (minimum 2 occurrences)
        if include_phrases:
            for phrase, count in phrase_counts.most_common(top_n):
                if count >= 2:
                    density = (count / total_words) * 100
                    all_keywords.append({
                        'keyword': phrase,
                        'frequency': count,
                        'density': round(density, 2),
                        'type': 'phrase'
                    })
        
        # Sort by frequency and return top N
        all_keywords.sort(key=lambda x: x['frequency'], reverse=True)
        return all_keywords[:top_n]
    
    def analyze_title_keywords(self, title: str, keywords: List[dict]) -> List[dict]:
        """
        Check which keywords appear in the title.
        
        Args:
            title: Page title
            keywords: List of keywords
            
        Returns:
            Keywords with 'in_title' flag added
        """
        title_lower = title.lower()
        for kw in keywords:
            kw['in_title'] = kw['keyword'] in title_lower
        return keywords
    
    def analyze_heading_keywords(
        self, 
        headings: List[dict], 
        keywords: List[dict]
    ) -> List[dict]:
        """
        Check which keywords appear in headings.
        
        Args:
            headings: List of {level, text} dicts
            keywords: List of keywords
            
        Returns:
            Keywords with 'in_headings' info added
        """
        heading_text = ' '.join(h['text'].lower() for h in headings)
        for kw in keywords:
            kw['in_headings'] = kw['keyword'] in heading_text
        return keywords
    
    def analyze_keyword_placement(
        self,
        body_text: str,
        keywords: List[dict],
        meta_description: str = ""
    ) -> List[dict]:
        """
        Analyze keyword placement in different sections of content.
        
        Args:
            body_text: Full body text
            keywords: List of keyword dicts
            meta_description: Meta description text
            
        Returns:
            Keywords with placement and prominence data
        """
        # Get first 200 words as "first paragraph"
        words = body_text.split()
        first_paragraph = ' '.join(words[:200]).lower() if len(words) > 200 else body_text.lower()
        last_section = ' '.join(words[-100:]).lower() if len(words) > 100 else body_text.lower()
        meta_lower = meta_description.lower()
        
        for kw in keywords:
            keyword = kw['keyword'].lower()
            
            # Check placement locations
            kw['in_first_paragraph'] = keyword in first_paragraph
            kw['in_meta'] = keyword in meta_lower
            kw['in_conclusion'] = keyword in last_section
            
            # Calculate prominence score (0-100)
            prominence = 0
            if kw.get('in_title', False):
                prominence += 30
            if kw.get('in_headings', False):
                prominence += 25
            if kw.get('in_first_paragraph', False):
                prominence += 20
            if kw.get('in_meta', False):
                prominence += 15
            if kw.get('in_conclusion', False):
                prominence += 10
            
            kw['prominence_score'] = min(prominence, 100)
            
        return keywords
    
    def calculate_content_quality(
        self,
        body_text: str,
        headings: List[dict],
        title: str,
        meta_description: str
    ) -> dict:
        """
        Calculate content quality metrics.
        
        Returns:
            Dict with quality metrics
        """
        words = body_text.split()
        word_count = len(words)
        
        # Sentence count (rough estimate)
        sentences = re.split(r'[.!?]+', body_text)
        sentence_count = len([s for s in sentences if s.strip()])
        
        # Average sentence length
        avg_sentence_length = word_count / max(sentence_count, 1)
        
        # Heading structure analysis
        h1_count = sum(1 for h in headings if h['level'] == 1)
        h2_count = sum(1 for h in headings if h['level'] == 2)
        h3_count = sum(1 for h in headings if h['level'] == 3)
        
        # Calculate readability score (simplified Flesch-Kincaid estimate)
        avg_word_length = sum(len(w) for w in words) / max(word_count, 1)
        readability = max(0, min(100, 206.835 - (1.015 * avg_sentence_length) - (84.6 * (avg_word_length / 5))))
        
        # Content length assessment
        if word_count < 300:
            length_assessment = 'too_short'
        elif word_count < 600:
            length_assessment = 'short'
        elif word_count < 1500:
            length_assessment = 'optimal'
        elif word_count < 3000:
            length_assessment = 'long'
        else:
            length_assessment = 'very_long'
        
        # Title and meta analysis
        title_length = len(title)
        meta_length = len(meta_description)
        
        title_status = 'good' if 30 <= title_length <= 60 else ('too_short' if title_length < 30 else 'too_long')
        meta_status = 'good' if 120 <= meta_length <= 160 else ('too_short' if meta_length < 120 else 'too_long')
        
        return {
            'word_count': word_count,
            'sentence_count': sentence_count,
            'avg_sentence_length': round(avg_sentence_length, 1),
            'readability_score': round(readability, 1),
            'length_assessment': length_assessment,
            'heading_structure': {
                'h1_count': h1_count,
                'h2_count': h2_count,
                'h3_count': h3_count,
                'total_headings': len(headings),
                'has_proper_h1': h1_count == 1,
                'has_subheadings': h2_count > 0
            },
            'title_analysis': {
                'length': title_length,
                'status': title_status
            },
            'meta_analysis': {
                'length': meta_length,
                'status': meta_status
            }
        }
    
    def generate_seo_recommendations(
        self,
        keywords: List[dict],
        content_quality: dict,
        title: str,
        h1: str
    ) -> List[dict]:
        """
        Generate actionable SEO recommendations.
        
        Returns:
            List of recommendation dicts with priority
        """
        recommendations = []
        
        # Check for keyword stuffing
        overstuffed = [kw for kw in keywords if kw['density'] > 3]
        if overstuffed:
            recommendations.append({
                'type': 'warning',
                'priority': 'high',
                'category': 'keyword_stuffing',
                'message': f"Reduce density for: {', '.join(kw['keyword'] for kw in overstuffed[:3])}",
                'details': f"{len(overstuffed)} keywords exceed 3% density threshold"
            })
        
        # Check for optimal keywords
        optimal = [kw for kw in keywords if 1 <= kw['density'] <= 3]
        if len(optimal) < 3:
            recommendations.append({
                'type': 'suggestion',
                'priority': 'medium',
                'category': 'keyword_optimization',
                'message': "Add more keywords in the 1-3% optimal density range",
                'details': f"Currently only {len(optimal)} keywords in optimal range"
            })
        
        # Content length check
        if content_quality['length_assessment'] == 'too_short':
            recommendations.append({
                'type': 'warning',
                'priority': 'high',
                'category': 'content_length',
                'message': "Content is too short for good SEO",
                'details': f"Aim for at least 600 words. Current: {content_quality['word_count']}"
            })
        
        # Heading structure
        if not content_quality['heading_structure']['has_proper_h1']:
            recommendations.append({
                'type': 'warning',
                'priority': 'high',
                'category': 'heading_structure',
                'message': "Page should have exactly one H1 tag",
                'details': f"Found {content_quality['heading_structure']['h1_count']} H1 tags"
            })
        
        if not content_quality['heading_structure']['has_subheadings']:
            recommendations.append({
                'type': 'suggestion',
                'priority': 'medium',
                'category': 'heading_structure',
                'message': "Add H2 subheadings to break up content",
                'details': "Subheadings improve readability and SEO"
            })
        
        # Title analysis
        if content_quality['title_analysis']['status'] != 'good':
            recommendations.append({
                'type': 'suggestion',
                'priority': 'medium',
                'category': 'meta_tags',
                'message': f"Title tag is {content_quality['title_analysis']['status'].replace('_', ' ')}",
                'details': f"Ideal length is 30-60 characters. Current: {content_quality['title_analysis']['length']}"
            })
        
        # Meta description
        if content_quality['meta_analysis']['status'] == 'too_short':
            recommendations.append({
                'type': 'suggestion',
                'priority': 'medium',
                'category': 'meta_tags',
                'message': "Meta description is too short",
                'details': f"Aim for 120-160 characters. Current: {content_quality['meta_analysis']['length']}"
            })
        
        # Readability
        if content_quality['readability_score'] < 40:
            recommendations.append({
                'type': 'suggestion',
                'priority': 'low',
                'category': 'readability',
                'message': "Content may be difficult to read",
                'details': "Consider shorter sentences and simpler words"
            })
        
        # Check for top keyword prominence
        top_keywords = keywords[:5]
        low_prominence = [kw for kw in top_keywords if kw.get('prominence_score', 0) < 30]
        if low_prominence:
            recommendations.append({
                'type': 'suggestion',
                'priority': 'medium',
                'category': 'keyword_placement',
                'message': "Top keywords have low prominence",
                'details': f"Consider adding '{low_prominence[0]['keyword']}' to title, headings, or first paragraph"
            })
        
        # Sort by priority
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        recommendations.sort(key=lambda x: priority_order.get(x['priority'], 3))
        
        return recommendations

    
    async def analyze_page(
        self, 
        url: str, 
        use_ai: bool = True,
        top_n: int = 50
    ) -> dict:
        """
        Perform complete keyword density analysis on a page.
        
        Args:
            url: URL to analyze
            use_ai: Whether to use AI for enhanced analysis
            top_n: Number of top keywords to return
            
        Returns:
            Complete analysis results
        """
        try:
            # Fetch page
            html = await self.fetch_page(url)
            
            # Extract text
            extracted = self.extract_text(html)
            
            # Count total words
            total_words = self.count_words(extracted['body_text'])
            
            # Extract keywords
            keywords = self.extract_keywords(
                extracted['body_text'], 
                top_n=top_n,
                include_phrases=True
            )
            
            # Check title presence
            keywords = self.analyze_title_keywords(extracted['title'], keywords)
            
            # Check heading presence
            keywords = self.analyze_heading_keywords(extracted['headings'], keywords)
            
            # NEW: Analyze keyword placement and prominence
            keywords = self.analyze_keyword_placement(
                extracted['body_text'],
                keywords,
                extracted['meta_description']
            )
            
            # NEW: Calculate content quality metrics
            content_quality = self.calculate_content_quality(
                extracted['body_text'],
                extracted['headings'],
                extracted['title'],
                extracted['meta_description']
            )
            
            # NEW: Generate SEO recommendations
            seo_recommendations = self.generate_seo_recommendations(
                keywords,
                content_quality,
                extracted['title'],
                extracted['h1']
            )
            
            result = {
                'url': url,
                'title': extracted['title'],
                'meta_description': extracted['meta_description'],
                'h1': extracted['h1'],
                'total_words': total_words,
                'unique_keywords': len(keywords),
                'keywords': keywords,
                'headings_count': len(extracted['headings']),
                'headings': extracted['headings'],  # Include full heading data
                'content_quality': content_quality,  # NEW
                'seo_recommendations': seo_recommendations,  # NEW
                'ai_analysis': None
            }
            
            # AI enhancement if enabled and available
            if use_ai and self.ai_service.is_available():
                try:
                    ai_analysis = await self.ai_service.analyze_keyword_density(
                        url=url,
                        keywords=keywords,
                        total_words=total_words
                    )
                    result['ai_analysis'] = ai_analysis
                except Exception as e:
                    logger.error(f"AI analysis failed: {e}")
                    result['ai_analysis'] = {'error': str(e)}
            
            return result
            
        except Exception as e:
            logger.error(f"Page analysis failed for {url}: {e}")
            return {
                'url': url,
                'error': str(e)
            }
    
    async def compare_pages(
        self, 
        urls: List[str],
        use_ai: bool = True
    ) -> dict:
        """
        Compare keyword density across multiple pages.
        
        Args:
            urls: List of URLs to compare
            use_ai: Whether to use AI analysis
            
        Returns:
            Comparison results
        """
        results = []
        for url in urls:
            result = await self.analyze_page(url, use_ai=use_ai, top_n=30)
            results.append(result)
        
        # Find common keywords across pages
        all_keywords = []
        for r in results:
            if 'keywords' in r:
                all_keywords.extend([kw['keyword'] for kw in r['keywords'][:20]])
        
        keyword_counts = Counter(all_keywords)
        common_keywords = [kw for kw, count in keyword_counts.items() if count > 1]
        
        return {
            'pages': results,
            'common_keywords': common_keywords,
            'total_pages': len(urls)
        }
