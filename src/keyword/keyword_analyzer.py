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
        Fetch HTML content from a URL.
        
        Args:
            url: The URL to fetch
            
        Returns:
            HTML content as string
        """
        # Ensure URL has protocol
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                return response.text
            else:
                raise Exception(f"Failed to fetch URL: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise
    
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
            
            result = {
                'url': url,
                'title': extracted['title'],
                'meta_description': extracted['meta_description'],
                'h1': extracted['h1'],
                'total_words': total_words,
                'unique_keywords': len(keywords),
                'keywords': keywords,
                'headings_count': len(extracted['headings']),
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
