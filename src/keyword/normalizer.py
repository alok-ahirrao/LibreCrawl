"""
Keyword Normalizer Module
Provides utilities for cleaning, standardizing, and classifying keywords.
"""

import re
from typing import Optional, Dict, List


def normalize(keyword: str) -> str:
    """
    Normalize a keyword for consistent processing.
    
    Operations:
    - Lowercase
    - Strip whitespace
    - Remove special characters (@, #, !, etc.)
    - Collapse multiple spaces
    
    Args:
        keyword: Raw keyword string
        
    Returns:
        Cleaned, normalized keyword
    """
    if not keyword:
        return ""
    
    # Lowercase
    kw = keyword.lower()
    
    # Remove special characters but keep hyphens and apostrophes
    kw = re.sub(r'[^\w\s\'-]', '', kw)
    
    # Collapse multiple spaces
    kw = re.sub(r'\s+', ' ', kw)
    
    # Strip leading/trailing whitespace
    kw = kw.strip()
    
    return kw


def get_word_count(keyword: str) -> int:
    """
    Get the number of words in a keyword.
    
    Args:
        keyword: The keyword to count
        
    Returns:
        Number of words
    """
    if not keyword:
        return 0
    
    normalized = normalize(keyword)
    return len(normalized.split())


def get_length_category(keyword: str) -> str:
    """
    Classify keyword by word count into SEO categories.
    
    Categories:
    - Short: 1-2 words (head terms, high competition)
    - Medium: 3-5 words (body terms, moderate competition)
    - Long: 6+ words (long-tail, lower competition)
    
    Args:
        keyword: The keyword to classify
        
    Returns:
        Category string: 'Short', 'Medium', or 'Long'
    """
    word_count = get_word_count(keyword)
    
    if word_count <= 2:
        return "Short"
    elif word_count <= 5:
        return "Medium"
    else:
        return "Long"


def get_keyword_metadata(keyword: str) -> Dict:
    """
    Get comprehensive metadata for a keyword.
    
    Args:
        keyword: The keyword to analyze
        
    Returns:
        Dict with normalized keyword and metadata
    """
    normalized = normalize(keyword)
    word_count = get_word_count(keyword)
    
    return {
        'original': keyword,
        'normalized': normalized,
        'word_count': word_count,
        'length_category': get_length_category(keyword),
        'has_location': _has_location_signal(normalized),
        'is_question': _is_question(normalized),
        'has_transactional_intent': _has_transactional_signal(normalized)
    }


def _has_location_signal(keyword: str) -> bool:
    """Check if keyword has location-based signals."""
    location_patterns = [
        r'\bnear me\b',
        r'\bnearby\b',
        r'\bin [a-z]+\b',
        r'\b[a-z]+ city\b',
        r'\blocal\b'
    ]
    return any(re.search(p, keyword) for p in location_patterns)


def _is_question(keyword: str) -> bool:
    """Check if keyword is a question."""
    question_starters = [
        'how', 'what', 'why', 'when', 'where', 'which', 'who',
        'can', 'does', 'is', 'are', 'should', 'will', 'would'
    ]
    first_word = keyword.split()[0] if keyword else ''
    return first_word in question_starters or '?' in keyword


def _has_transactional_signal(keyword: str) -> bool:
    """Check if keyword has transactional intent signals."""
    transactional_words = [
        'buy', 'price', 'cost', 'cheap', 'affordable', 'discount',
        'order', 'purchase', 'hire', 'book', 'schedule', 'quote',
        'free trial', 'coupon', 'deal', 'sale'
    ]
    return any(word in keyword for word in transactional_words)


def deduplicate_keywords(keywords: List[str]) -> List[str]:
    """
    Remove duplicate keywords (case-insensitive).
    
    Args:
        keywords: List of keywords
        
    Returns:
        Deduplicated list preserving order
    """
    seen = set()
    unique = []
    
    for kw in keywords:
        normalized = normalize(kw)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(kw)
    
    return unique


def batch_normalize(keywords: List[str]) -> List[Dict]:
    """
    Normalize and get metadata for a batch of keywords.
    
    Args:
        keywords: List of keywords
        
    Returns:
        List of keyword metadata dicts
    """
    return [get_keyword_metadata(kw) for kw in keywords if kw]
