"""
Keyword Research Module
Provides AI-enhanced keyword analysis tools.
"""

from .ai_service import GeminiKeywordAI
from .keyword_analyzer import KeywordDensityAnalyzer
from .competitor_keywords import CompetitorKeywordResearcher
from .keyword_data import KeywordDataService
from .cannibalization import KeywordCannibalizationDetector
from .content_mapper import ContentMapper
from .research_workflow import KeywordResearchWorkflow, run_keyword_research
from .normalizer import (
    normalize,
    get_word_count,
    get_length_category,
    get_keyword_metadata,
    deduplicate_keywords,
    batch_normalize
)

__all__ = [
    'GeminiKeywordAI',
    'KeywordDensityAnalyzer', 
    'CompetitorKeywordResearcher',
    'KeywordDataService',
    'KeywordCannibalizationDetector',
    'ContentMapper',
    'KeywordResearchWorkflow',
    'run_keyword_research',
    # Normalizer utilities
    'normalize',
    'get_word_count',
    'get_length_category',
    'get_keyword_metadata',
    'deduplicate_keywords',
    'batch_normalize'
]
