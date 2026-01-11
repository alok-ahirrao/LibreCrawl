"""
Unit tests for the Keyword Normalizer module.
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.keyword.normalizer import (
    normalize,
    get_word_count,
    get_length_category,
    get_keyword_metadata,
    deduplicate_keywords,
    batch_normalize
)


class TestNormalize:
    """Tests for the normalize function."""
    
    def test_basic_normalization(self):
        """Test basic cleanup operations."""
        assert normalize("  DENTAL Clinic  ") == "dental clinic"
        assert normalize("Best Dentist!!!") == "best dentist"
        assert normalize("@price #cost") == "price cost"
    
    def test_preserves_hyphens_and_apostrophes(self):
        """Test that hyphens and apostrophes are kept."""
        assert normalize("full-time job") == "full-time job"
        assert normalize("doctor's appointment") == "doctor's appointment"
    
    def test_collapses_multiple_spaces(self):
        """Test that multiple spaces become one."""
        assert normalize("dental    clinic   near   me") == "dental clinic near me"
    
    def test_empty_string(self):
        """Test handling of empty input."""
        assert normalize("") == ""
        assert normalize(None) == ""


class TestWordCount:
    """Tests for word count function."""
    
    def test_word_count(self):
        assert get_word_count("dental clinic") == 2
        assert get_word_count("best dental clinic near me") == 5
        assert get_word_count("dentist") == 1
        assert get_word_count("") == 0


class TestLengthCategory:
    """Tests for length category classification."""
    
    def test_short_keywords(self):
        """1-2 words should be Short."""
        assert get_length_category("dentist") == "Short"
        assert get_length_category("dental clinic") == "Short"
    
    def test_medium_keywords(self):
        """3-5 words should be Medium."""
        assert get_length_category("best dental clinic boston") == "Medium"
        assert get_length_category("affordable dentist near me") == "Medium"
    
    def test_long_keywords(self):
        """6+ words should be Long."""
        assert get_length_category("best affordable dental clinic near me boston") == "Long"
        assert get_length_category("how much does dental implant surgery cost") == "Long"


class TestKeywordMetadata:
    """Tests for comprehensive metadata extraction."""
    
    def test_basic_metadata(self):
        meta = get_keyword_metadata("dental clinic near me")
        assert meta['normalized'] == "dental clinic near me"
        assert meta['word_count'] == 4
        assert meta['length_category'] == "Medium"
        assert meta['has_location'] == True
    
    def test_question_detection(self):
        meta = get_keyword_metadata("how much does dental implant cost")
        assert meta['is_question'] == True
    
    def test_transactional_detection(self):
        meta = get_keyword_metadata("buy cheap dental insurance")
        assert meta['has_transactional_intent'] == True


class TestDeduplication:
    """Tests for keyword deduplication."""
    
    def test_removes_duplicates(self):
        keywords = ["Dental Clinic", "dental clinic", "DENTAL CLINIC", "dentist"]
        result = deduplicate_keywords(keywords)
        assert len(result) == 2
        assert "Dental Clinic" in result  # First occurrence kept
        assert "dentist" in result


class TestBatchNormalize:
    """Tests for batch processing."""
    
    def test_batch_processing(self):
        keywords = ["Dental Clinic!!", "Best Dentist Near Me"]
        results = batch_normalize(keywords)
        assert len(results) == 2
        assert results[0]['normalized'] == "dental clinic"
        assert results[1]['length_category'] == "Medium"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
