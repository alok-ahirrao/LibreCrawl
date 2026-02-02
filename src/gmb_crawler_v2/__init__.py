"""
GMB Crawler V2 - Google Maps Complete Data Extraction

A comprehensive, isolated module for extracting all 10+ attribute
categories from Google Maps business listings.

This module is completely independent from gmb_core.
"""

from .crawler import GMBCrawlerV2, extract_gmb_data, extract_gmb_batch
from .routes import gmb_v2_bp
from .driver import GMBDriverV2


# Parser imports for advanced usage
from .parsers import (
    BaseParser,
    BasicDetailsParser,
    LocationDataParser,
    ContactInfoParser,
    MediaAssetsParser,
    ReviewsRatingsParser,
    BusinessAttributesParser,
    OperatingHoursParser,
    PopularTimesParser,
    CompetitiveDataParser,
    AdditionalDataParser,
)

__version__ = '2.0.0'

__all__ = [
    # Main classes
    'GMBCrawlerV2',
    'GMBDriverV2',

    
    # Flask routes
    'gmb_v2_bp',
    
    # Convenience functions
    'extract_gmb_data',
    'extract_gmb_batch',
    
    # Parsers
    'BaseParser',
    'BasicDetailsParser',
    'LocationDataParser',
    'ContactInfoParser',
    'MediaAssetsParser',
    'ReviewsRatingsParser',
    'BusinessAttributesParser',
    'OperatingHoursParser',
    'PopularTimesParser',
    'CompetitiveDataParser',
    'AdditionalDataParser',
]
