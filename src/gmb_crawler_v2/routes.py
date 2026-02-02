"""
GMB Crawler V2 - Flask API Routes

REST API endpoints for the GMB data extraction crawler.
"""

from flask import Blueprint, request, jsonify
from functools import wraps
import logging
import traceback

from .crawler import GMBCrawlerV2

logger = logging.getLogger(__name__)

# Create blueprint
gmb_v2_bp = Blueprint('gmb_v2', __name__, url_prefix='/api/gmb-v2')

# Lazy crawler instance (created on first request)
_crawler_instance = None


def get_crawler() -> GMBCrawlerV2:
    """Get or create crawler instance."""
    global _crawler_instance
    if _crawler_instance is None:
        _crawler_instance = GMBCrawlerV2(
            headless=False
        )
    return _crawler_instance


def handle_errors(f):
    """Error handling decorator for routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error(f"API error: {e}\n{traceback.format_exc()}")
            return jsonify({
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__
            }), 500
    return decorated


# ==================== Health Check ====================

@gmb_v2_bp.route('/status', methods=['GET'])
def status():
    """Health check endpoint."""
    return jsonify({
        'status': 'ok',
        'service': 'GMB Crawler V2',
        'version': '2.0.0',
        'categories': GMBCrawlerV2.CATEGORIES
    })


# ==================== Extract Endpoints ====================

@gmb_v2_bp.route('/extract', methods=['POST'])
@handle_errors
def extract_full():
    """
    Extract full business data from URL.
    
    Request body:
    {
        "url": "https://www.google.com/maps/place/...",
        "query": "Business Name",  // Alternative to url
        "location": "City, State",  // Optional location context
        "lat": 40.7128,  // Optional latitude
        "lng": -74.0060,  // Optional longitude
        "save": true  // Whether to save to database
    }
    
    Returns:
    {
        "success": true,
        "data": { ... all extracted data ... }
    }
    """
    data = request.get_json() or {}
    
    url = data.get('url')
    query = data.get('query')
    location = data.get('location')
    lat = data.get('lat')
    lng = data.get('lng')
    lng = data.get('lng')
    
    if not url and not query:
        return jsonify({
            'success': False,
            'error': 'Either url or query is required'
        }), 400
    
    crawler = get_crawler()
    
    # Use URL if provided, otherwise use query
    target = url if url else query
    
    result = crawler.extract_full(
        url_or_query=target,
        location=location,
        lat=lat,
        lng=lng
    )
    
    return jsonify({
        'success': result.get('extraction_metadata', {}).get('success', False),
        'data': result
    })


@gmb_v2_bp.route('/extract/partial', methods=['POST'])
@handle_errors
def extract_partial():
    """
    Extract specific categories only.
    
    Request body:
    {
        "url": "https://www.google.com/maps/place/...",
        "categories": ["basic_details", "reviews_ratings", "contact_info"],
        "lat": 40.7128,
        "lng": -74.0060
    }
    
    Available categories:
    - basic_details
    - location_data
    - contact_info
    - media_assets
    - reviews_ratings
    - business_attributes
    - operating_hours
    - popular_times
    - competitive_data
    - additional_data
    """
    data = request.get_json() or {}
    
    url = data.get('url')
    categories = data.get('categories', ['basic_details'])
    lat = data.get('lat')
    lng = data.get('lng')
    
    if not url:
        return jsonify({
            'success': False,
            'error': 'url is required'
        }), 400
    
    if not isinstance(categories, list):
        return jsonify({
            'success': False,
            'error': 'categories must be a list'
        }), 400
    
    crawler = get_crawler()
    
    result = crawler.extract_partial(
        url=url,
        categories=categories,
        lat=lat,
        lng=lng
    )
    
    return jsonify({
        'success': True,
        'data': result,
        'categories_extracted': list(result.keys())
    })


@gmb_v2_bp.route('/extract/batch', methods=['POST'])
@handle_errors
def extract_batch():
    """
    Extract data from multiple URLs.
    
    Request body:
    {
        "urls": [
            "https://www.google.com/maps/place/...",
            "https://www.google.com/maps/place/..."
        ],
        "delay": 2.0  // Delay between requests in seconds
    }
    
    Returns:
    {
        "success": true,
        "results": [ ... array of results ... ],
        "total": 5,
        "successful": 4,
        "failed": 1
    }
    """
    data = request.get_json() or {}
    
    urls = data.get('urls', [])
    delay = data.get('delay', 2.0)
    
    if not urls:
        return jsonify({
            'success': False,
            'error': 'urls list is required'
        }), 400
    
    if len(urls) > 20:
        return jsonify({
            'success': False,
            'error': 'Maximum 20 URLs per batch'
        }), 400
    
    crawler = get_crawler()
    
    results = crawler.extract_batch(
        urls=urls,
        delay_between=delay
    )
    
    successful = sum(
        1 for r in results 
        if r.get('extraction_metadata', {}).get('success')
    )
    
    return jsonify({
        'success': True,
        'results': results,
        'total': len(results),
        'successful': successful,
        'failed': len(results) - successful
    })





# ==================== Info Endpoints ====================

@gmb_v2_bp.route('/categories', methods=['GET'])
def list_categories():
    """List available data categories."""
    return jsonify({
        'categories': GMBCrawlerV2.CATEGORIES,
        'descriptions': {
            'basic_details': 'Business name, CID, Place ID, categories, description',
            'location_data': 'Address, coordinates, address components, Plus Code',
            'contact_info': 'Phone, email, website, social media links',
            'media_assets': 'Photos, videos, logo, Street View availability',
            'reviews_ratings': 'Rating, review count, distribution, recent reviews',
            'business_attributes': '50+ attribute flags (dine-in, accessibility, etc.)',
            'operating_hours': 'Weekly hours, current status, special hours',
            'popular_times': 'Hourly traffic, live busyness, typical time spent',
            'competitive_data': 'People also search, similar places, nearby',
            'additional_data': 'Q&A, posts, menu, services, products, booking links',
        }
    })
