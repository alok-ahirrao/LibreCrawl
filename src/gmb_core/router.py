"""
GMB Core Flask Router
REST API endpoints for GMB authentication, locations, reviews, and grid scanning.
"""
from flask import Blueprint, request, jsonify, session, redirect
from .api.auth import GMBAuthManager
from .api.client import GMBClient
from .crawler.geo_driver import GeoCrawlerDriver
from .crawler.parsers import GoogleMapsParser
from .models import init_gmb_tables, save_location, save_review, get_cached_serp, save_serp_cache
from .config import config
from .logger import log
import threading
import json
import time

# Create Flask Blueprint
gmb_bp = Blueprint('gmb', __name__, url_prefix='/api/gmb')


def get_user_id():
    """Get current user ID from session."""
    user_id = session.get('user_id')
    if not user_id:
        raise Exception("User not logged in")
    return user_id


def get_auth_manager():
    """Get GMBAuthManager for current user."""
    return GMBAuthManager(get_user_id())


def get_client():
    """Get GMBClient for current user."""
    return GMBClient(get_user_id())


@gmb_bp.record_once
def on_load(state):
    """Ensure GMB tables exist on startup."""
    init_gmb_tables()


# ==================== Status ====================

@gmb_bp.route('/status', methods=['GET'])
def status():
    """Health check and configuration status."""
    return jsonify({
        'status': 'GMB Core Module Active',
        'version': '0.2.0',
        'configured': config.is_configured(),
        'proxy_enabled': config.PROXY_ENABLED
    })


# ==================== Business Search ====================

@gmb_bp.route('/business/search', methods=['POST'])
def search_business():
    """
    Search for a business by name on Google Maps.
    Returns structured business data with confidence scoring.
    
    Body: {
        "query": "Pizza Hut",
        "location": "New York"  # Optional
    }
    
    Returns: {
        "success": true,
        "confidence": 0.92,
        "matches": [
            {
                "place_id": "...",
                "name": "Pizza Hut",
                "rating": 4.2,
                "review_count": 156,
                "address": "123 Main St, NY 10001",
                "phone": "(555) 123-4567",
                "lat": 40.7580,
                "lng": -73.9855,
                "is_open": true,
                "category": "Pizza Restaurant"
            }
        ]
    }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    query = data.get('query', '').strip()
    location = data.get('location', '').strip()
    
    # [NEW] Optional lat/lng for accurate geo-context
    lat = data.get('lat')
    lng = data.get('lng')
    
    if not query or len(query) < 2:
        return jsonify({
            'success': False, 
            'error': 'Query must be at least 2 characters'
        }), 400
    
    log.debug(f"[BusinessSearch] Searching for: '{query}' in '{location or 'default location'}' (Lat/Lng: {lat}, {lng})")
    
    try:
        # Initialize crawler
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        
        # Perform search - RETURNS TUPLE (html, final_url)
        # If location string is provided but no lat/lng, geo_driver will skip geo-spoofing
        # and let Google naturally find businesses based on the search query
        html, final_url = driver.search_business(
            query, 
            location if location else None,
            lat=float(lat) if lat is not None else None,
            lng=float(lng) if lng is not None else None
        )
        
        if not html:
            return jsonify({
                'success': True,
                'confidence': 0,
                'matches': [],
                'message': "We couldn't find this business on Google. Please refine the name or add a location."
            })
        
        # Parse results
        parser = GoogleMapsParser()
        result = parser.parse_business_search(html, query)

        # Enhanced Coordinate Extraction using Validated Final URL
        if final_url and result.get('matches'):
            # Extract coordinates from the final URL if general matches lack them
            url_lat, url_lng = parser._extract_coordinates_from_url(final_url)
            
            # Update matches if they lack coordinates (especially single match case)
            for match in result['matches']:
                if (match.get('lat') is None or match.get('lat') == 0) and url_lat is not None:
                    match['lat'] = url_lat
                    match['lng'] = url_lng
                    log.debug(f"[Router] Backfilled coordinates from Final URL: {url_lat}, {url_lng}")
        
        log.debug(f"[BusinessSearch] Found {len(result['matches'])} matches with confidence {result['confidence']:.2f}")
        
        return jsonify({
            'success': True,
            'confidence': result['confidence'],
            'matches': result['matches']
        })
        
    except Exception as e:
        log.debug(f"[BusinessSearch] Error: {e}")
        log.error(f"Stack trace: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== Business URL Resolution ====================

@gmb_bp.route('/business/resolve-url', methods=['POST'])
def resolve_business_url():
    """
    Resolve a Google Maps URL to structured business data.
    Supports various URL formats:
    - https://maps.app.goo.gl/xxxxx (short URLs)
    - https://www.google.com/maps/place/...
    - https://goo.gl/maps/xxxxx
    
    Body: {
        "url": "https://maps.app.goo.gl/L3q31V3rQKeATSHF9"
    }
    
    Returns: {
        "success": true,
        "business": {
            "place_id": "...",
            "name": "Business Name",
            "rating": 4.2,
            "review_count": 156,
            "address": "123 Main St",
            "phone": "(555) 123-4567",
            "lat": 40.7580,
            "lng": -73.9855,
            "category": "Restaurant"
        }
    }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    data = request.get_json()
    url = data.get('url', '').strip()
    
    if not url:
        return jsonify({
            'success': False, 
            'error': 'URL is required'
        }), 400
    
    # Validate URL format
    import re
    valid_patterns = [
        r'maps\.app\.goo\.gl',
        r'goo\.gl/maps',
        r'google\.[a-z]+/maps',
        r'maps\.google\.[a-z]+'
    ]
    
    if not any(re.search(pattern, url) for pattern in valid_patterns):
        return jsonify({
            'success': False,
            'error': 'Invalid Google Maps URL format. Please use a Google Maps share link.'
        }), 400
    
    log.debug(f"[BusinessResolveURL] Resolving URL: {url}")
    
    try:
        # Initialize crawler
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        
        # Navigate to the URL and get the page content + final URL after redirect
        html, final_url = driver.scan_place_details(url)
        
        if not html:
            return jsonify({
                'success': False,
                'error': "Could not load the business page. Please check the URL and try again."
            }), 400
        
        # Parse the place details
        parser = GoogleMapsParser()
        details = parser.parse_place_details(html)
        
        if not details.get('name'):
            return jsonify({
                'success': False,
                'error': "Could not extract business information from this URL."
            }), 400
        
        # Try to extract coordinates from the final URL (after redirect - has coords)
        lat, lng = None, None
        if final_url:
            lat, lng = parser._extract_coordinates_from_url(final_url)
            log.debug(f"[BusinessResolveURL] Extracted coords from final URL: {lat}, {lng}")
        
        # Fallback: try original URL
        if lat is None or lng is None:
            lat, lng = parser._extract_coordinates_from_url(url)
        
        # If still no coords, try to extract from HTML
        if lat is None or lng is None:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, 'html.parser')
            # Look for coordinates in meta tags or data attributes
            meta_content = soup.find('meta', {'content': re.compile(r'@[-0-9.]+,[-0-9.]+')})
            if meta_content:
                coord_match = re.search(r'@([-0-9.]+),([-0-9.]+)', meta_content.get('content', ''))
                if coord_match:
                    lat = float(coord_match.group(1))
                    lng = float(coord_match.group(2))
        
        # Try extracting Place ID from URL
        place_id = parser._extract_place_id(url)
        
        # Build response
        business = {
            'place_id': place_id or '',
            'name': details.get('name'),
            'rating': details.get('rating') or 0,
            'review_count': details.get('review_count') or 0,
            'address': details.get('address') or '',
            'phone': details.get('phone') or None,
            'lat': lat or 0,
            'lng': lng or 0,
            'category': details.get('primary_category') or 'Business',
            'website': details.get('website') or None,
            'is_open': None  # Would need additional parsing
        }
        
        log.debug(f"[BusinessResolveURL] Resolved: {business['name']} at ({lat}, {lng})")
        
        return jsonify({
            'success': True,
            'business': business
        })
        
    except Exception as e:
        log.debug(f"[BusinessResolveURL] Error: {e}")
        log.error(f"Stack trace: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# ==================== OAuth Endpoints ====================

@gmb_bp.route('/auth/url', methods=['GET'])
def get_auth_url():
    """
    Generate Google OAuth2 consent URL.
    Returns URL to redirect user to for authorization.
    """
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        mgr = get_auth_manager()
        result = mgr.get_auth_url()
        
        # Store state in session for CSRF verification
        session['gmb_oauth_state'] = result['state']
        
        return jsonify({
            'success': True,
            'url': result['url'],
            'state': result['state']
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/auth/callback', methods=['POST'])
def auth_callback():
    """
    Exchange authorization code for tokens.
    Expected body: { "code": "...", "state": "..." }
    """
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        data = request.get_json()
        code = data.get('code')
        state = data.get('state')
        
        if not code:
            return jsonify({'success': False, 'error': 'Missing authorization code'}), 400
        
        # Verify state for CSRF protection
        stored_state = session.get('gmb_oauth_state')
        if state and stored_state and state != stored_state:
            return jsonify({'success': False, 'error': 'Invalid state parameter'}), 400
        
        mgr = get_auth_manager()
        result = mgr.exchange_code(code)
        
        # Clear state from session
        session.pop('gmb_oauth_state', None)
        
        return jsonify({
            'success': True,
            'account_id': result['account_id'],
            'email': result['email']
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Accounts ====================

@gmb_bp.route('/accounts', methods=['GET'])
def list_gmb_accounts():
    """List all connected GMB accounts for current user."""
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        mgr = get_auth_manager()
        accounts = mgr.list_accounts()
        
        return jsonify({
            'success': True,
            'accounts': accounts
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/accounts/<int:account_id>', methods=['DELETE'])
def revoke_account(account_id):
    """Revoke access for a GMB account."""
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        mgr = get_auth_manager()
        success = mgr.revoke_access(account_id)
        
        return jsonify({'success': success})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Locations ====================

@gmb_bp.route('/accounts/<int:account_id>/google-accounts', methods=['GET'])
def list_google_accounts(account_id):
    """List Google Business Accounts accessible with this connection."""
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        client = get_client()
        accounts = client.list_accounts(account_id)
        
        return jsonify({
            'success': True,
            'google_accounts': accounts
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/accounts/<int:account_id>/locations', methods=['GET'])
def list_locations(account_id):
    """
    List locations for a GMB account.
    Query params: google_account (required) - The Google account name (e.g., accounts/123)
    """
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        google_account = request.args.get('google_account')
        if not google_account:
            return jsonify({'success': False, 'error': 'google_account parameter required'}), 400
        
        client = get_client()
        locations = client.list_locations(account_id, google_account)
        
        # Save locations to database
        saved_ids = []
        for loc in locations:
            loc_id = save_location(account_id, loc)
            saved_ids.append(loc_id)
        
        return jsonify({
            'success': True,
            'locations': locations,
            'saved_count': len(saved_ids)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Reviews ====================

@gmb_bp.route('/locations/<int:location_id>/reviews', methods=['GET'])
def get_reviews(location_id):
    """
    Fetch reviews for a location.
    Query params: 
        account_id (required) - The GMB account ID
        location_name (required) - The Google location name (e.g., locations/123)
    """
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        account_id = request.args.get('account_id', type=int)
        location_name = request.args.get('location_name')
        
        if not account_id or not location_name:
            return jsonify({'success': False, 'error': 'account_id and location_name required'}), 400
        
        client = get_client()
        reviews = client.list_all_reviews(account_id, location_name)
        
        # Save reviews to database
        for review in reviews:
            save_review(location_id, review)
        
        return jsonify({
            'success': True,
            'reviews': reviews,
            'count': len(reviews)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/reviews/<review_name>/reply', methods=['POST'])
def reply_to_review(review_name):
    """
    Reply to a review.
    Body: { "account_id": 1, "comment": "Thank you..." }
    """
    try:
        if not session.get('user_id'):
            return jsonify({'success': False, 'error': 'Not logged in'}), 401
        
        data = request.get_json()
        account_id = data.get('account_id')
        comment = data.get('comment')
        
        if not account_id or not comment:
            return jsonify({'success': False, 'error': 'account_id and comment required'}), 400
        
        client = get_client()
        result = client.reply_to_review(account_id, review_name, comment)
        
        return jsonify({
            'success': True,
            'reply': result
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Grid Scanning ====================

@gmb_bp.route('/scan/preview', methods=['POST'])
def preview_scan():
    """
    Run a single point scan to specific lat/lng.
    This is the 'God Mode' capability test.
    Body: { "keyword": "pizza", "lat": 40.7128, "lng": -74.0060 }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    data = request.get_json()
    keyword = data.get('keyword')
    lat = data.get('lat')
    lng = data.get('lng')
    use_cache = data.get('use_cache', True)
    
    if not keyword or lat is None or lng is None:
        return jsonify({'success': False, 'error': 'Missing keyword, lat, or lng'})

    # Check cache first
    if use_cache:
        cached = get_cached_serp(keyword, float(lat), float(lng))
        if cached:
            return jsonify({
                'success': True,
                'results': cached,
                'cached': True
            })

    # Run scan synchronously for preview (small delay acceptable)
    try:
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        html = driver.scan_grid_point(keyword, float(lat), float(lng))
        
        if html:
            parser = GoogleMapsParser()
            results = parser.parse_list_results(html)
            
            # Cache results
            save_serp_cache(keyword, float(lat), float(lng), results, config.CACHE_TTL_SERP_RESULT)
            
            return jsonify({
                'success': True,
                'results': results,
                'cached': False
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Failed to fetch results'
            })
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/scan/grid', methods=['POST'])
def start_grid_scan():
    """
    Start a full grid scan (async).
    Body: {
        "keyword": "pizza",
        "center_lat": 40.7128,
        "center_lng": -74.0060,
        "radius_meters": 3000,
        "grid_size": 5,
        "target_business": "My Business Name"  # Optional - name to track rankings for
    }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401

    data = request.get_json()
    keyword = data.get('keyword')
    center_lat = data.get('center_lat')
    center_lng = data.get('center_lng')
    radius_meters = data.get('radius_meters', 3000)
    grid_size = data.get('grid_size', 5)
    grid_shape = data.get('grid_shape', 'square')
    location_id = data.get('location_id')
    target_business = data.get('target_business')
    target_place_id = data.get('target_place_id')  # Extract place_id for exact match (New)
    client_id = data.get('client_id')  # [NEW] Client Isolation
    
    if not keyword or center_lat is None or center_lng is None:
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400
    
    # Limit grid size
    grid_size = min(max(grid_size, 3), 9)  # 3x3 to 9x9
    
    log.debug(f"[GridScan] Received request - keyword='{keyword}', target='{target_business}', place_id='{target_place_id}', shape='{grid_shape}'")

    # Create scan record
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_grid_scans (
                location_id, keyword, target_business, target_place_id, center_lat, center_lng, 
                radius_meters, grid_size, total_points, status, client_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running', ?)
        ''', (
            location_id, keyword, target_business, target_place_id, center_lat, center_lng,
            radius_meters, grid_size, grid_size * grid_size, client_id
        ))
        scan_id = cursor.lastrowid

    # Run grid scan in background
    def run_grid_scan():
        log.debug(f"[GridScan] Background thread started for scan {scan_id}")
        try:
            from .crawler.grid_engine import GridEngine
            log.debug(f"[GridScan] Creating GridEngine...")
            engine = GridEngine()
            log.debug(f"[GridScan] Starting scan execution for keyword='{keyword}', target_business='{target_business}'...")
            engine.execute_scan(
                scan_id, 
                keyword, 
                center_lat, 
                center_lng, 
                radius_meters, 
                grid_size,
                target_place_id=target_place_id,  # Pass extracted place_id
                target_business_name=target_business,
                grid_shape=grid_shape
            )
            log.debug(f"[GridScan] Scan {scan_id} completed successfully!")
        except Exception as e:
            import traceback
            log.debug(f"[GridScan] Scan {scan_id} FAILED with error: {e}")
            traceback.print_exc()
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE gmb_grid_scans SET status = 'failed' WHERE id = ?",
                    (scan_id,)
                )

    log.debug(f"[GridScan] Starting background thread for scan {scan_id}...")
    thread = threading.Thread(target=run_grid_scan, daemon=True)
    thread.start()
    log.debug(f"[GridScan] Background thread launched")

    return jsonify({
        'success': True,
        'scan_id': scan_id,
        'message': 'Grid scan started in background'
    })


@gmb_bp.route('/scan/<int:scan_id>/status', methods=['GET'])
def get_scan_status(scan_id):
    """Get status of a grid scan."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM gmb_grid_scans WHERE id = ?
        ''', (scan_id,))
        scan = cursor.fetchone()
        
        if not scan:
            return jsonify({'success': False, 'error': 'Scan not found'}), 404
        
        return jsonify({
            'success': True,
            'scan': dict(scan)
        })


@gmb_bp.route('/scan/<int:scan_id>/results', methods=['GET'])
def get_scan_results(scan_id):
    """Get results of a grid scan."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            SELECT * FROM gmb_grid_results WHERE scan_id = ? ORDER BY point_index
        ''', (scan_id,))
        results = cursor.fetchall()
        
        results_list = []
        for r in results:
            item = dict(r)
            # Map fields to frontend GridPoint interface
            item['rank'] = item.pop('target_rank', None)
            
            # Parse top_results JSON
            if item.get('top_results'):
                try:
                    item['topResults'] = json.loads(item['top_results'])
                except:
                    item['topResults'] = []
            else:
                item['topResults'] = []
            
            # Remove raw json string
            item.pop('top_results', None)
            
            results_list.append(item)
        
        return jsonify({
            'success': True,
            'results': results_list
        })


@gmb_bp.route('/scan/<int:scan_id>', methods=['DELETE'])
def delete_scan(scan_id):
    """Delete a grid scan and its results."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        
        # Check if scan exists
        cursor.execute('SELECT id FROM gmb_grid_scans WHERE id = ?', (scan_id,))
        scan = cursor.fetchone()
        
        if not scan:
            return jsonify({'success': False, 'error': 'Scan not found'}), 404
        
        # Delete results first (foreign key constraint)
        cursor.execute('DELETE FROM gmb_grid_results WHERE scan_id = ?', (scan_id,))
        
        # Delete the scan
        cursor.execute('DELETE FROM gmb_grid_scans WHERE id = ?', (scan_id,))
        
        return jsonify({
            'success': True,
            'message': f'Scan {scan_id} deleted successfully'
        })


@gmb_bp.route('/scans', methods=['GET'])
def list_scans():
    """List all grid scans for the current user."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        
        # [NEW] Client Isolation Logic
        client_id = request.args.get('client_id')
        
        query = 'SELECT * FROM gmb_grid_scans'
        params = []
        
        if client_id:
            query += ' WHERE client_id = ?'
            params.append(client_id)
            
        query += ' ORDER BY started_at DESC LIMIT 50'
        
        cursor.execute(query, params)
        scans = cursor.fetchall()
        
        return jsonify({
            'success': True,
            'scans': [dict(s) for s in scans]
        })


# ==================== SERP Checker ====================

@gmb_bp.route('/serp/check', methods=['POST'])
def check_serp_ranking():
    """
    Check a website's ranking in Google Search results.
    
    Body: {
        "keyword": "best running shoes",
        "location": "United States",
        "domain": "example.com",  # Optional - domain to find ranking for
        "device": "desktop",       # desktop or mobile
        "depth": 10,               # 10, 20, 50, 100
        "language": "en"
    }
    
    Returns: {
        "success": true,
        "organic_results": [...],
        "local_pack": [...],
        "serp_features": {...},
        "target_rank": 5,
        "target_url": "https://example.com/page"
    }
    """
    # Auth is handled by the Next.js proxy layer
    try:
        data = request.get_json()
        keyword = data.get('keyword', '').strip()
        location = data.get('location', 'United States')
        domain = data.get('domain', '').strip()
        device = data.get('device', 'desktop')
        depth = data.get('depth', 10)
        language = data.get('language', 'en')
        
        # [NEW] Fast mode option - uses requests instead of Playwright (default: True)
        fast_mode = data.get('fast_mode', True)
        
        # [NEW] Optional lat/lng for accurate geo-context
        lat = data.get('lat')
        lng = data.get('lng')

        # [NEW] Client Isolation
        client_id = data.get('client_id')
        
        # [NEW] Parse complex queries like "Tusk Berry in Boston, Massachusetts"
        # This extracts the business name as keyword and location separately
        if keyword and (lat is None or lng is None):
            from .geoip import parse_query_location
            parsed = parse_query_location(keyword)
            
            if parsed.get('has_location_intent') and parsed.get('location'):
                # Found location in query - use extracted parts
                log.debug(f"[SerpCheck] Query parsed: keyword='{parsed['keyword']}', location='{parsed['location']}'")
                keyword = parsed['keyword']  # Use just the business/keyword part
                location = parsed['location']  # Override location from query
                
                # If geocoding was already done during parsing, use those coords
                if parsed.get('geocoded') and parsed['geocoded'].get('lat'):
                    lat = parsed['geocoded']['lat']
                    lng = parsed['geocoded']['lng']
                    log.debug(f"[SerpCheck] Using parsed coords: ({lat}, {lng})")
        
        # [NEW] IP-based location detection
        use_ip_location = data.get('use_ip_location', False)
        detected_location = None
        
        if use_ip_location:
            from .geoip import get_location_from_ip, get_client_ip_from_request
            client_ip = get_client_ip_from_request(request)
            log.debug(f"[SerpCheck] IP-based location requested. Client IP: {client_ip}")
            
            if client_ip:
                geo = get_location_from_ip(client_ip)
                if geo and geo.get('lat') and geo.get('lng'):
                    # Use detected location
                    detected_location = f"{geo['city']}, {geo['region']}" if geo['city'] else geo['country']
                    
                    # [FIX] For product/shopping searches, Google uses the 'gl' (country) parameter
                    # The geo_driver's scan_serp function infers gl_code from the location string
                    # So we need to include country name for proper gl inference
                    # Format: "City, Region, Country" - this helps with both geolocation AND gl inference
                    if geo.get('country'):
                        location = f"{geo['city']}, {geo['region']}, {geo['country']}" if geo['city'] else geo['country']
                    else:
                        location = detected_location
                        
                    lat = geo['lat']
                    lng = geo['lng']
                    log.debug(f"[SerpCheck] Auto-detected location: {location} ({lat}, {lng})")
                else:
                    log.debug(f"[SerpCheck] Could not resolve IP {client_ip}, using default location")
        
        # [NEW] Geocode the selected location if no coordinates provided
        # This ensures local searches like "dentist near me" work with the selected location
        if lat is None or lng is None:
            from .geoip import geocode_location
            log.debug(f"[SerpCheck] Geocoding selected location: '{location}'...")
            geo_result = geocode_location(location)
            if geo_result and geo_result.get('lat') and geo_result.get('lng'):
                lat = geo_result['lat']
                lng = geo_result['lng']
                log.debug(f"[SerpCheck] Geocoded '{location}' -> ({lat}, {lng})")
            else:
                log.debug(f"[SerpCheck] WARNING: Could not geocode '{location}' - local searches may not work correctly")
        
        if not keyword:
            return jsonify({
                'success': False,
                'error': 'Keyword is required'
            }), 400
        
        # Validate depth
        valid_depths = [10, 20, 50, 100]
        if depth not in valid_depths:
            depth = 10
        
        log.debug(f"[SerpCheck] Keyword='{keyword}', Location='{location}' (Lat/Lng: {lat}, {lng}), Domain='{domain}', FastMode={fast_mode}")
        
        
        from .crawler.geo_driver import GeoCrawlerDriver
        from .crawler.serp_parser import GoogleSerpParser
        
        # Initialize driver
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        
        # [NEW] Determine if query requires full browser mode
        # Local intent keywords that need geolocation spoofing
        local_intent_keywords = ['near me', 'nearby', 'local', 'closest', 'around me', 'in my area']
        has_local_intent = any(k in keyword.lower() for k in local_intent_keywords)
        has_explicit_coords = lat is not None and lng is not None
        needs_deep_crawl = depth > 10  # Multi-page crawling needs browser
        
        # Force browser mode if:
        # 1. User explicitly disabled fast mode
        # 2. Query has local intent (requires geolocation)
        # 3. Depth > 10 (requires pagination via browser)
        # 4. [FIX] Explicit coordinates provided (requires browser geolocation API)
        needs_browser = (
            fast_mode == False or
            has_local_intent or
            needs_deep_crawl or
            has_explicit_coords
        )
        
        html = None
        final_url = None
        used_fast_mode = False
        
        if not needs_browser:
            # Try fast mode first
            log.debug(f"[SerpCheck] Attempting fast mode (requests-based)...")
            html, final_url, success = driver.scan_serp_fast(
                keyword=keyword,
                location=location,
                device=device,
                depth=depth,
                language=language
            )
            
            if success and html:
                used_fast_mode = True
                log.debug(f"[SerpCheck] ✓ Fast mode succeeded!")
            else:
                log.debug(f"[SerpCheck] Fast mode failed, falling back to browser...")
                html = None  # Reset for browser attempt
        
        # Fallback to browser mode if needed
        if not html:
            log.debug(f"[SerpCheck] Using browser mode (Playwright)...")
            log.info(f"[SerpCheck] STARTING scan_serp for '{keyword}'...")
            t0 = time.time()
            html, final_url = driver.scan_serp(
                keyword=keyword,
                location=location,
                device=device,
                depth=depth,
                language=language,
                lat=float(lat) if lat is not None else None,
                lng=float(lng) if lng is not None else None
            )
            log.info(f"[SerpCheck] FINISHED scan_serp in {time.time()-t0:.2f}s. HTML len: {len(html) if html else 0}")
        
        if not html:
            return jsonify({
                'success': False,
                'error': 'Failed to fetch search results. Google might be blocking requests (CAPTCHA). Please Try again later or configure a Proxy.'
            }), 503
        
        # Parse results
        try:
            parser = GoogleSerpParser()
            results = parser.parse_serp_results(html, target_domain=domain if domain else None)
        except Exception as parse_err:
            log.error(f"[SerpCheck] Parser Error: {parse_err}")
            # Return partial results or error? 
            # For now re-raise to hit the main error handler but log it specifically
            raise parse_err
        
        # [NEW] Save search to history
        try:
            from .models import save_serp_search
            save_serp_search({
                'keyword': keyword,
                'location': location,
                'lat': lat,
                'lng': lng,
                'device': device,
                'language': language,
                'depth': depth,
                'organic_count': len(results['organic_results']),
                'local_pack_count': len(results['local_pack']),
                'hotel_count': len(results.get('hotel_results', [])),
                'shopping_count': len(results.get('shopping_results', [])),
                'target_rank': results['target_rank'],
                'target_url': results['target_url'],
                'results': results,
                'client_id': client_id  # [NEW] Client Isolation
            })
        except Exception as db_err:
            log.error(f"[SerpCheck] DB Save Error: {db_err}")
            # Don't fail the request just because save failed
            pass
        
        return jsonify({
            'success': True,
            'keyword': keyword,
            'location': location,
            'detected_location': detected_location,
            'device': device,
            'organic_results': results['organic_results'],
            'local_pack': results['local_pack'],
            'serp_features': results['serp_features'],
            'target_rank': results['target_rank'],
            'target_url': results['target_url'],
            'total_results': results['total_results'],
            'hotel_results': results.get('hotel_results', []),
            'shopping_results': results.get('shopping_results', []),
            'ai_overview': results.get('ai_overview')  # [NEW] AI Overview data
        })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        log.error(f"[SerpCheck] Critical Error: {error_details}")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': error_details
        }), 500


@gmb_bp.route('/serp/history', methods=['GET'])
def get_serp_search_history():
    """Get SERP search history."""
    from .models import get_serp_history
    
    limit = request.args.get('limit', 50, type=int)
    client_id = request.args.get('client_id')
    history = get_serp_history(limit=limit, client_id=client_id)
    
    return jsonify({
        'success': True,
        'history': history
    })


@gmb_bp.route('/serp/history/<int:search_id>', methods=['GET'])
def get_serp_search_detail(search_id):
    """Get a specific SERP search with full results."""
    from .models import get_serp_search_by_id
    
    client_id = request.args.get('client_id')
    search = get_serp_search_by_id(search_id, client_id=client_id)
    if not search:
        return jsonify({'success': False, 'error': 'Search not found'}), 404
    
    return jsonify({
        'success': True,
        'search': search
    })


@gmb_bp.route('/serp/history/<int:search_id>', methods=['DELETE'])
def delete_serp_search_endpoint(search_id):
    """Delete a SERP search from history."""
    from .models import delete_serp_search
    
    client_id = request.args.get('client_id')
    
    if delete_serp_search(search_id, client_id=client_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Search not found'}), 404


@gmb_bp.route('/serp/test-db', methods=['GET'])
def test_serp_db():
    """Debug endpoint to test DB connection."""
    try:
        from src.database import get_db
        results = []
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 as val')
            results.append(dict(cursor.fetchone()))
            
            client_id = request.args.get('client_id', '17')
            cursor.execute('SELECT count(*) as count FROM serp_searches WHERE client_id = ?', (client_id,))
            results.append(dict(cursor.fetchone()))
            
        return jsonify({'success': True, 'results': results})
    except Exception as e:
        import traceback
        return jsonify({
            'success': False, 
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500


# ==================== Sync Operations ====================

@gmb_bp.route('/sync', methods=['POST'])
def trigger_sync():
    """
    Trigger a sync job.
    Body: {
        "account_id": 1,
        "location_id": null,  # Optional - if provided, syncs single location
        "sync_type": "FULL_SYNC"  # FULL_SYNC, LOCATION_SYNC, REVIEWS_SYNC, HEALTH_CHECK
    }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .sync_manager import SyncManager
    
    data = request.get_json()
    account_id = data.get('account_id')
    location_id = data.get('location_id')
    sync_type = data.get('sync_type', 'FULL_SYNC')
    
    try:
        user_id = get_user_id()
        sync_mgr = SyncManager(user_id)
        
        if sync_type == 'FULL_SYNC':
            if not account_id:
                return jsonify({'success': False, 'error': 'account_id required for FULL_SYNC'}), 400
            job_id = sync_mgr.sync_all_locations(account_id)
        elif sync_type == 'LOCATION_SYNC':
            if not account_id or not location_id:
                return jsonify({'success': False, 'error': 'account_id and location_id required'}), 400
            job_id = sync_mgr.sync_location(location_id, account_id)
        elif sync_type == 'REVIEWS_SYNC':
            if not account_id or not location_id:
                return jsonify({'success': False, 'error': 'account_id and location_id required'}), 400
            job_id = sync_mgr.sync_reviews(location_id, account_id)
        elif sync_type == 'HEALTH_CHECK':
            if not location_id:
                return jsonify({'success': False, 'error': 'location_id required for HEALTH_CHECK'}), 400
            job_id = sync_mgr.calculate_health(location_id)
        else:
            return jsonify({'success': False, 'error': f'Unknown sync_type: {sync_type}'}), 400
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'message': f'{sync_type} job queued'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/sync/status/<job_id>', methods=['GET'])
def get_sync_job_status(job_id):
    """Get status of a sync job."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_sync_job
    
    job = get_sync_job(job_id)
    if not job:
        return jsonify({'success': False, 'error': 'Job not found'}), 404
    
    return jsonify({
        'success': True,
        'job': job
    })


@gmb_bp.route('/sync/history', methods=['GET'])
def get_sync_history():
    """Get sync job history for current user."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_sync_job_history
    
    user_id = get_user_id()
    limit = request.args.get('limit', 20, type=int)
    
    jobs = get_sync_job_history(user_id=user_id, limit=limit)
    
    return jsonify({
        'success': True,
        'jobs': jobs
    })


@gmb_bp.route('/sync/process', methods=['POST'])
def process_sync_jobs():
    """
    Process pending sync jobs (admin/cron endpoint).
    Can be called manually or via cron to process queue.
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_pending_sync_jobs
    from .sync_manager import SyncManager
    
    jobs = get_pending_sync_jobs(limit=5)
    processed = 0
    failed = 0
    
    for job in jobs:
        try:
            # Create manager for the job's user
            sync_mgr = SyncManager(job.get('user_id') or get_user_id())
            success = sync_mgr.process_job(job)
            if success:
                processed += 1
            else:
                failed += 1
        except Exception as e:
            log.error(f"Error processing job {job['job_id']}: {e}")
            failed += 1
    
    return jsonify({
        'success': True,
        'processed': processed,
        'failed': failed,
        'remaining': len(get_pending_sync_jobs(limit=100))
    })


# ==================== Location Health ====================

@gmb_bp.route('/locations/health', methods=['GET'])
def get_all_locations_health_endpoint():
    """Get all locations with their latest health scores."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_all_locations_with_health
    
    account_id = request.args.get('account_id', type=int)
    locations = get_all_locations_with_health(account_id=account_id)
    
    return jsonify({
        'success': True,
        'locations': locations
    })


@gmb_bp.route('/location/<int:location_id>/health', methods=['GET'])
def get_location_health(location_id):
    """Get health snapshot history for a location."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_health_history, get_latest_health_score
    from .health_calculator import HealthCalculator
    
    days = request.args.get('days', 30, type=int)
    
    latest = get_latest_health_score(location_id)
    history = get_health_history(location_id, days=days)
    
    # Get recommendations based on latest scores
    recommendations = []
    if latest:
        scores = {
            'profile': latest.get('profile_score', 0),
            'photos': latest.get('photos_score', 0),
            'reviews': latest.get('reviews_score', 0),
            'posts': latest.get('posts_score', 0),
            'qa': latest.get('qa_score', 0)
        }
        recommendations = HealthCalculator.get_improvement_recommendations(scores)
    
    return jsonify({
        'success': True,
        'latest': latest,
        'history': history,
        'recommendations': recommendations
    })


@gmb_bp.route('/location/<int:location_id>/health/calculate', methods=['POST'])
def calculate_location_health(location_id):
    """Manually trigger health score calculation for a location."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .sync_manager import SyncManager
    
    try:
        user_id = get_user_id()
        sync_mgr = SyncManager(user_id)
        job_id = sync_mgr.calculate_health(location_id)
        
        # Process immediately (synchronous for single location)
        from .models import get_sync_job
        job = get_sync_job(job_id)
        if job:
            sync_mgr.process_job(job)
        
        # Get updated score
        from .models import get_latest_health_score
        score = get_latest_health_score(location_id)
        
        return jsonify({
            'success': True,
            'job_id': job_id,
            'score': score
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ==================== Audit Logs ====================

@gmb_bp.route('/location/<int:location_id>/audit', methods=['GET'])
def get_location_audit_logs(location_id):
    """Get audit history for a location."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_audit_logs
    
    limit = request.args.get('limit', 50, type=int)
    logs = get_audit_logs(entity_type='LOCATION', entity_id=location_id, limit=limit)
    
    return jsonify({
        'success': True,
        'logs': logs
    })


@gmb_bp.route('/audit', methods=['GET'])
def get_all_audit_logs():
    """Get all audit logs (admin view)."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_audit_logs
    
    entity_type = request.args.get('entity_type')
    entity_id = request.args.get('entity_id', type=int)
    limit = request.args.get('limit', 100, type=int)
    
    logs = get_audit_logs(entity_type=entity_type, entity_id=entity_id, limit=limit)
    
    return jsonify({
        'success': True,
        'logs': logs
    })


# ==================== Quota Usage ====================

@gmb_bp.route('/quota/usage', methods=['GET'])
def get_quota_usage():
    """Get API quota usage statistics."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_quota_stats
    
    period = request.args.get('period', 'day')  # day, week, month
    
    if period not in ('day', 'week', 'month'):
        return jsonify({'success': False, 'error': 'Invalid period. Use: day, week, month'}), 400
    
    stats = get_quota_stats(period=period)
    
    # Add daily limit info from config
    from .config import config
    stats['daily_limit'] = int(config.API_RATE_LIMIT * 86400)  # requests per day at rate limit
    
    return jsonify({
        'success': True,
        'stats': stats
    })


# ==================== Crawl-Only Location Management ====================

@gmb_bp.route('/location/add', methods=['POST'])
def add_location_by_crawl():
    """
    Add a location by crawling Google Maps (no OAuth required).
    
    Body: {
        "url": "https://maps.google.com/...",  # or
        "name": "Business Name",
        "location": "City, State"  # optional
    }
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import save_crawled_location, log_audit_event, save_health_snapshot
    from .health_calculator import calculate_location_health
    
    data = request.get_json()
    url = data.get('url', '').strip()
    name = data.get('name', '').strip()
    location = data.get('location', '').strip()
    
    # NEW: Check if user already selected a business from search results
    selected_business = data.get('selectedBusiness')
    if selected_business or data.get('place_id'):
        # User selected from search results - use provided data directly
        log.debug(f"[AddLocation] Using pre-selected business: {data.get('name')}")
        
        from .models import save_crawled_location, log_audit_event, save_health_snapshot
        from .health_calculator import calculate_location_health
        
        business = {
            'place_id': data.get('place_id') or selected_business.get('place_id', ''),
            'name': data.get('name') or selected_business.get('name'),
            'address': data.get('address') or selected_business.get('address', ''),
            'lat': data.get('lat') or selected_business.get('lat'),
            'lng': data.get('lng') or selected_business.get('lng'),
            'category': data.get('category') or selected_business.get('category', ''),
            'phone': data.get('phone') or selected_business.get('phone'),
            'website': data.get('website') or selected_business.get('website'),
            'rating': data.get('rating') or selected_business.get('rating', 0),
            'review_count': data.get('review_count') or selected_business.get('review_count', 0),
            'photo_count': 0,  # Will be populated on refresh
            'source_url': ''
        }
        
        if not business.get('name'):
            return jsonify({'success': False, 'error': 'Business name is required'}), 400
        
        # Save to database
        location_id = save_crawled_location(business)
        
        # Calculate health score
        from .models import get_location_by_id
        location_data = get_location_by_id(location_id)
        if location_data:
            health_result = calculate_location_health(location_data)
            business['health_score'] = health_result['scores']['overall']
            save_health_snapshot(location_id, health_result['scores'])
        
        # Log audit event
        log_audit_event(
            entity_type='LOCATION',
            entity_id=location_id,
            action='ADDED_BY_SELECTION',
            after_state=business,
            user_id=session.get('user_id')
        )
        
        return jsonify({
            'success': True,
            'location_id': location_id,
            'business': business
        })
    
    if not url and not name:
        return jsonify({'success': False, 'error': 'Provide either URL or business name'}), 400
    
    try:
        # Initialize crawler
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        parser = GoogleMapsParser()
        
        if url:
            # Resolve URL to get business data
            html, final_url = driver.scan_place_details(url)
            if not html:
                return jsonify({'success': False, 'error': 'Could not load the business page'}), 400
            
            details = parser.parse_place_details(html)
            
            # SELF-HEALING ADD: If URL scan yielded empty data, fallback to search
            if not details.get('rating') or not details.get('review_count'):
                log.warning("[AddLocation] URL scan yielded incomplete data. Falling back to search.")
                query = details.get('name') or name
                
                if query:
                    search_html, search_url = driver.search_business(query)
                    if search_html:
                        search_res = parser.parse_business_search(search_html, query)
                        if search_res.get('matches'):
                            log.info(f"[AddLocation] Self-healing successful. Using search data for '{query}'")
                            match = search_res['matches'][0]
                            # Merge search data into details, preferring search results for metrics
                            details['rating'] = match.get('rating')
                            details['review_count'] = match.get('review_count')
                            details['photo_count'] = match.get('photo_count') # Ensure photos are captured
                            
                            # Update coords if needed
                            if match.get('lat'): 
                                final_url = search_url # Use the search URL for coords extraction if needed

            
            log.debug(f"[AddLocation] Parsed details: rating={details.get('rating')}, review_count={details.get('review_count')}, photo_count={details.get('photo_count')}")
            
            lat, lng = parser._extract_coordinates_from_url(final_url or url)
            place_id = parser._extract_place_id(final_url or url) or details.get('name', '').replace(' ', '_')
            
            business = {
                'place_id': place_id,
                'name': details.get('name'),
                'address': details.get('address'),
                'lat': lat,
                'lng': lng,
                'category': details.get('primary_category'),
                'phone': details.get('phone'),
                'website': details.get('website'),
                'rating': details.get('rating') or 0,
                'review_count': details.get('review_count') or 0,
                'photo_count': details.get('photo_count') or 0,
                'source_url': url
            }
        else:
            # Search by name
            query = f"{name} {location}" if location else name
            html, final_url = driver.search_business(query, location)
            if not html:
                return jsonify({'success': False, 'error': 'Could not find business'}), 404
            
            result = parser.parse_business_search(html, name)
            
            if not result.get('matches'):
                return jsonify({'success': False, 'error': 'No matching business found'}), 404
            
            # Use first match
            business = result['matches'][0]
            business['category'] = business.get('category', '')
        
        if not business.get('name'):
            return jsonify({'success': False, 'error': 'Could not extract business data'}), 400
        
        # Save to database
        location_id = save_crawled_location(business)
        
        # Calculate health score
        from .models import get_location_by_id
        location_data = get_location_by_id(location_id)
        if location_data:
            health_result = calculate_location_health(location_data)
            business['health_score'] = health_result['scores']['overall']
            
            # Save health snapshot
            save_health_snapshot(location_id, health_result['scores'])
        
        # Log audit event
        log_audit_event(
            entity_type='LOCATION',
            entity_id=location_id,
            action='ADDED_BY_CRAWL',
            after_state=business,
            actor_type='USER',
            actor_id=session.get('user_id'),
            source='CRAWL'
        )
        
        return jsonify({
            'success': True,
            'location_id': location_id,
            'business': business
        })
        
    except Exception as e:
        log.debug(f"[AddLocation] Error: {e}")
        log.error(f"Stack trace: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/location/<int:location_id>/refresh', methods=['POST'])
def refresh_location_by_crawl(location_id):
    """
    Refresh location data by re-crawling Google Maps.
    """
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_location_by_id, save_crawled_location, log_audit_event, save_health_snapshot, get_db
    from .health_calculator import calculate_location_health
    
    location = get_location_by_id(location_id)
    if not location:
        return jsonify({'success': False, 'error': 'Location not found'}), 404
    
    try:
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        parser = GoogleMapsParser()
        
        # Search using name and coordinates for precision
        name = location['location_name']
        lat = location.get('lat')
        lng = location.get('lng')
        
        # PRIORITIZE: Use original Source URL if available (Highest Priority)
        # This respects the User's explicit intent
        target_url = None
        place_id_valid = False
        
        current_place_id = location.get('google_location_id')
        
        if location.get('source_url'):
            target_url = location['source_url']
            log.info(f"[RefreshLocation] Using original Source URL: {target_url}")
            place_id_valid = True # Treat as valid to skip fallback search unless empty data
        elif current_place_id and current_place_id.startswith('ChIJ'):
            # Only use ChIJ format place IDs - hex format (0x...) doesn't work with direct URLs
            # Use the correct Google Maps URL format
            target_url = f"https://www.google.com/maps/search/?api=1&query=Google+Maps&query_place_id={current_place_id}"
            log.debug(f"[RefreshLocation] Using direct Place ID URL: {target_url}")
            place_id_valid = True
        elif current_place_id and ':' in current_place_id and not current_place_id.startswith('0x'):
            # CID format - use search instead as direct CID URLs are unreliable
            log.warning(f"[RefreshLocation] CID format place_id '{current_place_id}'. Falling back to search.")
        else:
             log.warning(f"[RefreshLocation] Invalid or missing Place ID '{current_place_id}'. Falling back to search.")
        
        match = None
        direct_url_data = None  # Store data from direct URL scan even if incomplete
        
        if target_url:
            # Use direct scan - this is the MOST RELIABLE source of truth
            html, final_url = driver.scan_place_details(target_url)
            if html:
                # Use place details parser
                direct_url_data = parser.parse_place_details(html)
                # Map fields safely
                direct_url_data['lat'], direct_url_data['lng'] = parser._extract_coordinates_from_url(final_url)
                direct_url_data['category'] = direct_url_data.get('primary_category')
                
                # Check if we have complete data
                if direct_url_data.get('rating') and direct_url_data.get('review_count'):
                    match = direct_url_data
                    log.info(f"[RefreshLocation] Direct URL scan successful: rating={match.get('rating')}, reviews={match.get('review_count')}")
                else:
                    # Direct scan has incomplete metrics - do a supplementary search
                    # Search results always show review count even in "limited view"
                    log.warning(f"[RefreshLocation] Direct scan yielded incomplete metrics (rating={direct_url_data.get('rating')}, reviews={direct_url_data.get('review_count')}). Doing supplementary search.")
                    
                    # Build search query
                    search_query = name
                    if location.get('address_lines'):
                        search_query += f" {location['address_lines']}"
                    
                    search_html, _ = driver.search_business(search_query, lat=lat, lng=lng)
                    if search_html:
                        search_result = parser.parse_business_search(search_html, name)
                        if search_result.get('matches'):
                            search_match = search_result['matches'][0]
                            # Merge: use direct_url_data as base, supplement missing metrics from search
                            log.info(f"[RefreshLocation] Supplementary search found: rating={search_match.get('rating')}, reviews={search_match.get('review_count')}, photos={search_match.get('photo_count')}")
                            
                            # Fill in missing metrics from search
                            if not direct_url_data.get('review_count') and search_match.get('review_count'):
                                direct_url_data['review_count'] = search_match['review_count']
                            if not direct_url_data.get('photo_count') and search_match.get('photo_count'):
                                direct_url_data['photo_count'] = search_match['photo_count']
                            if not direct_url_data.get('rating') and search_match.get('rating'):
                                direct_url_data['rating'] = search_match['rating']
                    
                    match = direct_url_data
            else:
                log.warning("[RefreshLocation] Direct scan failed (no HTML). Falling back to search with full address.")

        # Only fall back to search if direct URL scan COMPLETELY failed (no HTML at all)
        # This prevents fetching wrong business based on current physical location
        if not match and not direct_url_data:
            # Fallback to search - use full address for maximum specificity
            # Build search query with as much location info as possible
            search_query = name
            if location.get('address_lines'):
                search_query += f" {location['address_lines']}"
            elif location.get('locality'):
                search_query += f" {location['locality']}"
            if location.get('region'):
                search_query += f" {location['region']}"
            
            log.info(f"[RefreshLocation] Searching for business with full address: {search_query}")
            html, final_url = driver.search_business(search_query, lat=lat, lng=lng)
            if not html:
                # Even search failed - preserve existing data
                log.error("[RefreshLocation] Both direct URL and search failed. Preserving existing data.")
                return jsonify({'success': False, 'error': 'Could not fetch updated data. Existing data preserved.'}), 400
            
            result = parser.parse_business_search(html, name)
            if not result.get('matches'):
                log.error("[RefreshLocation] Search returned no matches. Preserving existing data to avoid overwriting with wrong business.")
                return jsonify({'success': False, 'error': 'Could not find matching business. Existing data preserved.'}), 404
            
            # Verify the match looks correct before using it
            search_match = result['matches'][0]
            
            # Safety check: Don't update if the found business name is completely different
            # This prevents overwriting "Nice Dental Clinic" with "ABC Dental Pune"
            found_name = (search_match.get('name') or '').lower()
            original_name = (name or '').lower()
            
            # Check if names have reasonable overlap (at least first word matches)
            original_words = set(original_name.split())
            found_words = set(found_name.split())
            common_words = original_words.intersection(found_words)
            
            if len(common_words) == 0 and original_name and found_name:
                log.warning(f"[RefreshLocation] Search found '{search_match.get('name')}' which doesn't match '{name}'. Preserving existing data.")
                return jsonify({'success': False, 'error': f"Found business '{search_match.get('name')}' doesn't match. Existing data preserved."}), 400
            
            match = search_match
            log.info(f"[RefreshLocation] Using search result: {match.get('name')}")

        # Log before state
        before_state = dict(location)
        
        # Safety check - if we have no match at all, preserve existing data
        if not match:
            log.error("[RefreshLocation] No data obtained from scan or search. Preserving existing data.")
            return jsonify({'success': False, 'error': 'Could not fetch data. Existing data preserved.'}), 400
        
        # Update with fresh data - IMPORTANT: Preserve original location_name
        # The name should never change on refresh, only metrics should update
        # Use crawled value if present, otherwise preserve existing value
        updated_data = {
            'place_id': location['google_location_id'],
            'name': location['location_name'],  # Always preserve original name
            'address': match.get('address') or location.get('address_lines'),
            'lat': match.get('lat') or lat,
            'lng': match.get('lng') or lng,
            'category': match.get('category') or location.get('primary_category'),
            'phone': match.get('phone') or location.get('phone_number'),
            'source_url': match.get('source_url') or location.get('source_url'),
            # IMPORTANT: Only update metrics if we got NEW values, otherwise keep existing
            'rating': match.get('rating') if match.get('rating') is not None else location.get('rating'),
            'review_count': match.get('review_count') if match.get('review_count') is not None else location.get('total_reviews'),
            'photo_count': match.get('photo_count') if match.get('photo_count') is not None else location.get('photo_count'),
            # Enhanced data from parser (use None-safe logic)
            'hours': match.get('hours') or location.get('business_hours'),
            'attributes': match.get('attributes') or [],
            'description': match.get('description') or location.get('description'),
            'service_area': match.get('service_area') or {},
            'qa_count': match.get('qa_count') if match.get('qa_count') is not None else (location.get('qa_count') or 0),
            'post_count': match.get('post_count') if match.get('post_count') is not None else (location.get('post_count') or 0),
            'last_post_date': match.get('last_post_date') or location.get('last_post_date'),
        }
        
        log.info(f"[RefreshLocation] Merged data: rating={updated_data['rating']}, reviews={updated_data['review_count']}, photos={updated_data['photo_count']}")
            
        save_crawled_location(updated_data)
        
        # Recalculate health
        fresh_location = get_location_by_id(location_id)
        health_result = calculate_location_health(fresh_location)
        
        # Save health snapshot
        save_health_snapshot(location_id, health_result['scores'])
        
        log_audit_event(
            entity_type='LOCATION',
            entity_id=location_id,
            action='REFRESHED_BY_CRAWL',
            before_state=before_state,
            after_state=updated_data,
            actor_type='USER',
            actor_id=session.get('user_id'),
            source='CRAWL'
        )
        
        return jsonify({
            'success': True,
            'location': fresh_location,
            'health_score': health_result['scores']['overall']
        })

            
    except Exception as e:
        log.debug(f"[RefreshLocation] Error: {e}")
        log.error(f"Stack trace: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@gmb_bp.route('/location/<int:location_id>', methods=['DELETE'])
def delete_location_endpoint(location_id):
    """Delete a location and all its data."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import delete_location, get_location_by_id, log_audit_event
    
    location = get_location_by_id(location_id)
    if not location:
        return jsonify({'success': False, 'error': 'Location not found'}), 404
    
    # Log before deleting
    log_audit_event(
        entity_type='LOCATION',
        entity_id=location_id,
        action='DELETED',
        before_state=location,
        actor_type='USER',
        actor_id=session.get('user_id'),
        source='API'
    )
    
    success = delete_location(location_id)
    
    return jsonify({
        'success': success,
        'message': f'Location {location_id} deleted' if success else 'Failed to delete'
    })


@gmb_bp.route('/location/<int:location_id>', methods=['GET'])
def get_location_detail(location_id):
    """Get details for a single location."""
    if not session.get('user_id'):
        return jsonify({'success': False, 'error': 'Unauthorized'}), 401
    
    from .models import get_location_by_id, get_latest_health_score, get_audit_logs
    from .health_calculator import HealthCalculator
    
    location = get_location_by_id(location_id)
    if not location:
        return jsonify({'success': False, 'error': 'Location not found'}), 404
    
    # Get health data
    health = get_latest_health_score(location_id)
    
    # Get recommendations
    recommendations = []
    if health:
        scores = {
            'profile': health.get('profile_score', 0),
            'photos': health.get('photos_score', 0),
            'reviews': health.get('reviews_score', 0),
            'posts': health.get('posts_score', 0),
            'qa': health.get('qa_score', 0)
        }
        recommendations = HealthCalculator.get_improvement_recommendations(scores)
    
    # Get recent audit logs
    logs = get_audit_logs(entity_type='LOCATION', entity_id=location_id, limit=10)
    
    return jsonify({
        'success': True,
        'location': location,
        'health': health,
        'recommendations': recommendations,
        'audit_logs': logs
    })


