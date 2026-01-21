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
import threading
import json

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
    
    print(f"[BusinessSearch] Searching for: '{query}' in '{location or 'default location'}' (Lat/Lng: {lat}, {lng})")
    
    try:
        # Initialize crawler
        driver = GeoCrawlerDriver(
            headless=config.CRAWLER_HEADLESS,
            proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
        )
        
        # Perform search - RETURNS TUPLE (html, final_url)
        # Pass lat/lng if available to force browser location
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
                    print(f"[Router] Backfilled coordinates from Final URL: {url_lat}, {url_lng}")
        
        print(f"[BusinessSearch] Found {len(result['matches'])} matches with confidence {result['confidence']:.2f}")
        
        return jsonify({
            'success': True,
            'confidence': result['confidence'],
            'matches': result['matches']
        })
        
    except Exception as e:
        print(f"[BusinessSearch] Error: {e}")
        import traceback
        traceback.print_exc()
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
    
    print(f"[BusinessResolveURL] Resolving URL: {url}")
    
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
            print(f"[BusinessResolveURL] Extracted coords from final URL: {lat}, {lng}")
        
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
        
        print(f"[BusinessResolveURL] Resolved: {business['name']} at ({lat}, {lng})")
        
        return jsonify({
            'success': True,
            'business': business
        })
        
    except Exception as e:
        print(f"[BusinessResolveURL] Error: {e}")
        import traceback
        traceback.print_exc()
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
    
    if not keyword or center_lat is None or center_lng is None:
        return jsonify({'success': False, 'error': 'Missing required parameters'}), 400
    
    # Limit grid size
    grid_size = min(max(grid_size, 3), 9)  # 3x3 to 9x9
    
    print(f"[GridScan] Received request - keyword='{keyword}', target='{target_business}', place_id='{target_place_id}', shape='{grid_shape}'")

    # Create scan record
    from .models import get_db
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO gmb_grid_scans (
                location_id, keyword, target_business, target_place_id, center_lat, center_lng, 
                radius_meters, grid_size, total_points, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'running')
        ''', (
            location_id, keyword, target_business, target_place_id, center_lat, center_lng,
            radius_meters, grid_size, grid_size * grid_size
        ))
        scan_id = cursor.lastrowid

    # Run grid scan in background
    def run_grid_scan():
        print(f"[GridScan] Background thread started for scan {scan_id}")
        try:
            from .crawler.grid_engine import GridEngine
            print(f"[GridScan] Creating GridEngine...")
            engine = GridEngine()
            print(f"[GridScan] Starting scan execution for keyword='{keyword}', target_business='{target_business}'...")
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
            print(f"[GridScan] Scan {scan_id} completed successfully!")
        except Exception as e:
            import traceback
            print(f"[GridScan] Scan {scan_id} FAILED with error: {e}")
            traceback.print_exc()
            with get_db() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE gmb_grid_scans SET status = 'failed' WHERE id = ?",
                    (scan_id,)
                )

    print(f"[GridScan] Starting background thread for scan {scan_id}...")
    thread = threading.Thread(target=run_grid_scan, daemon=True)
    thread.start()
    print(f"[GridScan] Background thread launched")

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
        cursor.execute('''
            SELECT * FROM gmb_grid_scans 
            ORDER BY started_at DESC 
            LIMIT 50
        ''')
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
    
    # [NEW] Parse complex queries like "Tusk Berry in Boston, Massachusetts"
    # This extracts the business name as keyword and location separately
    if keyword and (lat is None or lng is None):
        from .geoip import parse_query_location
        parsed = parse_query_location(keyword)
        
        if parsed.get('has_location_intent') and parsed.get('location'):
            # Found location in query - use extracted parts
            print(f"[SerpCheck] Query parsed: keyword='{parsed['keyword']}', location='{parsed['location']}'")
            keyword = parsed['keyword']  # Use just the business/keyword part
            location = parsed['location']  # Override location from query
            
            # If geocoding was already done during parsing, use those coords
            if parsed.get('geocoded') and parsed['geocoded'].get('lat'):
                lat = parsed['geocoded']['lat']
                lng = parsed['geocoded']['lng']
                print(f"[SerpCheck] Using parsed coords: ({lat}, {lng})")
    
    # [NEW] IP-based location detection
    use_ip_location = data.get('use_ip_location', False)
    detected_location = None
    
    if use_ip_location:
        from .geoip import get_location_from_ip, get_client_ip_from_request
        client_ip = get_client_ip_from_request(request)
        print(f"[SerpCheck] IP-based location requested. Client IP: {client_ip}")
        
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
                print(f"[SerpCheck] Auto-detected location: {location} ({lat}, {lng})")
            else:
                print(f"[SerpCheck] Could not resolve IP {client_ip}, using default location")
    
    # [NEW] Geocode the selected location if no coordinates provided
    # This ensures local searches like "dentist near me" work with the selected location
    if lat is None or lng is None:
        from .geoip import geocode_location
        print(f"[SerpCheck] Geocoding selected location: '{location}'...")
        geo_result = geocode_location(location)
        if geo_result and geo_result.get('lat') and geo_result.get('lng'):
            lat = geo_result['lat']
            lng = geo_result['lng']
            print(f"[SerpCheck] Geocoded '{location}' -> ({lat}, {lng})")
        else:
            print(f"[SerpCheck] WARNING: Could not geocode '{location}' - local searches may not work correctly")
    
    if not keyword:
        return jsonify({
            'success': False,
            'error': 'Keyword is required'
        }), 400
    
    # Validate depth
    valid_depths = [10, 20, 50, 100]
    if depth not in valid_depths:
        depth = 10
    
    print(f"[SerpCheck] Keyword='{keyword}', Location='{location}' (Lat/Lng: {lat}, {lng}), Domain='{domain}', FastMode={fast_mode}")
    
    try:
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
            print(f"[SerpCheck] Attempting fast mode (requests-based)...")
            html, final_url, success = driver.scan_serp_fast(
                keyword=keyword,
                location=location,
                device=device,
                depth=depth,
                language=language
            )
            
            if success and html:
                used_fast_mode = True
                print(f"[SerpCheck] ✓ Fast mode succeeded!")
            else:
                print(f"[SerpCheck] Fast mode failed, falling back to browser...")
                html = None  # Reset for browser attempt
        
        # Fallback to browser mode if needed
        if not html:
            print(f"[SerpCheck] Using browser mode (Playwright)...")
            html, final_url = driver.scan_serp(
                keyword=keyword,
                location=location,
                device=device,
                depth=depth,
                language=language,
                lat=float(lat) if lat is not None else None,
                lng=float(lng) if lng is not None else None
            )
        
        if not html:
            return jsonify({
                'success': False,
                'error': 'Failed to fetch search results. Please try again.'
            }), 500
        
        # Parse results
        parser = GoogleSerpParser()
        results = parser.parse_serp_results(html, target_domain=domain if domain else None)
        
        # [NEW] Save search to history
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
            'results': results
        })
        
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
        print(f"[SerpCheck] Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@gmb_bp.route('/serp/history', methods=['GET'])
def get_serp_search_history():
    """Get SERP search history."""
    from .models import get_serp_history
    
    limit = request.args.get('limit', 50, type=int)
    history = get_serp_history(limit=limit)
    
    return jsonify({
        'success': True,
        'history': history
    })


@gmb_bp.route('/serp/history/<int:search_id>', methods=['GET'])
def get_serp_search_detail(search_id):
    """Get a specific SERP search with full results."""
    from .models import get_serp_search_by_id
    
    search = get_serp_search_by_id(search_id)
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
    
    if delete_serp_search(search_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'Search not found'}), 404
