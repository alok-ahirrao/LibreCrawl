"""
Competitor Routes
REST API endpoints for competitor analysis.
"""
from flask import Blueprint, request, jsonify
import json
import threading

from .sniffer import CompetitorSniffer
from .analyzer import CompetitorAnalyzer
from ..gmb_core.models import get_db

# Create Flask Blueprint
competitor_bp = Blueprint('competitor', __name__, url_prefix='/api/competitor')

# Store for ongoing analyses
_active_analyses = {}


@competitor_bp.route('/status', methods=['GET'])
def status():
    """Health check for competitor module."""
    return jsonify({
        'status': 'ok',
        'module': 'competitor',
        'version': '1.0.0'
    })


@competitor_bp.route('/find', methods=['POST'])
def find_competitors():
    """
    Extract competitors from Google Maps for a keyword search.
    
    Body: {
        "keyword": "pizza",
        "lat": 40.7128,
        "lng": -74.0060,
        "max_results": 10
    }
    
    Returns: {
        "competitors": [...],
        "count": 10
    }
    """
    data = request.get_json() or {}
    
    keyword = data.get('keyword')
    lat = data.get('lat')
    lng = data.get('lng')
    max_results = data.get('max_results', 10)
    
    if not keyword or lat is None or lng is None:
        return jsonify({
            'error': 'Missing required fields: keyword, lat, lng'
        }), 400
    
    try:
        sniffer = CompetitorSniffer(headless=True)
        competitors = sniffer.extract_local_pack_competitors(
            keyword=keyword,
            lat=float(lat),
            lng=float(lng),
            max_results=max_results
        )
        sniffer.close()
        
        return jsonify({
            'competitors': competitors,
            'count': len(competitors),
            'keyword': keyword,
            'location': {'lat': lat, 'lng': lng}
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@competitor_bp.route('/profile/<place_id>', methods=['GET'])
def get_competitor_profile(place_id: str):
    """
    Get cached competitor profile or scrape fresh if needed.
    
    Query params:
        force_refresh: boolean - Force fresh scrape even if cached
    
    Returns: Competitor profile dict
    """
    force_refresh = request.args.get('force_refresh', 'false').lower() == 'true'
    
    try:
        # Check cache first
        if not force_refresh:
            cached = _get_cached_competitor(place_id)
            if cached:
                return jsonify({
                    'profile': cached,
                    'cached': True
                })
        
        # Scrape fresh
        sniffer = CompetitorSniffer(headless=True)
        profile = sniffer.scrape_competitor_profile(place_id)
        sniffer.close()
        
        if profile:
            return jsonify({
                'profile': profile,
                'cached': False
            })
        else:
            return jsonify({'error': 'Failed to scrape profile'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@competitor_bp.route('/analyze', methods=['POST'])
def analyze_competitors():
    """
    Run full deficit analysis between user profile and competitors.
    
    Body: {
        "user_profile": {
            "name": "My Business",
            "rating": 4.2,
            "review_count": 47,
            "photo_count": 12,
            "primary_category": "Pizza Restaurant",
            "additional_categories": [],
            "post_count": 0,
            "q_and_a_count": 0
        },
        "competitors": [
            {"place_id": "...", "name": "...", "rating": 4.5, "review_count": 100, ...},
            ...
        ]
        -- OR (legacy, will attempt scraping) --
        "competitor_place_ids": ["ChIJ...", "ChIJ...", "ChIJ..."]
    }
    
    Returns: {
        "deficits": [...],
        "comparison_matrix": {...},
        "competitors": [...]
    }
    """
    data = request.get_json() or {}
    
    user_profile = data.get('user_profile', {})
    
    if not user_profile:
        return jsonify({'error': 'Missing user_profile'}), 400
    
    # Check if full competitor data was passed directly (preferred)
    competitors = data.get('competitors', [])
    
    # Fallback: try place_ids for backward compatibility
    if not competitors:
        competitor_place_ids = data.get('competitor_place_ids', [])
        
        if not competitor_place_ids:
            return jsonify({'error': 'Missing competitors or competitor_place_ids'}), 400
        
        try:
            # Fetch competitor profiles via scraping (less reliable)
            sniffer = CompetitorSniffer(headless=True)
            
            for place_id in competitor_place_ids[:5]:  # Limit to 5 competitors
                # Check cache first
                cached = _get_cached_competitor(place_id)
                if cached:
                    competitors.append(cached)
                else:
                    profile = sniffer.scrape_competitor_profile(place_id)
                    if profile:
                        competitors.append(profile)
            
            sniffer.close()
        except Exception as e:
            print(f"[Competitor Routes] Scraping error: {e}")
    
    if not competitors:
        return jsonify({'error': 'No competitor data available'}), 404
    
    try:
        # Normalize competitor data fields
        normalized_competitors = []
        for c in competitors:
            normalized = {
                'place_id': c.get('place_id', ''),
                'name': c.get('name', 'Unknown'),
                'rating': c.get('rating') or 0,
                'review_count': c.get('review_count') or c.get('reviews') or 0,
                'photo_count': c.get('photo_count') or 0,
                'primary_category': c.get('primary_category') or c.get('category') or '',
                'additional_categories': c.get('additional_categories', []),
                'post_count': c.get('post_count') or 0,
                'q_and_a_count': c.get('q_and_a_count') or 0,
                'attributes': c.get('attributes', []),
                'rank': c.get('rank', 0)
            }
            normalized_competitors.append(normalized)
        
        # Run analysis
        analyzer = CompetitorAnalyzer()
        deficits = analyzer.calculate_deficits(user_profile, normalized_competitors)
        comparison_matrix = analyzer.generate_comparison_matrix(user_profile, normalized_competitors)
        
        # Save analysis
        analysis_id = _save_analysis(user_profile, normalized_competitors, deficits)
        
        return jsonify({
            'analysis_id': analysis_id,
            'deficits': deficits,
            'comparison_matrix': comparison_matrix,
            'competitors': normalized_competitors
        })
        
    except Exception as e:
        import traceback
        print(f"[Competitor Routes] Analysis error: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@competitor_bp.route('/analyze/async', methods=['POST'])
def analyze_competitors_async():
    """
    Start async competitor analysis (for large batches).
    
    Body: Same as /analyze
    
    Returns: {
        "analysis_id": "abc123",
        "status": "pending"
    }
    """
    data = request.get_json() or {}
    
    user_profile = data.get('user_profile', {})
    competitor_place_ids = data.get('competitor_place_ids', [])
    
    if not user_profile or not competitor_place_ids:
        return jsonify({'error': 'Missing required fields'}), 400
    
    # Generate analysis ID
    import uuid
    analysis_id = str(uuid.uuid4())[:8]
    
    # Store initial state
    _active_analyses[analysis_id] = {
        'status': 'pending',
        'progress': 0,
        'total': len(competitor_place_ids),
        'result': None
    }
    
    # Start background thread
    def run_analysis():
        try:
            sniffer = CompetitorSniffer(headless=True)
            competitors = []
            
            for i, place_id in enumerate(competitor_place_ids[:5]):
                _active_analyses[analysis_id]['progress'] = i + 1
                _active_analyses[analysis_id]['status'] = 'scraping'
                
                cached = _get_cached_competitor(place_id)
                if cached:
                    competitors.append(cached)
                else:
                    profile = sniffer.scrape_competitor_profile(place_id)
                    if profile:
                        competitors.append(profile)
            
            sniffer.close()
            
            # Run analysis
            _active_analyses[analysis_id]['status'] = 'analyzing'
            
            analyzer = CompetitorAnalyzer()
            deficits = analyzer.calculate_deficits(user_profile, competitors)
            comparison_matrix = analyzer.generate_comparison_matrix(user_profile, competitors)
            
            _save_analysis(user_profile, competitors, deficits)
            
            _active_analyses[analysis_id]['status'] = 'completed'
            _active_analyses[analysis_id]['result'] = {
                'deficits': deficits,
                'comparison_matrix': comparison_matrix,
                'competitors': competitors
            }
            
        except Exception as e:
            _active_analyses[analysis_id]['status'] = 'failed'
            _active_analyses[analysis_id]['error'] = str(e)
    
    thread = threading.Thread(target=run_analysis, daemon=True)
    thread.start()
    
    return jsonify({
        'analysis_id': analysis_id,
        'status': 'pending'
    })


@competitor_bp.route('/analyze/<analysis_id>/status', methods=['GET'])
def get_analysis_status(analysis_id: str):
    """Get status of an async analysis."""
    if analysis_id not in _active_analyses:
        return jsonify({'error': 'Analysis not found'}), 404
    
    analysis = _active_analyses[analysis_id]
    
    response = {
        'analysis_id': analysis_id,
        'status': analysis['status'],
        'progress': analysis['progress'],
        'total': analysis['total']
    }
    
    if analysis['status'] == 'completed':
        response['result'] = analysis['result']
    elif analysis['status'] == 'failed':
        response['error'] = analysis.get('error')
    
    return jsonify(response)


@competitor_bp.route('/analysis/<int:analysis_id>', methods=['GET'])
def get_saved_analysis(analysis_id: int):
    """Get a saved analysis by ID."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM competitive_analyses WHERE id = ?
            ''', (analysis_id,))
            
            row = cursor.fetchone()
            if not row:
                return jsonify({'error': 'Analysis not found'}), 404
            
            return jsonify({
                'id': row['id'],
                'user_place_id': row['user_place_id'],
                'keyword': row['keyword'],
                'competitor_ids': json.loads(row['competitor_ids']) if row['competitor_ids'] else [],
                'deficits': json.loads(row['deficits']) if row['deficits'] else [],
                'created_at': row['created_at']
            })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@competitor_bp.route('/analyses', methods=['GET'])
def list_analyses():
    """List all saved analyses."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, user_place_id, keyword, created_at 
                FROM competitive_analyses 
                ORDER BY created_at DESC 
                LIMIT 50
            ''')
            
            rows = cursor.fetchall()
            analyses = [dict(row) for row in rows]
            
            return jsonify({
                'analyses': analyses,
                'count': len(analyses)
            })
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ==================== Helper Functions ====================

def _get_cached_competitor(place_id: str) -> dict:
    """Get cached competitor profile."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM gmb_competitors 
                WHERE place_id = ? 
                AND last_scraped_at > datetime('now', '-7 days')
            ''', (place_id,))
            
            row = cursor.fetchone()
            if row:
                profile = dict(row)
                # Parse JSON fields
                for field in ['additional_categories', 'attributes', 'services']:
                    if profile.get(field):
                        try:
                            profile[field] = json.loads(profile[field])
                        except:
                            pass
                return profile
    except Exception as e:
        print(f"[Competitor Routes] Cache lookup error: {e}")
    
    return None


def _save_analysis(user_profile: dict, competitors: list, deficits: list) -> int:
    """Save analysis to database."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO competitive_analyses (
                    user_place_id, keyword, competitor_ids, deficits
                ) VALUES (?, ?, ?, ?)
            ''', (
                user_profile.get('place_id', ''),
                '',  # Keyword not always available
                json.dumps([c.get('place_id', '') for c in competitors]),
                json.dumps(deficits)
            ))
            
            return cursor.lastrowid
            
    except Exception as e:
        print(f"[Competitor Routes] Error saving analysis: {e}")
        return 0
