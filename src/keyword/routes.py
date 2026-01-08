"""
Keyword Research API Routes
Provides endpoints for keyword density analysis and competitor keyword research.
"""

from flask import Blueprint, request, jsonify
import asyncio
import logging
import threading

from .ai_service import GeminiKeywordAI
from .keyword_analyzer import KeywordDensityAnalyzer
from .competitor_keywords import CompetitorKeywordResearcher
from .keyword_data import KeywordDataService

logger = logging.getLogger(__name__)

keyword_bp = Blueprint('keyword', __name__, url_prefix='/api/keyword')

# Thread-local storage for event loops
_local = threading.local()


def get_event_loop():
    """Get or create an event loop for the current thread."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Event loop is closed")
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def run_async(coro):
    """Helper to run async coroutines in Flask - creates fresh loop each time."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        # Clean up pending tasks
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except:
            pass
        loop.close()


# Shared AI service instance (stateless, can be shared)
_ai_service = None


def get_ai_service():
    """Get or create AI service instance."""
    global _ai_service
    if _ai_service is None:
        _ai_service = GeminiKeywordAI()
    return _ai_service


def get_analyzer():
    """Create a fresh keyword analyzer instance for each request."""
    return KeywordDensityAnalyzer(get_ai_service())


def get_researcher():
    """Create a fresh competitor researcher instance for each request."""
    return CompetitorKeywordResearcher(get_ai_service())


@keyword_bp.route('/status', methods=['GET'])
def keyword_status():
    """Check if keyword tools are available and AI is configured."""
    ai_service = get_ai_service()
    return jsonify({
        'success': True,
        'ai_available': ai_service.is_available(),
        'features': {
            'keyword_density': True,
            'competitor_research': True,
            'ai_expansion': ai_service.is_available(),
            'ai_intent_classification': ai_service.is_available(),
            'ai_recommendations': ai_service.is_available()
        }
    })


@keyword_bp.route('/analyze', methods=['POST'])
def analyze_keyword_density():
    """
    Analyze keyword density for a URL.
    
    Request body:
    {
        "url": "https://example.com/page",
        "use_ai": true,  // Optional, default true
        "top_n": 50      // Optional, default 50
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        url = data.get('url')
        if not url:
            return jsonify({'success': False, 'error': 'URL is required'}), 400
        
        use_ai = data.get('use_ai', True)
        top_n = data.get('top_n', 50)
        
        analyzer = get_analyzer()
        
        # Run async analysis
        result = run_async(analyzer.analyze_page(url, use_ai=use_ai, top_n=top_n))
        
        if 'error' in result and result.get('error'):
            return jsonify({
                'success': False,
                'error': result['error']
            }), 400
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Keyword density analysis failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/compare-pages', methods=['POST'])
def compare_pages():
    """
    Compare keyword density across multiple pages.
    
    Request body:
    {
        "urls": ["https://example.com/page1", "https://example.com/page2"],
        "use_ai": true
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        urls = data.get('urls')
        if not urls or not isinstance(urls, list) or len(urls) < 2:
            return jsonify({
                'success': False, 
                'error': 'At least 2 URLs required for comparison'
            }), 400
        
        use_ai = data.get('use_ai', True)
        
        analyzer = get_analyzer()
        result = run_async(analyzer.compare_pages(urls, use_ai=use_ai))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Page comparison failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/competitor-research', methods=['POST'])
def competitor_research():
    """
    Research competitor keywords and find gaps.
    
    Request body:
    {
        "your_url": "https://yourdomain.com",
        "competitor_url": "https://competitor.com",
        "use_ai": true
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        your_url = data.get('your_url')
        competitor_url = data.get('competitor_url')
        
        if not your_url:
            return jsonify({'success': False, 'error': 'your_url is required'}), 400
        if not competitor_url:
            return jsonify({'success': False, 'error': 'competitor_url is required'}), 400
        
        use_ai = data.get('use_ai', True)
        
        researcher = get_researcher()
        result = run_async(researcher.research_competitor(
            your_url=your_url,
            competitor_url=competitor_url,
            use_ai=use_ai
        ))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Competitor research failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/competitor-research-multi', methods=['POST'])
def competitor_research_multi():
    """
    Research multiple competitors at once.
    
    Request body:
    {
        "your_url": "https://yourdomain.com",
        "competitor_urls": ["https://comp1.com", "https://comp2.com"],
        "use_ai": true
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        your_url = data.get('your_url')
        competitor_urls = data.get('competitor_urls')
        
        if not your_url:
            return jsonify({'success': False, 'error': 'your_url is required'}), 400
        if not competitor_urls or not isinstance(competitor_urls, list):
            return jsonify({
                'success': False, 
                'error': 'competitor_urls must be a list of URLs'
            }), 400
        
        use_ai = data.get('use_ai', True)
        
        researcher = get_researcher()
        result = run_async(researcher.research_multiple_competitors(
            your_url=your_url,
            competitor_urls=competitor_urls,
            use_ai=use_ai
        ))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Multi-competitor research failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/expand', methods=['POST'])
def expand_keywords():
    """
    Expand seed keywords with AI suggestions.
    
    Request body:
    {
        "keywords": ["dental implants", "teeth whitening"],
        "count": 20
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keywords = data.get('keywords')
        if not keywords or not isinstance(keywords, list):
            return jsonify({
                'success': False, 
                'error': 'keywords must be a list'
            }), 400
        
        count = data.get('count', 20)
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        result = run_async(ai_service.expand_keywords(keywords, count=count))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Keyword expansion failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/classify-intent', methods=['POST'])
def classify_keyword_intent():
    """
    Classify keywords by search intent.
    
    Request body:
    {
        "keywords": ["how to whiten teeth", "buy teeth whitening kit", "colgate"]
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keywords = data.get('keywords')
        if not keywords or not isinstance(keywords, list):
            return jsonify({
                'success': False, 
                'error': 'keywords must be a list'
            }), 400
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        result = run_async(ai_service.classify_intent(keywords))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Intent classification failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/group', methods=['POST'])
def group_keywords():
    """
    Group keywords into topic clusters.
    
    Request body:
    {
        "keywords": ["dental implants", "implant cost", "teeth whitening", "white teeth"]
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keywords = data.get('keywords')
        if not keywords or not isinstance(keywords, list):
            return jsonify({
                'success': False, 
                'error': 'keywords must be a list'
            }), 400
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        result = run_async(ai_service.group_keywords(keywords))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Keyword grouping failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/difficulty', methods=['POST'])
def estimate_difficulty():
    """
    Estimate keyword difficulty.
    
    Request body:
    {
        "keyword": "dental implants near me",
        "serp_data": {
            "top_results": ["url1", "url2"],
            "has_featured_snippet": true,
            "has_local_pack": true,
            "ad_count": 4
        }
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keyword = data.get('keyword')
        if not keyword:
            return jsonify({'success': False, 'error': 'keyword is required'}), 400
        
        serp_data = data.get('serp_data', {})
        
        researcher = get_researcher()
        result = run_async(researcher.estimate_keyword_difficulty(keyword, serp_data))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Difficulty estimation failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/recommendations', methods=['POST'])
def get_recommendations():
    """
    Generate SEO recommendations based on analysis data.
    
    Request body:
    {
        "analysis_data": {
            "gaps": 15,
            "opportunities": ["keyword1", "keyword2"],
            "your_top": ["keyword3"]
        },
        "business_type": "dental clinic"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        analysis_data = data.get('analysis_data')
        if not analysis_data:
            return jsonify({
                'success': False, 
                'error': 'analysis_data is required'
            }), 400
        
        business_type = data.get('business_type')
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        result = run_async(ai_service.generate_recommendations(
            analysis_data=analysis_data,
            business_type=business_type
        ))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Recommendation generation failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


# =============================================================================
# NEW ENDPOINTS FOR HIGH-TRAFFIC KEYWORD DISCOVERY
# =============================================================================

# Shared keyword data service instance
_keyword_data_service = None


def get_keyword_data_service(geo: str = ''):
    """Get or create keyword data service instance."""
    global _keyword_data_service
    if _keyword_data_service is None:
        _keyword_data_service = KeywordDataService(geo=geo)
    return _keyword_data_service


@keyword_bp.route('/discover', methods=['POST'])
def discover_keywords():
    """
    Comprehensive keyword discovery for high-traffic keywords.
    Combines Google Trends, Autocomplete, and AI for best results.
    
    Request body:
    {
        "seed_keywords": ["dental implants", "teeth whitening"],
        "geo": "US",           // Optional, geographic region
        "niche": "dental",     // Optional, business niche
        "include_trends": true, // Optional, default true
        "include_questions": true // Optional, default true
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        seed_keywords = data.get('seed_keywords', data.get('keywords', []))
        if not seed_keywords or not isinstance(seed_keywords, list):
            return jsonify({
                'success': False, 
                'error': 'seed_keywords must be a non-empty list'
            }), 400
        
        geo = data.get('geo', data.get('region', ''))
        niche = data.get('niche', '')
        location = data.get('location', '')  # New: specific location for local SEO
        include_trends = data.get('include_trends', True)
        include_questions = data.get('include_questions', True)
        include_niche_patterns = data.get('include_niche_patterns', True)
        include_competitor_templates = data.get('include_competitor_templates', True)
        
        # Use ENHANCED keyword discovery with all sources
        data_service = get_keyword_data_service(geo)
        discovery_result = data_service.discover_keywords_enhanced(
            seed_keywords=seed_keywords,
            geo=geo,
            niche=niche,
            location=location,
            include_trends=include_trends,
            include_autocomplete=True,
            include_questions=include_questions,
            include_niche_patterns=include_niche_patterns and bool(niche),
            include_competitor_templates=include_competitor_templates
        )
        
        # Generate long-tail keywords for the first seed keyword
        long_tail_keywords = []
        if seed_keywords:
            try:
                # Use patterns only for speed (skip alphabet soup to avoid rate limits on first load)
                long_tail_keywords = data_service.generate_long_tail_keywords(
                    seed_keyword=seed_keywords[0],
                    include_alphabet_soup=False,  # Skip for speed
                    include_patterns=True,
                    include_modifiers=True
                )
            except Exception as e:
                logger.warning(f"Long-tail generation failed: {e}")
        
        # Get People Also Ask questions
        paa_questions = []
        if seed_keywords:
            try:
                paa_questions = data_service.get_people_also_ask(seed_keywords[0])
            except Exception as e:
                logger.warning(f"PAA fetch failed: {e}")
        
        # Merge PAA questions with existing questions
        all_questions = discovery_result.get('questions', []) + paa_questions
        
        # Combine all discovered keywords
        all_discovered = discovery_result.get('discovered_keywords', []) + long_tail_keywords
        
        # Enhance with AI suggestions if available
        ai_service = get_ai_service()
        ai_suggestions = {}
        
        if ai_service.is_available():
            try:
                ai_suggestions = run_async(ai_service.suggest_high_traffic_keywords(
                    seed_keywords=seed_keywords,
                    niche=niche,
                    location=geo,
                    count=20
                ))
            except Exception as e:
                logger.warning(f"AI suggestions failed: {e}")
                ai_suggestions = {'keywords': [], 'error': str(e)}
            
            # Add opportunity scores to all keywords
            try:
                all_discovered = ai_service.enrich_keywords_with_scores(all_discovered)
                all_questions = ai_service.enrich_keywords_with_scores(all_questions)
            except Exception as e:
                logger.warning(f"Opportunity scoring failed: {e}")
        
        response_data = {
            'seed_keywords': seed_keywords,
            'geo': geo,
            'niche': niche,
            'location': location,
            'discovered_keywords': all_discovered,
            'trending_keywords': discovery_result.get('trending_keywords', []),
            'autocomplete_suggestions': discovery_result.get('autocomplete_suggestions', []),
            'questions': all_questions,
            'long_tail_keywords': long_tail_keywords + discovery_result.get('long_tail_keywords', []),
            'niche_keywords': discovery_result.get('niche_keywords', []),
            'local_keywords': discovery_result.get('local_keywords', []),
            'competitor_keywords': discovery_result.get('competitor_keywords', []),
            'interest_data': discovery_result.get('interest_data', []),
            'ai_suggestions': ai_suggestions.get('keywords', []),
            'strategy_summary': ai_suggestions.get('strategy_summary', ''),
            'quick_wins': ai_suggestions.get('quick_wins', []),
            'high_value_targets': ai_suggestions.get('high_value_targets', []),
            'sources_used': discovery_result.get('sources_used', []),
            'stats': discovery_result.get('stats', {}),
            'total_discovered': len(all_discovered)
        }

        # DEBUG LOGGING to verify data presence
        stats = discovery_result.get('stats', {})
        logger.info(f"Enhanced Discovery complete. Stats: "
                    f"Total={stats.get('total_unique', 0)}, "
                    f"Questions={len(response_data['questions'])}, "
                    f"NicheKWs={len(response_data['niche_keywords'])}, "
                    f"LocalKWs={len(response_data['local_keywords'])}, "
                    f"CompetitorKWs={len(response_data['competitor_keywords'])}, "
                    f"Trending={len(response_data['trending_keywords'])}, "
                    f"HighOpportunity={stats.get('high_opportunity', 0)}")
        
        # =====================================================================
        # GENERATE AI ACTIONABLE INSIGHTS
        # =====================================================================
        # This provides strategic recommendations focused on bookings/revenue
        actionable_insights = {}
        business_goal = data.get('business_goal', 'bookings')
        
        if ai_service.is_available():
            try:
                actionable_insights = run_async(ai_service.generate_actionable_insights(
                    keyword_data=discovery_result,
                    business_goal=business_goal,
                    business_type=niche,
                    monthly_budget=data.get('monthly_budget')
                ))
                logger.info(f"AI Insights generated: {len(actionable_insights.get('quick_wins', []))} quick wins")
            except Exception as e:
                logger.warning(f"AI insights generation failed, using fallback: {e}")
                actionable_insights = ai_service._generate_fallback_insights(discovery_result, business_goal)
        else:
            # Use rule-based fallback
            actionable_insights = ai_service._generate_fallback_insights(discovery_result, business_goal)
        
        response_data['actionable_insights'] = actionable_insights
        response_data['ai_insights_available'] = ai_service.is_available()
        
        return jsonify({
            'success': True,
            'data': response_data
        })
        
    except Exception as e:
        logger.error(f"Keyword discovery failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/trends', methods=['POST'])
def get_keyword_trends():
    """
    Get trending and rising keywords from Google Trends.
    
    Request body:
    {
        "keywords": ["dental implants"],
        "geo": "US",
        "timeframe": "today 12-m"  // today 12-m, today 3-m, now 7-d
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keywords = data.get('keywords', [])
        if not keywords or not isinstance(keywords, list):
            return jsonify({
                'success': False, 
                'error': 'keywords must be a non-empty list'
            }), 400
        
        geo = data.get('geo', '')
        timeframe = data.get('timeframe', 'today 12-m')
        
        data_service = get_keyword_data_service(geo)
        result = data_service.get_trending_keywords(
            seed_keywords=keywords,
            geo=geo,
            timeframe=timeframe
        )
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Trends fetch failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/autocomplete', methods=['POST'])
def get_autocomplete():
    """
    Get Google Autocomplete suggestions for a keyword.
    
    Request body:
    {
        "keyword": "dental implants",
        "language": "en",
        "country": "US"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keyword = data.get('keyword', '')
        if not keyword:
            return jsonify({'success': False, 'error': 'keyword is required'}), 400
        
        language = data.get('language', 'en')
        country = data.get('country', '')
        
        data_service = get_keyword_data_service()
        suggestions = data_service.get_autocomplete_suggestions(
            keyword=keyword,
            language=language,
            country=country
        )
        
        return jsonify({
            'success': True,
            'data': {
                'keyword': keyword,
                'suggestions': suggestions
            }
        })
        
    except Exception as e:
        logger.error(f"Autocomplete fetch failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/longtail', methods=['POST'])
def get_long_tail_keywords():
    """
    Generate long-tail keyword variations using multiple techniques.
    
    Request body:
    {
        "keyword": "dental implants",
        "include_alphabet_soup": true,  // Optional, default true
        "include_patterns": true,       // Optional, default true
        "include_modifiers": true       // Optional, default true
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keyword = data.get('keyword', '')
        if not keyword:
            return jsonify({'success': False, 'error': 'keyword is required'}), 400
        
        include_alphabet_soup = data.get('include_alphabet_soup', True)
        include_patterns = data.get('include_patterns', True)
        include_modifiers = data.get('include_modifiers', True)
        
        data_service = get_keyword_data_service()
        long_tails = data_service.generate_long_tail_keywords(
            seed_keyword=keyword,
            include_alphabet_soup=include_alphabet_soup,
            include_patterns=include_patterns,
            include_modifiers=include_modifiers
        )
        
        return jsonify({
            'success': True,
            'data': {
                'seed_keyword': keyword,
                'long_tail_keywords': long_tails,
                'total': len(long_tails)
            }
        })
        
    except Exception as e:
        logger.error(f"Long-tail keyword generation failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/paa', methods=['POST'])
def get_people_also_ask():
    """
    Get 'People Also Ask' questions from Google SERP.
    
    Request body:
    {
        "keyword": "dental implants"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keyword = data.get('keyword', '')
        if not keyword:
            return jsonify({'success': False, 'error': 'keyword is required'}), 400
        
        data_service = get_keyword_data_service()
        questions = data_service.get_people_also_ask(keyword)
        
        return jsonify({
            'success': True,
            'data': {
                'keyword': keyword,
                'questions': questions,
                'total': len(questions)
            }
        })
        
    except Exception as e:
        logger.error(f"People Also Ask fetch failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/high-traffic', methods=['POST'])
def get_high_traffic_keywords():
    """
    AI-powered high-traffic keyword suggestions with prioritization.
    
    Request body:
    {
        "seed_keywords": ["dental implants"],
        "niche": "dental clinic",
        "location": "New York",
        "goal": "traffic"  // traffic, conversions, brand_awareness
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        seed_keywords = data.get('seed_keywords', [])
        if not seed_keywords or not isinstance(seed_keywords, list):
            return jsonify({
                'success': False, 
                'error': 'seed_keywords must be a non-empty list'
            }), 400
        
        niche = data.get('niche', '')
        location = data.get('location', '')
        goal = data.get('goal', 'traffic')
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        # Get AI-powered high-traffic suggestions
        suggestions = run_async(ai_service.suggest_high_traffic_keywords(
            seed_keywords=seed_keywords,
            niche=niche,
            location=location,
            count=30
        ))
        
        # Also get trends data to enhance prioritization
        data_service = get_keyword_data_service()
        trends_data = data_service.get_trending_keywords(seed_keywords)
        
        # Prioritize the keywords
        if suggestions.get('keywords'):
            prioritized = run_async(ai_service.prioritize_keywords(
                keywords=suggestions['keywords'],
                trends_data=trends_data,
                business_goal=goal
            ))
            suggestions['prioritization'] = prioritized
        
        return jsonify({
            'success': True,
            'data': suggestions
        })
        
    except Exception as e:
        logger.error(f"High-traffic keyword generation failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/analyze-opportunity', methods=['POST'])
def analyze_keyword_opportunity():
    """
    Deep analysis of a specific keyword's opportunity.
    
    Request body:
    {
        "keyword": "dental implants near me",
        "competitor_data": [...]  // Optional
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keyword = data.get('keyword', '')
        if not keyword:
            return jsonify({'success': False, 'error': 'keyword is required'}), 400
        
        competitor_data = data.get('competitor_data', None)
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            return jsonify({
                'success': False,
                'error': 'AI service not available. Please set GEMINI_API_KEY.'
            }), 503
        
        result = run_async(ai_service.analyze_keyword_opportunity(
            keyword=keyword,
            competitor_data=competitor_data
        ))
        
        return jsonify({
            'success': True,
            'data': result
        })
        
    except Exception as e:
        logger.error(f"Keyword opportunity analysis failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/enrich', methods=['POST'])
def enrich_keywords_with_data():
    """
    Enrich a list of keywords with traffic and trend data.
    
    Request body:
    {
        "keywords": ["keyword1", "keyword2"],
        "geo": "US"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        keywords = data.get('keywords', [])
        if not keywords or not isinstance(keywords, list):
            return jsonify({
                'success': False, 
                'error': 'keywords must be a non-empty list'
            }), 400
        
        geo = data.get('geo', '')
        
        data_service = get_keyword_data_service(geo)
        enriched = data_service.enrich_keywords(keywords, geo=geo)
        
        return jsonify({
            'success': True,
            'data': {
                'keywords': enriched
            }
        })
        
    except Exception as e:
        logger.error(f"Keyword enrichment failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@keyword_bp.route('/actionable-insights', methods=['POST'])
def get_actionable_insights():
    """
    Generate AI-powered actionable insights from keyword discovery data.
    Focuses on easy-to-rank keywords that will increase bookings/revenue.
    
    Request body:
    {
        "keyword_data": { ... discovery results ... },  // Optional, will fetch if not provided
        "seed_keywords": ["dental", "insurance"],       // Required if keyword_data not provided
        "geo": "US",
        "niche": "dental",
        "location": "Boston",
        "business_goal": "bookings",      // bookings, revenue, traffic, brand_awareness
        "business_type": "dental clinic", // Optional
        "monthly_budget": "medium"        // Optional: low, medium, high
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'error': 'Request body required'}), 400
        
        # Get or generate keyword data
        keyword_data = data.get('keyword_data')
        
        if not keyword_data:
            # Need to discover keywords first
            seed_keywords = data.get('seed_keywords', data.get('keywords', []))
            if not seed_keywords:
                return jsonify({
                    'success': False, 
                    'error': 'Either keyword_data or seed_keywords is required'
                }), 400
            
            geo = data.get('geo', '')
            niche = data.get('niche', '')
            location = data.get('location', '')
            
            # Discover keywords using enhanced method
            data_service = get_keyword_data_service(geo)
            keyword_data = data_service.discover_keywords_enhanced(
                seed_keywords=seed_keywords,
                geo=geo,
                niche=niche,
                location=location,
                include_trends=True,
                include_autocomplete=True,
                include_questions=True,
                include_niche_patterns=bool(niche),
                include_competitor_templates=True
            )
        
        # Get AI insights
        business_goal = data.get('business_goal', 'bookings')
        business_type = data.get('business_type', data.get('niche', ''))
        monthly_budget = data.get('monthly_budget')
        
        ai_service = get_ai_service()
        if not ai_service.is_available():
            # Return rule-based fallback
            insights = ai_service._generate_fallback_insights(keyword_data, business_goal)
            return jsonify({
                'success': True,
                'data': {
                    'insights': insights,
                    'ai_available': False,
                    'message': 'Using rule-based analysis. Set GEMINI_API_KEY for AI-powered insights.'
                }
            })
        
        # Generate AI insights
        insights = run_async(ai_service.generate_actionable_insights(
            keyword_data=keyword_data,
            business_goal=business_goal,
            business_type=business_type,
            monthly_budget=monthly_budget
        ))
        
        return jsonify({
            'success': True,
            'data': {
                'insights': insights,
                'ai_available': True,
                'keyword_stats': keyword_data.get('stats', {}),
                'total_keywords_analyzed': keyword_data.get('total_discovered', 0)
            }
        })
        
    except Exception as e:
        logger.error(f"Actionable insights generation failed: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

