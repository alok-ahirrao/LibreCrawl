from flask import Blueprint, request, jsonify, session
import asyncio
import logging
import threading
from typing import Dict, Any

from .ai_service import AuditAIService
from src.keyword.keyword_analyzer import KeywordDensityAnalyzer
from src.crawl_db import get_crawl_by_id, load_crawled_urls, load_crawl_issues, load_crawl_links, get_audit_insights, save_audit_insights

logger = logging.getLogger(__name__)

audit_bp = Blueprint('audit', __name__, url_prefix='/api/audit')

# Thread-local storage for event loops
_local = threading.local()

def run_async(coro):
    """Helper to run async coroutines in Flask - creates fresh loop each time."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except:
            pass
        loop.close()

# Shared AI service instance
_ai_service = None

def get_ai_service():
    global _ai_service
    if _ai_service is None:
        _ai_service = AuditAIService()
    return _ai_service

@audit_bp.route('/chat', methods=['POST'])
def chat_with_audit():
    """
    Chat with the audit data.
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400

        question = data.get('question')
        crawl_id = data.get('crawl_id') or session.get('current_crawl_id')

        if not question:
            return jsonify({'success': False, 'error': 'No question provided'}), 400

        crawl = get_crawl_by_id(crawl_id)
        if not crawl:
            return jsonify({'success': False, 'error': 'Crawl not found'}), 404

        # Prepare Audit Data
        stats = {
            'crawled': crawl['urls_crawled'],
            'discovered': crawl['urls_discovered'],
            'base_url': crawl['base_url']
        }

        issues = load_crawl_issues(crawl_id)
        issues_list = [dict(i) for i in issues] if issues else []

        urls = load_crawled_urls(crawl_id, limit=100)
        urls_list = [dict(u) for u in urls] if urls else []

        audit_data = {
            'stats': stats,
            'issues': issues_list,
            'urls': urls_list
        }

        ai_service = get_ai_service()
        # Use run_async helper for async call in sync route
        answer = run_async(ai_service.chat_with_audit(question, audit_data))
        return jsonify({'success': True, 'answer': answer})

    except Exception as e:
        logger.error(f"Error in chat_with_audit: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@audit_bp.route('/insights', methods=['POST'])
def generate_audit_insights():
    """
    Generate or retrieve AI insights for a crawl.
    """
    try:
        data = request.get_json()
        crawl_id = data.get('crawl_id') or session.get('current_crawl_id')
        regenerate = data.get('regenerate', False) # Allow forcing fresh analysis
        
        if not crawl_id:
            return jsonify({'success': False, 'error': 'No crawl ID provided'}), 400
            
        crawl = get_crawl_by_id(crawl_id)
        if not crawl:
            return jsonify({'success': False, 'error': 'Crawl not found'}), 404

        # Check for existing insights (unless regenerating)
        if not regenerate:
            stored_insights = get_audit_insights(crawl_id)
            if stored_insights:
                return jsonify({'success': True, 'insights': stored_insights, 'source': 'database'})

        # Prepare Data
        stats = {
            'crawled': crawl['urls_crawled'],
            'discovered': crawl['urls_discovered'],
            'base_url': crawl['base_url']
        }
        
        issues = load_crawl_issues(crawl_id)
        issues_list = [dict(i) for i in issues] if issues else []
        
        urls = load_crawled_urls(crawl_id, limit=200)
        urls_list = [dict(u) for u in urls] if urls else []

        audit_data = {
            'stats': stats,
            'issues': issues_list,
            'urls': urls_list
        }
        
        ai_service = get_ai_service()
        
        insights = run_async(ai_service.generate_insights(audit_data))
        
        # Integrate Keyword Data
        try:
            # Run keyword analysis synchronously (via run_async helper)
            # disabling AI to keep it fast/robust for saving
            analyzer = KeywordDensityAnalyzer()
            keyword_res = run_async(analyzer.analyze_page(crawl['base_url'], use_ai=False))
            if keyword_res and 'error' not in keyword_res:
                insights['keyword_data'] = keyword_res
        except Exception as e:
            logger.error(f"Error merging keyword data into audit: {e}")

        # Save to database
        if insights and 'error' not in insights:
            save_audit_insights(crawl_id, insights)
            
        return jsonify({'success': True, 'insights': insights, 'source': 'ai'})

    except Exception as e:
        logger.error(f"Error generating insights: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
