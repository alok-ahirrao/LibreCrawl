"""
PostgreSQL Schema Verification Script
Checks if all expected tables and columns exist in the database.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Force PostgreSQL mode
os.environ['DB_TYPE'] = 'postgres'

from src.database import get_db

# Expected schema definition
EXPECTED_SCHEMA = {
    # From auth_db.py
    'users': [
        'id', 'username', 'email', 'password_hash', 'created_at', 
        'last_login', 'is_verified', 'tier'
    ],
    'user_settings': ['user_id', 'settings_json', 'updated_at'],
    'crawl_history': [
        'id', 'user_id', 'base_url', 'started_at', 'completed_at', 
        'urls_crawled', 'status'
    ],
    'guest_crawls': ['id', 'ip_address', 'created_at'],
    'verification_tokens': [
        'id', 'user_id', 'token', 'app_source', 'expires_at', 'created_at'
    ],
    
    # From crawl_db.py
    'crawls': [
        'id', 'user_id', 'session_id', 'base_url', 'base_domain', 'status',
        'config_snapshot', 'urls_discovered', 'urls_crawled', 'max_depth_reached',
        'started_at', 'completed_at', 'last_saved_at', 'peak_memory_mb',
        'estimated_size_mb', 'can_resume', 'resume_checkpoint', 'sitemap_urls',
        'resumable', 'pagespeed_results', 'robots_data', 'llms_data',
        'show_to_client', 'client_id'
    ],
    'crawled_urls': [
        'id', 'crawl_id', 'url', 'status_code', 'content_type', 'size',
        'is_internal', 'depth', 'title', 'meta_description', 'h1', 'h2', 'h3',
        'word_count', 'canonical_url', 'lang', 'charset', 'viewport', 'robots',
        'meta_tags', 'og_tags', 'twitter_tags', 'json_ld', 'analytics', 'images',
        'hreflang', 'schema_org', 'redirects', 'linked_from', 'external_links',
        'internal_links', 'response_time', 'javascript_rendered', 'dom_size',
        'dom_depth', 'requires_js', 'raw_html_hash', 'rendered_html_hash', 'crawled_at'
    ],
    'crawl_links': [
        'id', 'crawl_id', 'source_url', 'target_url', 'anchor_text',
        'is_internal', 'is_nofollow', 'target_status', 'placement', 'scope', 'created_at'
    ],
    'crawl_issues': [
        'id', 'crawl_id', 'url', 'type', 'category', 'issue', 'details', 
        'severity', 'created_at'
    ],
    'audit_insights': ['id', 'crawl_id', 'insights_json', 'created_at', 'updated_at'],
    
    # From keyword_db.py
    'keyword_history': [
        'id', 'type', 'user_id', 'input_params', 'results', 
        'show_to_client', 'client_id', 'created_at'
    ],
    'content_items': [
        'id', 'cluster_topic', 'primary_keyword', 'secondary_keywords',
        'content_type', 'content_type_name', 'intent', 'confidence', 'status',
        'priority_tier', 'priority_score', 'scheduled_date', 'week_number',
        'notes', 'brief', 'user_id', 'client_id', 'campaign_title', 
        'website_url', 'created_at', 'updated_at'
    ],
    
    # From gmb_core/models.py
    'gmb_accounts': [
        'id', 'user_id', 'email', 'access_token', 'refresh_token',
        'token_expiry', 'is_active', 'created_at', 'updated_at'
    ],
    'gmb_locations': [
        'id', 'account_id', 'google_location_id', 'google_account_id',
        'location_name', 'address_lines', 'locality', 'region', 'postal_code',
        'country', 'lat', 'lng', 'primary_category', 'additional_categories',
        'website_url', 'phone_number', 'source_url', 'description', 'business_hours',
        'attributes', 'service_area_type', 'service_area_places', 'post_count',
        'last_post_date', 'qa_count', 'total_reviews', 'rating', 'photo_count',
        'last_synced_at', 'created_at'
    ],
    'gmb_reviews': [
        'id', 'location_id', 'google_review_id', 'reviewer_name',
        'reviewer_photo_url', 'star_rating', 'comment', 'review_reply',
        'reply_time', 'create_time', 'update_time', 'sentiment_score',
        'sentiment_keywords', 'created_at'
    ],
    'gmb_grid_scans': [
        'id', 'location_id', 'keyword', 'target_business', 'target_place_id',
        'center_lat', 'center_lng', 'radius_meters', 'grid_size', 'status',
        'total_points', 'completed_points', 'average_rank', 'show_to_client',
        'client_id', 'started_at', 'completed_at'
    ],
    'gmb_grid_results': [
        'id', 'scan_id', 'point_index', 'lat', 'lng', 'target_rank',
        'target_found', 'top_results', 'crawled_at', 'error'
    ],
    'gmb_competitors': [
        'id', 'place_id', 'name', 'primary_category', 'additional_categories',
        'rating', 'review_count', 'photo_count', 'attributes', 'hours',
        'services', 'post_count', 'q_and_a_count', 'last_scraped_at', 'created_at'
    ],
    'gmb_api_cache': ['id', 'cache_key', 'response_data', 'expires_at', 'created_at'],
    'gmb_serp_cache': [
        'id', 'keyword', 'lat', 'lng', 'results_json', 'expires_at', 'created_at'
    ],
    'competitive_analyses': [
        'id', 'user_place_id', 'keyword', 'competitor_ids', 'deficits', 'created_at'
    ],
    'gmb_categories': [
        'id', 'category_id', 'display_name', 'parent_category', 'is_primary_eligible'
    ],
    'serp_searches': [
        'id', 'keyword', 'location', 'lat', 'lng', 'device', 'language', 'depth',
        'organic_count', 'local_pack_count', 'hotel_count', 'shopping_count',
        'target_rank', 'target_url', 'results_json', 'ai_overview_present',
        'show_to_client', 'client_id', 'created_at'
    ],
    'gmb_health_snapshots': [
        'id', 'location_id', 'snapshot_date', 'overall_score', 'profile_score',
        'photos_score', 'reviews_score', 'posts_score', 'qa_score', 'metrics_json',
        'show_to_client', 'created_at'
    ],
    'gmb_audit_logs': [
        'id', 'entity_type', 'entity_id', 'action', 'before_state', 'after_state',
        'actor_type', 'actor_id', 'source', 'source_ip', 'created_at'
    ],
    'gmb_sync_jobs': [
        'id', 'job_id', 'user_id', 'account_id', 'location_id', 'job_type',
        'status', 'priority', 'progress', 'total_items', 'retry_count',
        'max_retries', 'error_message', 'created_at', 'started_at', 'completed_at'
    ],
    'gmb_quota_logs': [
        'id', 'endpoint', 'method', 'quota_used', 'response_code',
        'response_time_ms', 'error_message', 'created_at'
    ],
}


def get_postgres_tables():
    """Get list of all tables in the PostgreSQL database."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        return [row['table_name'] for row in cursor.fetchall()]


def get_table_columns(table_name):
    """Get list of columns for a specific table."""
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        return [row['column_name'] for row in cursor.fetchall()]


def get_table_row_count(table_name):
    """Get approximate row count for a table."""
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
            result = cursor.fetchone()
            return result['count'] if result else 0
    except:
        return -1


def verify_schema():
    """Verify PostgreSQL schema against expected schema."""
    print("=" * 70)
    print("PostgreSQL Schema Verification Report")
    print("=" * 70)
    print()
    
    # Get actual tables
    try:
        actual_tables = get_postgres_tables()
    except Exception as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
        return False
    
    print(f"[INFO] Found {len(actual_tables)} tables in PostgreSQL")
    print()
    
    # Track results
    missing_tables = []
    missing_columns = {}
    extra_tables = []
    table_stats = []
    
    # Check for expected tables
    print("-" * 70)
    print("TABLE STATUS")
    print("-" * 70)
    
    for table_name, expected_columns in EXPECTED_SCHEMA.items():
        if table_name in actual_tables:
            # Table exists, check columns
            actual_columns = get_table_columns(table_name)
            row_count = get_table_row_count(table_name)
            
            missing_cols = [col for col in expected_columns if col not in actual_columns]
            extra_cols = [col for col in actual_columns if col not in expected_columns]
            
            if missing_cols:
                status = "[WARN]"
                missing_columns[table_name] = missing_cols
            else:
                status = "[OK]  "
            
            print(f"{status:10} {table_name:30} ({row_count:>6} rows, {len(actual_columns):>2} cols)")
            
            table_stats.append({
                'name': table_name,
                'rows': row_count,
                'cols': len(actual_columns),
                'missing_cols': missing_cols,
                'extra_cols': extra_cols
            })
        else:
            print(f"[MISSING]  {table_name}")
            missing_tables.append(table_name)
    
    # Check for extra tables not in our schema
    for table in actual_tables:
        if table not in EXPECTED_SCHEMA:
            extra_tables.append(table)
    
    print()
    print("-" * 70)
    print("SUMMARY")
    print("-" * 70)
    
    total_expected = len(EXPECTED_SCHEMA)
    total_found = total_expected - len(missing_tables)
    
    print(f"Expected Tables:  {total_expected}")
    print(f"Found Tables:     {total_found}")
    print(f"Missing Tables:   {len(missing_tables)}")
    print(f"Extra Tables:     {len(extra_tables)}")
    print()
    
    # Report missing tables
    if missing_tables:
        print("[X] MISSING TABLES:")
        for table in missing_tables:
            print(f"   - {table}")
        print()
    
    # Report missing columns
    if missing_columns:
        print("[!] MISSING COLUMNS:")
        for table, cols in missing_columns.items():
            print(f"   {table}:")
            for col in cols:
                print(f"      - {col}")
        print()
    
    # Report extra tables
    if extra_tables:
        print("[i] EXTRA TABLES (not in expected schema):")
        for table in extra_tables:
            row_count = get_table_row_count(table)
            print(f"   - {table} ({row_count} rows)")
        print()
    
    # Data summary
    print("-" * 70)
    print("DATA SUMMARY")
    print("-" * 70)
    
    tables_with_data = [t for t in table_stats if t['rows'] > 0]
    empty_tables = [t for t in table_stats if t['rows'] == 0]
    
    print(f"Tables with data: {len(tables_with_data)}")
    print(f"Empty tables:     {len(empty_tables)}")
    print()
    
    if tables_with_data:
        print("Tables with data:")
        for t in sorted(tables_with_data, key=lambda x: x['rows'], reverse=True):
            print(f"   - {t['name']:30} - {t['rows']:>6} rows")
    
    print()
    print("=" * 70)
    
    # Overall status
    if not missing_tables and not missing_columns:
        print("[OK] SCHEMA VERIFICATION PASSED - All tables and columns present!")
        return True
    else:
        print("[!!] SCHEMA VERIFICATION INCOMPLETE - Some tables/columns missing")
        print("     Run table initialization to create missing schema.")
        return False


if __name__ == '__main__':
    print()
    verify_schema()
    print()
