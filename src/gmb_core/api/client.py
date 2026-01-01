"""
GMB API Client
Client for interacting with Google Business Profile API.
"""
import time
from functools import wraps
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from .auth import GMBAuthManager
from ..config import config


def rate_limited(func):
    """Decorator to enforce rate limiting on API calls."""
    last_call_time = [0.0]
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        elapsed = time.time() - last_call_time[0]
        min_interval = 1.0 / config.API_RATE_LIMIT
        
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        
        result = func(*args, **kwargs)
        last_call_time[0] = time.time()
        return result
    
    return wrapper


def retry_on_error(max_retries=3, backoff_factor=2):
    """Decorator for exponential backoff retry on API errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    if e.resp.status in [429, 500, 503]:
                        retries += 1
                        if retries < max_retries:
                            wait_time = backoff_factor ** retries
                            print(f"API error {e.resp.status}, retrying in {wait_time}s...")
                            time.sleep(wait_time)
                        else:
                            raise
                    else:
                        raise
            return None
        return wrapper
    return decorator


class GMBClient:
    """
    Client for interacting with Google Business Profile API.
    Handles locations, reviews, insights, and posts.
    """
    
    def __init__(self, user_id: int):
        self.auth_manager = GMBAuthManager(user_id)
        self._service_cache = {}
    
    def _get_credentials_object(self, account_id: int) -> Credentials:
        """Helper to get Credentials object from stored tokens."""
        creds_dict = self.auth_manager.get_credentials(account_id)
        return Credentials(
            token=creds_dict['token'],
            refresh_token=creds_dict.get('refresh_token'),
            token_uri=creds_dict.get('token_uri'),
            client_id=creds_dict.get('client_id'),
            client_secret=creds_dict.get('client_secret'),
            scopes=creds_dict.get('scopes')
        )
    
    def _get_service(self, account_id: int, service_name: str, version: str):
        """Build and cache authorized service object."""
        cache_key = f"{account_id}_{service_name}_{version}"
        
        if cache_key not in self._service_cache:
            creds = self._get_credentials_object(account_id)
            self._service_cache[cache_key] = build(service_name, version, credentials=creds)
        
        return self._service_cache[cache_key]
    
    # ==================== Account Management ====================
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def list_accounts(self, account_id_db: int) -> list:
        """
        List all GBP accounts accessible by the credentials.
        
        Returns:
            List of account dicts with 'name', 'accountName', 'type', etc.
        """
        service = self._get_service(account_id_db, 'mybusinessaccountmanagement', 'v1')
        result = service.accounts().list().execute()
        return result.get('accounts', [])
    
    # ==================== Location Management ====================
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def list_locations(self, account_id_db: int, gmb_account_name: str) -> list:
        """
        List locations for a specific GMB account.
        
        Args:
            account_id_db: Database ID of the GMB account
            gmb_account_name: GMB account name (e.g., 'accounts/12345')
            
        Returns:
            List of location dicts
        """
        service = self._get_service(account_id_db, 'mybusinessbusinessinformation', 'v1')
        
        read_mask = ','.join([
            'name',
            'title', 
            'storeCode',
            'latlng',
            'categories',
            'storefrontAddress',
            'metadata',
            'phoneNumbers',
            'websiteUri',
            'regularHours',
            'serviceArea'
        ])
        
        result = service.accounts().locations().list(
            parent=gmb_account_name,
            readMask=read_mask
        ).execute()
        
        return result.get('locations', [])
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def get_location(self, account_id_db: int, location_name: str) -> dict:
        """
        Get a single location's details.
        
        Args:
            account_id_db: Database ID of the GMB account
            location_name: Full location name (e.g., 'locations/12345')
        """
        service = self._get_service(account_id_db, 'mybusinessbusinessinformation', 'v1')
        
        read_mask = ','.join([
            'name',
            'title',
            'storeCode', 
            'latlng',
            'categories',
            'storefrontAddress',
            'phoneNumbers',
            'websiteUri',
            'regularHours',
            'metadata'
        ])
        
        return service.locations().get(
            name=location_name,
            readMask=read_mask
        ).execute()
    
    # ==================== Reviews ====================
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def list_reviews(self, account_id_db: int, location_name: str, page_size: int = 50, page_token: str = None) -> dict:
        """
        List reviews for a location.
        
        Args:
            account_id_db: Database ID of the GMB account
            location_name: Full location name (e.g., 'locations/12345')
            page_size: Number of reviews per page (max 50)
            page_token: Token for pagination
            
        Returns:
            dict with 'reviews' list, 'averageRating', 'totalReviewCount', 'nextPageToken'
        """
        service = self._get_service(account_id_db, 'mybusinessbusinessinformation', 'v1')
        
        # Build request parameters
        params = {
            'parent': location_name,
            'pageSize': min(page_size, 50)
        }
        
        if page_token:
            params['pageToken'] = page_token
        
        # Note: Reviews are accessed via a different API
        # Using mybusiness.googleapis.com/v4 for reviews
        creds = self._get_credentials_object(account_id_db)
        reviews_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl='https://mybusiness.googleapis.com/$discovery/rest?version=v4')
        
        result = reviews_service.accounts().locations().reviews().list(
            parent=location_name,
            pageSize=min(page_size, 50),
            pageToken=page_token
        ).execute()
        
        return {
            'reviews': result.get('reviews', []),
            'averageRating': result.get('averageRating'),
            'totalReviewCount': result.get('totalReviewCount'),
            'nextPageToken': result.get('nextPageToken')
        }
    
    def list_all_reviews(self, account_id_db: int, location_name: str) -> list:
        """
        Fetch all reviews for a location (handles pagination).
        
        Returns:
            List of all review dicts
        """
        all_reviews = []
        page_token = None
        
        while True:
            result = self.list_reviews(account_id_db, location_name, page_token=page_token)
            all_reviews.extend(result.get('reviews', []))
            
            page_token = result.get('nextPageToken')
            if not page_token:
                break
        
        return all_reviews
    
    @rate_limited
    @retry_on_error(max_retries=3) 
    def reply_to_review(self, account_id_db: int, review_name: str, reply_comment: str) -> dict:
        """
        Reply to a review.
        
        Args:
            account_id_db: Database ID of the GMB account
            review_name: Full review name (e.g., 'accounts/123/locations/456/reviews/789')
            reply_comment: The reply text
            
        Returns:
            The reply object
        """
        creds = self._get_credentials_object(account_id_db)
        reviews_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl='https://mybusiness.googleapis.com/$discovery/rest?version=v4')
        
        result = reviews_service.accounts().locations().reviews().updateReply(
            name=review_name,
            body={'comment': reply_comment}
        ).execute()
        
        return result
    
    # ==================== Insights ====================
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def get_insights(self, account_id_db: int, location_name: str, metric: str = 'ALL', days: int = 30) -> dict:
        """
        Get business insights/metrics for a location.
        
        Args:
            account_id_db: Database ID of the GMB account
            location_name: Full location name
            metric: Metric type ('ALL', 'QUERIES_DIRECT', 'VIEWS_MAPS', etc.)
            days: Number of days of data (max 540)
            
        Returns:
            dict with insights data
        """
        creds = self._get_credentials_object(account_id_db)
        
        # Insights use the v4 API
        insights_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl='https://mybusiness.googleapis.com/$discovery/rest?version=v4')
        
        # Calculate date range
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        basic_metrics = [
            'QUERIES_DIRECT',
            'QUERIES_INDIRECT', 
            'VIEWS_MAPS',
            'VIEWS_SEARCH',
            'ACTIONS_WEBSITE',
            'ACTIONS_PHONE',
            'ACTIONS_DRIVING_DIRECTIONS'
        ]
        
        result = insights_service.accounts().locations().reportInsights(
            name=location_name.rsplit('/', 2)[0],  # Get account name
            body={
                'locationNames': [location_name],
                'basicRequest': {
                    'metricRequests': [{'metric': m} for m in basic_metrics],
                    'timeRange': {
                        'startTime': start_date.isoformat() + 'Z',
                        'endTime': end_date.isoformat() + 'Z'
                    }
                }
            }
        ).execute()
        
        return result
    
    # ==================== Posts ====================
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def create_post(self, account_id_db: int, location_name: str, post_data: dict) -> dict:
        """
        Create a new GMB post.
        
        Args:
            account_id_db: Database ID of the GMB account
            location_name: Full location name
            post_data: Post content dict with 'summary', 'callToAction', 'media', etc.
            
        Returns:
            Created post object
        """
        creds = self._get_credentials_object(account_id_db)
        posts_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl='https://mybusiness.googleapis.com/$discovery/rest?version=v4')
        
        result = posts_service.accounts().locations().localPosts().create(
            parent=location_name,
            body=post_data
        ).execute()
        
        return result
    
    @rate_limited
    @retry_on_error(max_retries=3)
    def list_posts(self, account_id_db: int, location_name: str, page_size: int = 100) -> list:
        """
        List posts for a location.
        """
        creds = self._get_credentials_object(account_id_db)
        posts_service = build('mybusiness', 'v4', credentials=creds, discoveryServiceUrl='https://mybusiness.googleapis.com/$discovery/rest?version=v4')
        
        result = posts_service.accounts().locations().localPosts().list(
            parent=location_name,
            pageSize=min(page_size, 100)
        ).execute()
        
        return result.get('localPosts', [])
