"""
GMB Core Configuration
Centralized environment configuration for OAuth, proxies, and rate limits.
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """
    Configuration class for GMB Core module.
    All sensitive values should be set via environment variables.
    """
    
    # Google OAuth 2.0 Configuration
    GOOGLE_CLIENT_ID = os.getenv('GOOGLE_GMB_CLIENT_ID', '')
    GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_GMB_CLIENT_SECRET', '')
    
    # OAuth URLs
    GOOGLE_AUTH_URI = 'https://accounts.google.com/o/oauth2/v2/auth'
    GOOGLE_TOKEN_URI = 'https://oauth2.googleapis.com/token'
    GOOGLE_REVOKE_URI = 'https://oauth2.googleapis.com/revoke'
    
    # Redirect URI (should match Google Cloud Console settings)
    # In production, this should be the actual domain
    OAUTH_REDIRECT_URI = os.getenv(
        'GOOGLE_GMB_REDIRECT_URI', 
        'http://localhost:3000/api/gmb/callback'
    )
    
    # OAuth Scopes for GBP API
    OAUTH_SCOPES = [
        'https://www.googleapis.com/auth/business.manage',
        'openid',
        'email',
        'profile'
    ]
    
    # Rate Limiting
    API_RATE_LIMIT = float(os.getenv('GMB_API_RATE_LIMIT', '1.0'))  # requests per second
    CRAWLER_RATE_LIMIT = float(os.getenv('GMB_CRAWLER_RATE_LIMIT', '0.2'))  # 1 request per 5 seconds
    
    # Proxy Configuration (for geo-targeted crawling)
    PROXY_ENABLED = os.getenv('GMB_PROXY_ENABLED', 'false').lower() == 'true'
    PROXY_URL = os.getenv('GMB_PROXY_URL', '')  # e.g., http://user:pass@proxy.example.com:8080
    PROXY_ROTATION_ENABLED = os.getenv('GMB_PROXY_ROTATION', 'false').lower() == 'true'
    
    # Crawler Settings
    CRAWLER_HEADLESS = os.getenv('GMB_CRAWLER_HEADLESS', 'true').lower() == 'true'
    CRAWLER_TIMEOUT = int(os.getenv('GMB_CRAWLER_TIMEOUT', '30000'))  # milliseconds
    CRAWLER_MAX_RETRIES = int(os.getenv('GMB_CRAWLER_MAX_RETRIES', '3'))
    
    # Cache TTL (seconds)
    CACHE_TTL_API_RESPONSE = int(os.getenv('GMB_CACHE_TTL_API', '900'))  # 15 minutes
    CACHE_TTL_SERP_RESULT = int(os.getenv('GMB_CACHE_TTL_SERP', '3600'))  # 1 hour
    
    # Performance Settings
    CRAWLER_FAST_MODE = os.getenv('GMB_CRAWLER_FAST_MODE', 'true').lower() == 'true'
    CRAWLER_WORKERS = int(os.getenv('GMB_CRAWLER_WORKERS', '5'))  # Increased default for fast mode
    
    # SERP API Configuration (for fast mode without browser)
    # Supports ScraperAPI, SerpAPI, or similar services
    # Set SERP_API_PROVIDER to 'scraperapi', 'serpapi', or 'none' (default: none - use browser)
    SERP_API_PROVIDER = os.getenv('SERP_API_PROVIDER', 'none').lower()
    SERP_API_KEY = os.getenv('SERP_API_KEY', '')  # API key for the SERP service
    
    # Anti-CAPTCHA Configuration (for automatic CAPTCHA solving)
    # Supports 2captcha, anticaptcha, or 'none' (default: manual solving)
    CAPTCHA_PROVIDER = os.getenv('CAPTCHA_PROVIDER', 'none').lower()
    CAPTCHA_API_KEY = os.getenv('CAPTCHA_API_KEY', '')  # API key for CAPTCHA service
    
    # Database
    DATABASE_FILE = os.getenv('GMB_DATABASE_FILE', 'users.db')
    
    @classmethod
    def is_configured(cls) -> bool:
        """Check if required OAuth credentials are configured."""
        return bool(cls.GOOGLE_CLIENT_ID and cls.GOOGLE_CLIENT_SECRET)
    
    @classmethod
    def get_oauth_config(cls) -> dict:
        """Return OAuth configuration as dictionary."""
        return {
            'client_id': cls.GOOGLE_CLIENT_ID,
            'client_secret': cls.GOOGLE_CLIENT_SECRET,
            'auth_uri': cls.GOOGLE_AUTH_URI,
            'token_uri': cls.GOOGLE_TOKEN_URI,
            'redirect_uri': cls.OAUTH_REDIRECT_URI,
            'scopes': cls.OAUTH_SCOPES
        }


# Create singleton instance
config = Config()
