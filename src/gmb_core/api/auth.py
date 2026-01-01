"""
GMB OAuth2 Authentication Manager
Handles OAuth flow, token storage, and credential management for GBP API.
"""
import json
import os
import secrets
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from ..models import get_db
from ..config import config

# OAuth Scopes
SCOPES = config.OAUTH_SCOPES


class GMBAuthManager:
    """
    Manages OAuth2 flow and Token storage for Google Business Profile API.
    Isolated from main app auth.
    """
    
    def __init__(self, user_id: int):
        self.user_id = user_id
    
    def get_auth_url(self, state: str = None) -> dict:
        """
        Generate Google OAuth2 consent URL.
        
        Args:
            state: Optional state parameter for CSRF protection. 
                   If not provided, generates a random one.
        
        Returns:
            dict with 'url' and 'state' keys
        """
        if not config.is_configured():
            raise ValueError("Google OAuth not configured. Set GOOGLE_GMB_CLIENT_ID and GOOGLE_GMB_CLIENT_SECRET environment variables.")
        
        if not state:
            state = secrets.token_urlsafe(32)
        
        params = {
            'client_id': config.GOOGLE_CLIENT_ID,
            'redirect_uri': config.OAUTH_REDIRECT_URI,
            'response_type': 'code',
            'scope': ' '.join(SCOPES),
            'access_type': 'offline',  # Required to get refresh_token
            'prompt': 'consent',  # Force consent to always get refresh_token
            'state': state,
            'include_granted_scopes': 'true'
        }
        
        url = f"{config.GOOGLE_AUTH_URI}?{urlencode(params)}"
        
        return {
            'url': url,
            'state': state
        }
    
    def exchange_code(self, code: str) -> dict:
        """
        Exchange authorization code for access and refresh tokens.
        
        Args:
            code: The authorization code from Google OAuth callback
            
        Returns:
            dict with token info and user email
        """
        if not config.is_configured():
            raise ValueError("Google OAuth not configured.")
        
        # Exchange code for tokens
        token_data = {
            'client_id': config.GOOGLE_CLIENT_ID,
            'client_secret': config.GOOGLE_CLIENT_SECRET,
            'code': code,
            'grant_type': 'authorization_code',
            'redirect_uri': config.OAUTH_REDIRECT_URI
        }
        
        response = requests.post(
            config.GOOGLE_TOKEN_URI,
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status_code != 200:
            error_data = response.json()
            raise Exception(f"Token exchange failed: {error_data.get('error_description', error_data.get('error', 'Unknown error'))}")
        
        tokens = response.json()
        
        # Get user info to identify the account
        user_info = self._get_user_info(tokens['access_token'])
        email = user_info.get('email', 'unknown@gmail.com')
        
        # Save tokens to database
        account_id = self.save_tokens(
            email=email,
            access_token=tokens['access_token'],
            refresh_token=tokens.get('refresh_token'),
            expiry_seconds=tokens.get('expires_in', 3600)
        )
        
        return {
            'account_id': account_id,
            'email': email,
            'access_token': tokens['access_token'],
            'expires_in': tokens.get('expires_in', 3600)
        }
    
    def _get_user_info(self, access_token: str) -> dict:
        """Fetch user info from Google to get email."""
        response = requests.get(
            'https://www.googleapis.com/oauth2/v2/userinfo',
            headers={'Authorization': f'Bearer {access_token}'}
        )
        
        if response.status_code == 200:
            return response.json()
        return {}
    
    def get_credentials(self, account_id: int) -> dict:
        """
        Retrieve valid Credentials object for a specific account.
        Auto-refreshes if expired.
        """
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT * FROM gmb_accounts WHERE id = ? AND user_id = ?', 
                (account_id, self.user_id)
            )
            row = cursor.fetchone()
            
            if not row:
                raise ValueError("GMB Account not found")
            
            account = dict(row)
            
            # Check if expired
            expiry = None
            if account.get('token_expiry'):
                try:
                    expiry = datetime.fromisoformat(account['token_expiry'])
                except:
                    pass
            
            if expiry and expiry < datetime.now():
                print(f"Token expired for account {account_id}, refreshing...")
                return self.refresh_token(account)
            
            return {
                'token': account['access_token'],
                'refresh_token': account['refresh_token'],
                'token_uri': config.GOOGLE_TOKEN_URI,
                'client_id': config.GOOGLE_CLIENT_ID,
                'client_secret': config.GOOGLE_CLIENT_SECRET,
                'scopes': SCOPES
            }

    def save_tokens(self, email: str, access_token: str, refresh_token: str, expiry_seconds: int) -> int:
        """Save or update tokens in DB."""
        expiry = datetime.now() + timedelta(seconds=expiry_seconds)
        
        with get_db() as conn:
            cursor = conn.cursor()
            
            # Check if exists
            cursor.execute(
                'SELECT id, refresh_token FROM gmb_accounts WHERE user_id = ? AND email = ?', 
                (self.user_id, email)
            )
            existing = cursor.fetchone()
            
            if existing:
                # Preserve existing refresh_token if new one not provided
                final_refresh_token = refresh_token or existing['refresh_token']
                
                cursor.execute('''
                    UPDATE gmb_accounts 
                    SET access_token = ?, refresh_token = ?, token_expiry = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (access_token, final_refresh_token, expiry.isoformat(), existing['id']))
                return existing['id']
            else:
                cursor.execute('''
                    INSERT INTO gmb_accounts (user_id, email, access_token, refresh_token, token_expiry)
                    VALUES (?, ?, ?, ?, ?)
                ''', (self.user_id, email, access_token, refresh_token, expiry.isoformat()))
                return cursor.lastrowid

    def refresh_token(self, account_data: dict) -> dict:
        """
        Refreshes access token using refresh_token.
        
        Args:
            account_data: dict containing at minimum 'refresh_token' and 'id'
            
        Returns:
            dict with new credentials
        """
        refresh_token = account_data.get('refresh_token')
        if not refresh_token:
            raise ValueError("No refresh token available. User must re-authenticate.")
        
        if not config.is_configured():
            raise ValueError("Google OAuth not configured.")
        
        # Call Google Token endpoint
        token_data = {
            'client_id': config.GOOGLE_CLIENT_ID,
            'client_secret': config.GOOGLE_CLIENT_SECRET,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }
        
        response = requests.post(
            config.GOOGLE_TOKEN_URI,
            data=token_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status_code != 200:
            error_data = response.json()
            raise Exception(f"Token refresh failed: {error_data.get('error_description', error_data.get('error', 'Unknown error'))}")
        
        tokens = response.json()
        new_access_token = tokens['access_token']
        expires_in = tokens.get('expires_in', 3600)
        
        # Update tokens in database
        expiry = datetime.now() + timedelta(seconds=expires_in)
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gmb_accounts 
                SET access_token = ?, token_expiry = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (new_access_token, expiry.isoformat(), account_data['id']))
        
        return {
            'token': new_access_token,
            'refresh_token': refresh_token,
            'token_uri': config.GOOGLE_TOKEN_URI,
            'client_id': config.GOOGLE_CLIENT_ID,
            'client_secret': config.GOOGLE_CLIENT_SECRET,
            'scopes': SCOPES
        }
    
    def list_accounts(self) -> list:
        """List all GMB accounts for this user."""
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, email, is_active, created_at, updated_at 
                FROM gmb_accounts 
                WHERE user_id = ?
                ORDER BY created_at DESC
            ''', (self.user_id,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
    
    def revoke_access(self, account_id: int) -> bool:
        """Revoke access for a specific account."""
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'SELECT access_token FROM gmb_accounts WHERE id = ? AND user_id = ?',
                (account_id, self.user_id)
            )
            row = cursor.fetchone()
            
            if not row:
                return False
            
            # Revoke token with Google
            try:
                requests.post(
                    config.GOOGLE_REVOKE_URI,
                    params={'token': row['access_token']},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                )
            except:
                pass  # Best effort revocation
            
            # Delete from database
            cursor.execute(
                'DELETE FROM gmb_accounts WHERE id = ? AND user_id = ?',
                (account_id, self.user_id)
            )
            
            return True
