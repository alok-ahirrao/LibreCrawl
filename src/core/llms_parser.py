
import requests
from urllib.parse import urlparse, urljoin
import logging

logger = logging.getLogger(__name__)

class LlmsTxtParser:
    """
    Parser for /llms.txt files.
    Standard proposal: https://llmstxt.org/
    """
    
    def __init__(self, session=None):
        self.session = session or requests.Session()
    
    def fetch_and_parse(self, base_url):
        """
        Fetch and parse llms.txt from the base URL.
        Returns a dict with 'content' and 'issues'.
        """
        try:
            parsed = urlparse(base_url)
            # llms.txt should be at the root
            llms_url = f"{parsed.scheme}://{parsed.netloc}/llms.txt"
            
            # Simple result structure
            result = {
                'url': llms_url,
                'content': None,
                'issues': [],
                'needs_generation': False
            }
            
            try:
                response = self.session.get(llms_url, timeout=10)
                
                if response.status_code == 200:
                    result['content'] = response.text
                    # Check for markdown content-type (optional warning)
                    bg_color = None
                    if 'text/plain' not in response.headers.get('Content-Type', '') and 'text/markdown' not in response.headers.get('Content-Type', ''):
                        result['issues'].append({
                            'line': 0,
                            'type': 'warning',
                            'message': f"Unexpected Content-Type: {response.headers.get('Content-Type')}. Should be text/plain or text/markdown."
                        })
                        # If it looks like HTML, we might need to generate one
                        if 'text/html' in response.headers.get('Content-Type', ''):
                             result['needs_generation'] = True
                        
                    # Basic Validation
                    validation_issues = self._validate_content(response.text)
                    result['issues'].extend(validation_issues)
                    
                else:
                    # Not found or error is common, just log it as an issue if it's not a 404
                    if response.status_code != 404:
                         result['issues'].append({
                            'line': 0,
                            'type': 'fetch_error',
                            'message': f"Failed to fetch llms.txt: Status {response.status_code}"
                        })
                    
                    if response.status_code == 404:
                        result['needs_generation'] = True
                    
            except Exception as e:
                result['issues'].append({
                    'line': 0,
                    'type': 'fetch_error',
                    'message': f"Error fetching llms.txt: {str(e)}"
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error in LlmsTxtParser: {e}")
            return {'content': None, 'issues': [{'line': 0, 'type': 'error', 'message': str(e)}]}

    def _validate_content(self, content):
        """
        Validate llms.txt content.
        Currently just checks closely for the H1 title as recommended by spec.
        """
        issues = []
        lines = content.split('\n')
        
        has_title = False
        
        for i, line in enumerate(lines):
            line_num = i + 1
            stripped = line.strip()
            
            if not stripped:
                continue
                
            # Spec recommendation: Should contain a Title
            if stripped.startswith('# '):
                has_title = True
                
            # TODO: Add more validation based on spec as it evolves
            # e.g. checking for 'Title:', 'Summary:', or link formats
            
        if not has_title:
            issues.append({
                'line': 1,
                'type': 'warning',
                'message': "File should start with an H1 title (e.g. '# Project Name')"
            })
            
        return issues
