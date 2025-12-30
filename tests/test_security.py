import sys
import os
import unittest
sys.path.append(os.getcwd())
from src.core.issue_detector import IssueDetector

class TestSecurityChecks(unittest.TestCase):
    def setUp(self):
        self.detector = IssueDetector()

    def test_mixed_content_detection(self):
        result = {
            'url': 'https://example.com',
            'status_code': 200,
            'images': [{'src': 'http://example.com/image.jpg'}], # Insecure image
            'links_data': [],
            'response_headers': {}
        }
        issues = []
        self.detector._check_security_issues(result, issues)
        
        mixed_content = next((i for i in issues if i['issue'] == 'Security: Mixed Content'), None)
        self.assertTrue(mixed_content)
        self.assertIn('http://example.com/image.jpg', mixed_content['details'])

    def test_missing_security_headers(self):
        result = {
            'url': 'https://example.com',
            'status_code': 200,
            'images': [],
            'links_data': [],
            'response_headers': {} # Empty headers
        }
        issues = []
        self.detector._check_security_issues(result, issues)
        
        has_csp = any(i['issue'] == 'Security: Missing Content-Security-Policy' for i in issues)
        has_hsts = any(i['issue'] == 'Security: Missing HSTS Header' for i in issues)
        has_xfo = any(i['issue'] == 'Security: Missing X-Frame-Options' for i in issues)
        
        self.assertTrue(has_csp)
        self.assertTrue(has_hsts)
        self.assertTrue(has_xfo)

    def test_clean_security_headers(self):
        result = {
            'url': 'https://example.com',
            'status_code': 200,
            'images': [],
            'links_data': [],
            'response_headers': {
                'Content-Security-Policy': 'default-src self',
                'Strict-Transport-Security': 'max-age=31536000',
                'X-Frame-Options': 'DENY'
            }
        }
        issues = []
        self.detector._check_security_issues(result, issues)
        
        has_csp = any(i['issue'] == 'Security: Missing Content-Security-Policy' for i in issues)
        has_hsts = any(i['issue'] == 'Security: Missing HSTS Header' for i in issues)
        has_xfo = any(i['issue'] == 'Security: Missing X-Frame-Options' for i in issues)
        
        self.assertFalse(has_csp)
        self.assertFalse(has_hsts)
        self.assertFalse(has_xfo)

if __name__ == '__main__':
    unittest.main()
