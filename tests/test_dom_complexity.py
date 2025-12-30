import sys
import os
import unittest
from bs4 import BeautifulSoup

sys.path.append(os.getcwd())
try:
    from src.core.seo_extractor import SEOExtractor
except ImportError:
    # Use direct import if src module structure is different in test env
    sys.path.append(os.path.join(os.getcwd(), 'src'))
    from core.seo_extractor import SEOExtractor

class TestDOMComplexity(unittest.TestCase):
    def test_dom_complexity_extraction(self):
        html_content = """
        <html>
            <head>
                <title>Test Page</title>
            </head>
            <body>
                <div id="container">
                    <h1>Header</h1>
                    <ul>
                        <li>Item 1</li>
                        <li>Item 2</li>
                    </ul>
                    <div>
                        <p>Deep <span>Nested <b>Content</b></span></p>
                    </div>
                </div>
            </body>
        </html>
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        result = {'meta_tags': {}, 'og_tags': {}, 'twitter_tags': {}, 'json_ld': [], 'analytics': {}, 'images': [], 'headings_structure': [], 'links_data': []}
        
        # SEOExtractor modifies the result dictionary in place
        SEOExtractor.extract_basic_seo_data(soup, result)
        
        # Verify DOM Size (Node Count)
        # html, head, title, body, div#container, h1, ul, li, li, div, p, span, b -> ~13 tags depending on parser details
        self.assertGreater(result['dom_size'], 10) 
        
        # Verify DOM Depth
        # html -> body -> div -> div -> p -> span -> b = 7 levels deep
        self.assertGreaterEqual(result['dom_depth'], 6)

if __name__ == '__main__':
    unittest.main()
