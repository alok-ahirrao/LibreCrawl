"""SEO issue detection and reporting"""
import threading
import re
from fnmatch import fnmatch
from urllib.parse import urlparse
from difflib import SequenceMatcher


class IssueDetector:
    """Detects SEO and technical issues in crawled pages"""

    def __init__(self, exclusion_patterns=None):
        self.exclusion_patterns = exclusion_patterns or []
        self.detected_issues = []
        self.issues_lock = threading.Lock()
        # Track site-wide issues that only need to be reported once
        self.reported_sitewide_issues = set()  # Set of (domain, issue_type) tuples

    def detect_issues(self, result):
        """Detect SEO issues for a crawled URL"""
        url = result.get('url', '')
        issues = []

        # Skip if URL matches exclusion patterns
        if self._should_exclude(url):
            return

        # Check for connection failure (Status 0)
        status_code = result.get('status_code', 0)
        if status_code == 0:
            with self.issues_lock:
                self.detected_issues.append({
                    'url': url,
                    'type': 'error',
                    'category': 'Technical',
                    'issue': 'Connection Failed',
                    'details': result.get('error', 'Failed to connect to server or request blocked')
                })
            return

        # Critical SEO Issues
        self._check_title_issues(result, issues)
        self._check_meta_description_issues(result, issues)
        self._check_heading_issues(result, issues)
        self._check_content_issues(result, issues)
        self._check_technical_issues(result, issues)
        self._check_mobile_issues(result, issues)
        self._check_accessibility_issues(result, issues)
        self._check_social_media_issues(result, issues)
        self._check_structured_data_issues(result, issues)
        self._check_performance_issues(result, issues)
        self._check_indexability_issues(result, issues)
        self._check_url_issues(result, issues)
        self._check_link_issues(result, issues)
        self._check_security_issues(result, issues)

        # Add all detected issues
        with self.issues_lock:
            self.detected_issues.extend(issues)

    def _normalize_url_for_comparison(self, url):
        """
        Normalize URL for comparison purposes.
        Handles trailing slashes, case sensitivity, and common variations.
        """
        if not url:
            return ''
        try:
            # Parse the URL
            parsed = urlparse(url.lower())
            
            # Normalize path - remove trailing slash (except for root)
            path = parsed.path.rstrip('/')
            if not path:
                path = ''  # Root path
            
            # Rebuild normalized URL (scheme://host/path)
            normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
            
            # Add query string if present (but not fragment)
            if parsed.query:
                normalized += f"?{parsed.query}"
            
            return normalized
        except:
            return url.lower().rstrip('/')

    def _check_title_issues(self, result, issues):
        """Check for title-related issues"""
        url = result.get('url', '')
        title = result.get('title', '')
        
        # Classify page type to determine severity
        page_type = self._classify_page_type(url)
        is_archive_or_utility = page_type == 'archive'
        
        # Also check for utility pages
        parsed_path = urlparse(url).path.lower()
        utility_patterns = [
            '/thank-you', '/thankyou', '/confirmation',
            '/privacy-policy', '/privacy', '/terms', '/legal',
            '/cookie-policy', '/gdpr', '/dmca',
            '/login', '/register', '/signup', '/account',
            '/cart', '/checkout', '/wishlist',
            '/search', '/404', '/error',
        ]
        if any(p in parsed_path for p in utility_patterns):
            is_archive_or_utility = True

        if not title:
            # Archive/utility: downgrade to 'warning' (still needs attention, but lower priority)
            # Important/other: keep as 'error' (critical SEO issue)
            severity = 'warning' if is_archive_or_utility else 'error'
            issue_title = 'Missing Title Tag (Archive/Utility)' if severity == 'warning' else 'Missing Title Tag'
            
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': issue_title,
                'details': f"Page has no title tag{' (archive/utility page - lower priority)' if is_archive_or_utility else ''}"
            })
        else:
            if len(title) > 60:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'Page Titles: Over 60 Characters',
                    'details': f"Title is {len(title)} characters"
                })
            
            # Approximate pixel width (assuming ~9px per char on average)
            pixel_width = len(title) * 9
            if pixel_width > 561:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'Page Titles: Over 561 Pixels',
                    'details': f"Title is approx {pixel_width} pixels"
                })

            if len(title) < 30:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'Title Too Short',
                    'details': f"Title is {len(title)} characters (recommended: 30-60)"
                })

    def _check_meta_description_issues(self, result, issues):
        """Check for meta description issues"""
        url = result.get('url', '')
        meta_desc = result.get('meta_description', '')
        
        # Classify page type to determine severity
        page_type = self._classify_page_type(url)

        if not meta_desc:
            # Archive/utility pages: downgrade to 'info' (expected behavior, low priority)
            # Important/other pages: keep as 'warning' (should be fixed for CTR)
            severity = 'info' if page_type == 'archive' else 'warning'
            
            # Also check for utility pages not covered by _classify_page_type
            parsed_path = urlparse(url).path.lower()
            utility_patterns = [
                '/thank-you', '/thankyou', '/confirmation',
                '/privacy-policy', '/privacy', '/terms', '/legal',
                '/cookie-policy', '/gdpr', '/dmca',
                '/login', '/register', '/signup', '/account',
                '/cart', '/checkout', '/wishlist',
                '/search', '/404', '/error',
            ]
            if any(p in parsed_path for p in utility_patterns):
                severity = 'info'
            
            issue_title = 'Meta Description: Missing (Archive/Utility)' if severity == 'info' else 'Meta Description: Missing'
            
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': issue_title,
                'details': f"Page has no meta description{' (archive/utility page - low priority)' if severity == 'info' else ''}"
            })
        else:
            if len(meta_desc) > 155:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'Meta Description: Over 155 Characters',
                    'details': f"Description is {len(meta_desc)} characters"
                })
            
            # Approximate pixel width
            pixel_width = len(meta_desc) * 9
            if pixel_width > 985:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'Meta Description: Over 985 Pixels',
                    'details': f"Description is approx {pixel_width} pixels"
                })

    def _check_heading_issues(self, result, issues):
        """Check for heading-related issues"""
        url = result.get('url', '')
        h1 = result.get('h1', '')
        h1_list = result.get('h1_list', [])
        headings = result.get('headings_structure', [])
        
        # Classify page type to determine severity
        page_type = self._classify_page_type(url)
        is_archive_or_utility = page_type == 'archive'
        
        # Check for utility pages
        parsed_path = urlparse(url).path.lower()
        utility_patterns = [
            '/thank-you', '/thankyou', '/confirmation',
            '/privacy-policy', '/privacy', '/terms', '/legal',
            '/cookie-policy', '/gdpr', '/dmca',
            '/login', '/register', '/signup', '/account',
            '/cart', '/checkout', '/wishlist',
            '/search', '/404', '/error',
        ]
        if any(p in parsed_path for p in utility_patterns):
            is_archive_or_utility = True

        if not h1 and not h1_list:
            # Archives often lack a formal H1 or use a generic one.
            severity = 'warning' if is_archive_or_utility else 'error'
            issue_title = 'Missing H1 Tag (Archive/Utility)' if severity == 'warning' else 'Missing H1 Tag'
            
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': issue_title,
                'details': f"Page has no H1 heading{' (archive/utility - lower priority)' if severity == 'warning' else ''}"
            })
        elif len(h1_list) > 1:
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'SEO',
                'issue': 'H1: Multiple',
                'details': f'Page has {len(h1_list)} H1 tags'
            })
            
            # Check for duplicate H1s
            if len(set(h1_list)) != len(h1_list):
                 issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'SEO',
                    'issue': 'H1: Duplicate',
                    'details': 'Page has duplicate H1 tags'
                })

        if h1 and len(h1) > 70:
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'SEO',
                'issue': 'H1: Over 70 Characters',
                'details': f"H1 is {len(h1)} characters"
            })

        # Check Heading Structure (Sequential)
        last_level = 0
        h2_count = 0
        h2_texts = []
        
        for i, h in enumerate(headings):
            level = h['level']
            text = h['text']
            
            # Check Non-Sequential (skipping levels, e.g. H1 -> H3)
            # Allowed: level <= last_level + 1
            
            if level > last_level + 1:
                # Exception: last_level 0 (start) can go to H1.
                if last_level == 0 and level != 1:
                     # FIRST heading is not H1 (e.g. H2 appears before H1)
                     severity = 'info' if is_archive_or_utility else 'warning'
                     issue_title = f'H{level} appears before H1 (Archive/Utility)' if severity == 'info' else f'H{level} appears before H1'
                     
                     issues.append({
                        'url': url,
                        'type': severity,
                        'category': 'SEO',
                        'issue': issue_title,
                        'details': f'The first heading is an H{level}, should be H1.'
                    })
                elif last_level > 0:
                     issues.append({
                        'url': url,
                        'type': 'warning',
                        'category': 'SEO',
                        'issue': f'H{level}: Non-Sequential',
                        'details': f'Heading structure skips from H{last_level} to H{level}'
                    })
            
            last_level = level
            
            if level == 2:
                h2_count += 1
                h2_texts.append(text)

        if h2_count > 1 and len(set(h2_texts)) != len(h2_texts):
             # Find which texts are duplicated
             from collections import Counter
             duplicates = [item for item, count in Counter(h2_texts).items() if count > 1]
             
             # Boilerplate headings that are common in themes/templates
             # If the only duplicates are these, it's a structural false positive
             common_boilerplate = {
                 'leave a reply', 'comments', 'recent posts', 'related posts', 
                 'share this post', 'navigate', 'navigation', 'menu', 
                 'sidebar', 'footer', 'search', 'overview', 'description', 
                 'reviews', 'categories', 'archives', 'tags', 'meta'
             }
             
             non_boilerplate_dupes = [d for d in duplicates if d.lower().strip() not in common_boilerplate]
             
             if non_boilerplate_dupes:
                 # Real content duplication issues
                 severity = 'info' if is_archive_or_utility else 'warning'
                 issue_title = 'H2: Duplicate (Archive/Utility)' if severity == 'info' else 'H2: Duplicate'

                 issues.append({
                    'url': url,
                    'type': severity,
                    'category': 'SEO',
                    'issue': issue_title,
                    'details': f"Page has duplicate H2 tags: {', '.join(non_boilerplate_dupes)}{' (archive/utility - low priority)' if severity == 'info' else ''}"
                })
             elif duplicates:
                 # Only boilerplate duplicates found (e.g. 'Leave a Reply' twice)
                 # Treat as INFO regardless of page type
                 issues.append({
                    'url': url,
                    'type': 'info',
                    'category': 'SEO',
                    'issue': 'H2: Duplicate (Boilerplate)',
                    'details': f"Duplicate template headings found: {', '.join(duplicates)}"
                })

    def _check_content_issues(self, result, issues):
        """Check for content-related issues"""
        url = result.get('url', '')
        word_count = result.get('word_count', 0)

        if word_count < 300:
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Content',
                'issue': 'Thin Content',
                'details': f'Page has only {word_count} words (recommended: ≥300)'
            })

    def _check_technical_issues(self, result, issues):
        """Check for technical SEO issues"""
        url = result.get('url', '')
        status_code = result.get('status_code', 0)

        if status_code >= 400 and status_code < 500:
            issues.append({
                'url': url,
                'type': 'error',
                'category': 'Technical',
                'issue': 'Response Codes: External Client Error (4xx)',
                'details': self._get_status_code_message(status_code)
            })
        elif status_code >= 500:
            issues.append({
                'url': url,
                'type': 'error',
                'category': 'Technical',
                'issue': 'Response Codes: External Server Error (5xx)',
                'details': self._get_status_code_message(status_code)
            })
        elif status_code >= 300 and status_code < 400:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Technical',
                'issue': 'Response Codes: Internal Redirection (3xx)',
                'details': 'URL redirects to another location'
            })
        elif status_code == 0:
             issues.append({
                'url': url,
                'type': 'error',
                'category': 'Technical',
                'issue': 'Response Codes: External No Response',
                'details': 'Server did not respond'
            })

        # Soft 404 Detection: 200 OK but appears to be an error page
        if status_code == 200:
            title = result.get('title', '').lower()
            h1 = result.get('h1', '').lower()
            word_count = result.get('word_count', 0)
            page_size = result.get('size', 0)
            
            # Error patterns to detect soft 404s
            error_patterns = [
                'not found', '404', 'page not found', 'error 404',
                'page doesn\'t exist', 'page does not exist',
                'no longer available', 'has been removed',
                'could not be found', 'cannot be found',
                'doesn\'t exist', 'does not exist',
                'oops', 'sorry', 'nothing here'
            ]
            
            # Check if title or H1 contains error patterns
            is_error_title = any(pattern in title for pattern in error_patterns)
            is_error_h1 = any(pattern in h1 for pattern in error_patterns)
            
            # Very thin content (less than 100 words AND small page size) can indicate soft 404
            is_thin_error_page = word_count < 100 and page_size < 5000 and (is_error_title or is_error_h1)
            
            # Flag as soft 404 if clear error signals are present
            if is_error_title or is_error_h1:
                # Determine severity based on signals
                if is_error_title and is_error_h1:
                    severity = 'error'
                    details = f'Title: "{result.get("title", "")[:50]}" and H1: "{result.get("h1", "")[:50]}" suggest error page'
                elif is_error_title:
                    severity = 'warning'
                    details = f'Title "{result.get("title", "")[:60]}" suggests this is an error page'
                else:
                    severity = 'warning'
                    details = f'H1 "{result.get("h1", "")[:60]}" suggests this is an error page'
                
                issues.append({
                    'url': url,
                    'type': severity,
                    'category': 'Technical',
                    'issue': 'Soft 404: Returns 200 but appears broken',
                    'details': details
                })
                
                # Mark the result for frontend badge display
                result['is_soft_404'] = True

        # [NEW] Redirect Chain Detection
        redirect_chain = result.get('redirect_chain', [])
        redirect_count = result.get('redirect_count', 0) or len(redirect_chain) - 1 if redirect_chain else 0
        
        if redirect_count > 0:
            # Check for redirect loops (same URL appearing multiple times)
            chain_urls = [r.get('url', '') for r in redirect_chain]
            seen_urls = set()
            has_loop = False
            loop_url = None
            for chain_url in chain_urls:
                normalized = self._normalize_url_for_comparison(chain_url)
                if normalized in seen_urls:
                    has_loop = True
                    loop_url = chain_url
                    break
                seen_urls.add(normalized)
            
            if has_loop:
                # Redirect Loop - Critical Error
                issues.append({
                    'url': url,
                    'type': 'error',
                    'category': 'Technical',
                    'issue': 'Redirect Loop Detected',
                    'details': f'URL redirects back to itself: {loop_url}',
                    'redirect_chain': redirect_chain
                })
                result['has_redirect_loop'] = True
            elif redirect_count > 3:
                # Long redirect chain (> 3 hops) - Warning
                chain_summary = ' → '.join([f"{r.get('status_code', '?')}" for r in redirect_chain])
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'Technical',
                    'issue': 'Long Redirect Chain',
                    'details': f'{redirect_count} redirects before final destination. Chain: {chain_summary}',
                    'redirect_chain': redirect_chain
                })
                result['has_long_redirect_chain'] = True
            elif redirect_count > 1:
                # Multi-hop redirect (2-3 hops) - Info
                chain_summary = ' → '.join([f"{r.get('status_code', '?')}" for r in redirect_chain])
                issues.append({
                    'url': url,
                    'type': 'info',
                    'category': 'Technical',
                    'issue': 'Redirect Chain',
                    'details': f'{redirect_count} redirects: {chain_summary}',
                    'redirect_chain': redirect_chain
                })
            # Single redirect (1 hop) is normal, no issue needed

        # Canonical URL checks
        canonical_url = result.get('canonical_url', '')
        
        # Check indexability to filter false positives
        robots_meta = result.get('robots', '').lower()
        x_robots_tag = result.get('x_robots_tag', '').lower()
        is_noindex = 'noindex' in robots_meta or 'noindex' in x_robots_tag

        if not canonical_url:
            if is_noindex:
                 # False Positive: NoIndex pages don't strictly need a canonical (Google ignores them anyway)
                 pass
            else:
                # Real Issue: Indexable page with no canonical
                page_type = self._classify_page_type(url)
                
                if page_type == 'important':
                    issues.append({
                        'url': url,
                        'type': 'error', # Critical for posts/pages
                        'category': 'Technical',
                        'issue': 'Missing Canonical URL',
                        'details': 'Indexable content page has no canonical URL'
                    })
                elif page_type == 'archive':
                     issues.append({
                        'url': url,
                        'type': 'warning', # Medium for archives
                        'category': 'Technical',
                        'issue': 'Missing Canonical URL',
                        'details': 'Archive page missing canonical (Review if this should be indexed)'
                    })
                else:
                    issues.append({
                        'url': url,
                        'type': 'error', # Default to high for unknown pages to be safe
                        'category': 'Technical',
                        'issue': 'Missing Canonical URL',
                        'details': 'Page has no canonical URL specified'
                    })
        else:
            # Normalize URLs before comparing (handle trailing slashes, case, etc.)
            normalized_url = self._normalize_url_for_comparison(url)
            normalized_canonical = self._normalize_url_for_comparison(canonical_url)
            
            if normalized_canonical != normalized_url:
                # Analyze the mismatch severity
                severity = 'warning'
                issue_label = 'Canonicals: Canonicalised'
                details = f"Page is canonicalised to: {canonical_url}"
                
                try:
                    p_url = urlparse(url)
                    p_can = urlparse(canonical_url)
                    
                    # check for root/homepage redirect (Soft 404 risk)
                    if p_can.path in ['', '/'] and p_url.path not in ['', '/']:
                         severity = 'error'
                         details = "Critical: Content page canonicalises to Homepage (Soft 404 risk)"
                    
                    # check for __trashed
                    elif '__trashed' in canonical_url:
                        severity = 'error'
                        details = "Critical: Canonical points to a trashed post URL"
                        
                    # check if only query params or hash differ (Safe/optimization)
                    elif p_url.scheme == p_can.scheme and p_url.netloc == p_can.netloc and p_url.path == p_can.path:
                        severity = 'info'
                        details = "Safe: Canonical removes query parameters or fragments"
                        
                    # check if only slash/scheme differ (Safe/normalization)
                    elif p_url.netloc == p_can.netloc and p_url.path.strip('/') == p_can.path.strip('/'):
                         severity = 'info'
                         details = "Safe: Canonical normalizes slash or protocol"

                except:
                    pass

                issues.append({
                    'url': url,
                    'type': severity,
                    'category': 'Technical',
                    'issue': issue_label,
                    'details': details
                })
            
            # Check if canonical is non-indexable (simplified check if self-ref or valid)
            # If canonical is pointing to a different domain or path, we can't easily check indexability without crawling it.
            # But we can flag if it looks suspicious.

    def _check_mobile_issues(self, result, issues):
        """Check for mobile optimization issues"""
        url = result.get('url', '')

        if not result.get('viewport'):
            issues.append({
                'url': url,
                'type': 'error',
                'category': 'Mobile',
                'issue': 'Missing Viewport Meta Tag',
                'details': 'Page is not mobile-optimized'
            })

    def _check_accessibility_issues(self, result, issues):
        """Check for accessibility and image issues"""
        url = result.get('url', '')

        if not result.get('lang'):
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Accessibility',
                'issue': 'Missing Language Attribute',
                'details': 'Template issue: HTML tag missing lang attribute (accessibility best practice)'
            })

        # Image issues
        images = result.get('images', [])
        
        missing_alt_count = 0
        missing_size_count = 0
        
        for img in images:
            if not img.get('alt'):
                missing_alt_count += 1
            if not img.get('width') or not img.get('height'):
                missing_size_count += 1
                
        if missing_alt_count > 0:
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Images',
                'issue': 'Images: Missing Alt Text',
                'details': f'{missing_alt_count} images lack alt text'
            })
            
        if missing_size_count > 0:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Images',
                'issue': 'Images: Missing Size Attributes',
                'details': f'{missing_size_count} images lack width/height attributes'
            })

    def _check_social_media_issues(self, result, issues):
        """Check for social media optimization issues"""
        url = result.get('url', '')

        if not result.get('og_tags'):
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Social',
                'issue': 'Missing OpenGraph Tags',
                'details': 'Page has no OpenGraph tags for social sharing'
            })

        if not result.get('twitter_tags'):
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Social',
                'issue': 'Missing Twitter Card Tags',
                'details': 'Page has no Twitter Card tags'
            })

    def _check_structured_data_issues(self, result, issues):
        """Check for structured data issues and AI readiness"""
        url = result.get('url', '')
        json_ld_data = result.get('json_ld', [])
        schema_org_data = result.get('schema_org', [])
        
        # AI-ready schema types that enable rich results
        ai_ready_types = {
            'FAQPage': {'name': 'FAQ', 'rich_result': 'FAQ Accordion'},
            'HowTo': {'name': 'How-To', 'rich_result': 'Step-by-step Guide'},
            'Recipe': {'name': 'Recipe', 'rich_result': 'Recipe Card'},
            'Product': {'name': 'Product', 'rich_result': 'Product Snippet'},
            'Review': {'name': 'Review', 'rich_result': 'Review Stars'},
            'AggregateRating': {'name': 'Rating', 'rich_result': 'Star Rating'},
            'LocalBusiness': {'name': 'Local Business', 'rich_result': 'Knowledge Panel'},
            'Organization': {'name': 'Organization', 'rich_result': 'Knowledge Panel'},
            'Person': {'name': 'Person', 'rich_result': 'Knowledge Panel'},
            'Article': {'name': 'Article', 'rich_result': 'Article Preview'},
            'NewsArticle': {'name': 'News Article', 'rich_result': 'News Carousel'},
            'BlogPosting': {'name': 'Blog Post', 'rich_result': 'Article Preview'},
            'Event': {'name': 'Event', 'rich_result': 'Event Listing'},
            'JobPosting': {'name': 'Job Posting', 'rich_result': 'Job Listing'},
            'Course': {'name': 'Course', 'rich_result': 'Course Card'},
            'SoftwareApplication': {'name': 'Software App', 'rich_result': 'App Info'},
            'VideoObject': {'name': 'Video', 'rich_result': 'Video Preview'},
            'BreadcrumbList': {'name': 'Breadcrumbs', 'rich_result': 'Breadcrumb Trail'},
        }
        
        if not json_ld_data and not schema_org_data:
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Structured Data',
                'issue': 'No Structured Data',
                'details': 'Page has no JSON-LD or Schema.org markup'
            })
            return
        
        # Extract and validate schema types
        detected_types = set()
        schema_analysis = {
            'types': [],
            'faq_items': [],
            'has_organization': False,
            'has_website': False,
            'has_breadcrumbs': False,
            'issues': []
        }
        
        def extract_type(schema_obj, depth=0):
            """Recursively extract @type from schema object"""
            if depth > 10:  # Prevent infinite recursion
                return
            
            if isinstance(schema_obj, dict):
                schema_type = schema_obj.get('@type')
                if schema_type:
                    if isinstance(schema_type, list):
                        for t in schema_type:
                            detected_types.add(t)
                    else:
                        detected_types.add(schema_type)
                    
                    # Extract FAQ items
                    if schema_type == 'FAQPage' or (isinstance(schema_type, list) and 'FAQPage' in schema_type):
                        main_entity = schema_obj.get('mainEntity', [])
                        if isinstance(main_entity, list):
                            for item in main_entity:
                                if item.get('@type') == 'Question':
                                    q = item.get('name', '')
                                    a_obj = item.get('acceptedAnswer', {})
                                    a = a_obj.get('text', '') if isinstance(a_obj, dict) else ''
                                    if q:
                                        schema_analysis['faq_items'].append({'question': q, 'answer': a[:200] + '...' if len(a) > 200 else a})
                    
                    # Check for organization
                    if schema_type in ['Organization', 'LocalBusiness', 'Corporation']:
                        schema_analysis['has_organization'] = True
                    
                    # Check for website
                    if schema_type == 'WebSite':
                        schema_analysis['has_website'] = True
                    
                    # Check for breadcrumbs
                    if schema_type == 'BreadcrumbList':
                        schema_analysis['has_breadcrumbs'] = True
                
                # Recurse into nested objects
                for key, value in schema_obj.items():
                    if isinstance(value, (dict, list)):
                        extract_type(value, depth + 1)
            
            elif isinstance(schema_obj, list):
                for item in schema_obj:
                    extract_type(item, depth + 1)
        
        # Process JSON-LD
        for schema in json_ld_data:
            extract_type(schema)
        
        # Process Schema.org microdata
        for schema in schema_org_data:
            schema_type = schema.get('type', '')
            if schema_type:
                # Extract type from full URL like "https://schema.org/Article"
                if '/' in schema_type:
                    schema_type = schema_type.split('/')[-1]
                detected_types.add(schema_type)
        
        # Store detected types in result for frontend
        result['schema_types'] = list(detected_types)
        result['schema_analysis'] = schema_analysis
        schema_analysis['types'] = list(detected_types)
        
        # Check for AI-ready schemas
        ai_ready_found = []
        for schema_type in detected_types:
            if schema_type in ai_ready_types:
                ai_ready_found.append(ai_ready_types[schema_type])
        
        result['ai_ready_schemas'] = ai_ready_found
        
        # Validation checks
        # Check 1: Has basic organizational schema (recommended for all sites)
        page_type = self._classify_page_type(url)
        
        if page_type == 'important' and not schema_analysis['has_organization'] and not schema_analysis['has_website']:
            if 'Article' not in detected_types and 'BlogPosting' not in detected_types and 'Product' not in detected_types:
                issues.append({
                    'url': url,
                    'type': 'info',
                    'category': 'Structured Data',
                    'issue': 'Schema: Missing Organization/WebSite',
                    'details': 'Consider adding Organization or WebSite schema for brand visibility'
                })
        
        # Check 2: FAQ validation
        if 'FAQPage' in detected_types:
            if len(schema_analysis['faq_items']) == 0:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'Structured Data',
                    'issue': 'Schema: FAQPage has no questions',
                    'details': 'FAQPage schema found but no Question items detected'
                })
            elif len(schema_analysis['faq_items']) < 3:
                issues.append({
                    'url': url,
                    'type': 'info',
                    'category': 'Structured Data',
                    'issue': 'Schema: FAQPage has few questions',
                    'details': f'Only {len(schema_analysis["faq_items"])} FAQ items found (3+ recommended)'
                })
        
        # Check 3: Article/BlogPosting validation
        if 'Article' in detected_types or 'BlogPosting' in detected_types or 'NewsArticle' in detected_types:
            # These should have author, datePublished, headline
            has_required = False
            for schema in json_ld_data:
                if schema.get('@type') in ['Article', 'BlogPosting', 'NewsArticle']:
                    if schema.get('headline') and schema.get('datePublished'):
                        has_required = True
                        break
            
            if not has_required:
                issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'Structured Data',
                    'issue': 'Schema: Article missing required fields',
                    'details': 'Article schema should have headline and datePublished'
                })

    def _check_performance_issues(self, result, issues):
        """Check for performance issues"""
        url = result.get('url', '')
        response_time = result.get('response_time', 0)
        page_size = result.get('size', 0)

        if response_time > 3000:
            issues.append({
                'url': url,
                'type': 'error',
                'category': 'Performance',
                'issue': 'Slow Response Time',
                'details': f'Page took {response_time}ms to respond (recommended: <3000ms)'
            })

        if page_size > 3 * 1024 * 1024:
            issues.append({
                'url': url,
                'type': 'error',
                'category': 'Performance',
                'issue': 'Large Page Size',
                'details': f'Page size is {page_size / 1024 / 1024:.1f}MB (recommended: <3MB)'
            })

    def _check_indexability_issues(self, result, issues):
        """Check for indexability issues"""
        url = result.get('url', '')
        robots_meta = result.get('robots', '').lower()
        x_robots_tag = result.get('x_robots_tag', '').lower()

        # Check Noindex
        meta_noindex = 'noindex' in robots_meta
        header_noindex = 'noindex' in x_robots_tag
        
        if meta_noindex or header_noindex:
            sources = []
            if meta_noindex: sources.append("HTML Meta Tag")
            if header_noindex: sources.append("HTTP Header (X-Robots-Tag)")
            
            source_str = " & ".join(sources)
            
            # Determine severity based on page type
            page_type = self._classify_page_type(url)
            
            if page_type == 'archive':
                # Archive pages with noindex = expected, low severity
                issue_type = 'info'
                details = f'Source: {source_str} (Expected for archive page)'
            elif page_type == 'important':
                # Important pages with noindex = problem, high severity
                issue_type = 'error'
                details = f'Source: {source_str} (Critical: Important page is blocked!)'
            else:
                # Unknown/other pages = warning
                issue_type = 'warning'
                details = f'Source: {source_str}'
            
            issues.append({
                'url': url,
                'type': issue_type,
                'category': 'Indexability',
                'issue': 'Directives: Noindex',
                'details': details
            })

        # Check Nofollow
        meta_nofollow = 'nofollow' in robots_meta
        header_nofollow = 'nofollow' in x_robots_tag
        
        if meta_nofollow or header_nofollow:
            sources = []
            if meta_nofollow: sources.append("HTML Meta Tag")
            if header_nofollow: sources.append("HTTP Header (X-Robots-Tag)")
            
            source_str = " & ".join(sources)
            
            issues.append({
                'url': url,
                'type': 'warning',  # Nofollow is less severe than noindex
                'category': 'Indexability',
                'issue': 'Directives: Nofollow',
                'details': f'Source: {source_str}'
            })

    def _classify_page_type(self, url):
        """
        Classify page type for severity determination.
        Returns: 'archive', 'important', or 'other'
        """
        if not url:
            return 'other'
        
        try:
            parsed = urlparse(url)
            path = parsed.path.lower()
            
            # ===== ARCHIVE PATTERNS (noindex is EXPECTED) =====
            archive_patterns = [
                # Date archives: /2024/, /2024/01/, /2024/01/15/
                r'/\d{4}/$',           # Year archive
                r'/\d{4}/\d{2}/$',     # Month archive
                r'/\d{4}/\d{2}/\d{2}/$',  # Day archive (but NOT blog posts)
                
                # WordPress taxonomy archives
                '/author/',
                '/tag/',
                '/category/',
                '/tags/',
                '/categories/',
                
                # Pagination
                '/page/',
                '/feed/',
                
                # Search results
                '/search/',
                '/?s=',
                
                # WordPress attachment pages
                '/attachment/',
                
                # Archive paths
                '/archive/',
                '/archives/',
            ]
            
            for pattern in archive_patterns:
                if pattern.startswith('r/'):
                    # Regex pattern - remove 'r' prefix
                    continue
                elif pattern in path or pattern in url:
                    return 'archive'
            
            # Regex patterns for date archives
            import re
            date_pattern = re.compile(r'/\d{4}/(\d{2}/)?(\d{2}/)?$')
            if date_pattern.search(path):
                return 'archive'
            
            # Check for pagination in query string
            if 'page=' in url or 'paged=' in url:
                return 'archive'
            
            # ===== IMPORTANT PATTERNS (noindex is a PROBLEM) =====
            important_patterns = [
                # Homepage
                lambda p: p == '/' or p == '',
                
                # Service pages
                lambda p: '/service' in p,
                lambda p: '/services' in p,
                
                # Location pages
                lambda p: '/location' in p,
                lambda p: '/locations' in p,
                lambda p: '-dentist' in p,  # e.g., /dentists-billerica/
                
                # Contact/About
                lambda p: '/contact' in p,
                lambda p: '/about' in p,
                
                # Blog posts (have slug, not just archive path)
                # If path has multiple segments and doesn't match archive patterns, 
                # it's likely a content page
            ]
            
            for check in important_patterns:
                if callable(check) and check(path):
                    return 'important'
            
            # Blog posts detection: has content after date path
            # e.g., /2024/01/15/my-blog-post/ = important
            # vs    /2024/01/ = archive
            date_post_pattern = re.compile(r'/\d{4}/\d{2}/\d{2}/[a-z0-9-]+')
            if date_post_pattern.search(path):
                return 'important'
            
            # Single slug paths are likely important content pages
            segments = [s for s in path.split('/') if s]
            if len(segments) == 1 and len(segments[0]) > 3:
                return 'important'
            
            return 'other'
            
        except:
            return 'other'


    def _check_url_issues(self, result, issues):
        """Check for URL structure issues"""
        url = result.get('url', '')
        parsed = urlparse(url)
        path = parsed.path
        
        if len(url) > 115:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'URL',
                'issue': 'URL: Over 115 Characters',
                'details': f'URL is {len(url)} characters long'
            })
            
        if '_' in path:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'URL',
                'issue': 'URL: Underscores',
                'details': 'URL contains underscores (use hyphens instead)'
            })
            
        if '?' in url:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'URL',
                'issue': 'URL: Parameters',
                'details': 'URL contains query parameters'
            })
            
        # Check repetitive path (e.g. /foo/foo/bar)
        path_parts = [p for p in path.split('/') if p]
        if len(path_parts) != len(set(path_parts)):
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'URL',
                'issue': 'URL: Repetitive Path',
                'details': 'URL path contains duplicate segments'
            })

    def _check_link_issues(self, result, issues):
        """Check for link-related issues"""
        url = result.get('url', '')
        links_data = result.get('links_data', [])
        external_links_count = result.get('external_links', 0)
        
        if external_links_count > 50: # Arbitrary threshold for "High External Outlinks"
             issues.append({
                'url': url,
                'type': 'info',
                'category': 'Links',
                'issue': 'Links: Pages With High External Outlinks',
                'details': f'Page has {external_links_count} external links'
            })
            
        internal_nofollow_count = 0
        empty_anchor_count = 0
        non_descriptive_anchors = 0
        unsafe_cross_origin = 0
        
        generic_anchors = ['click here', 'read more', 'more', 'here', 'link', 'this', 'go']
        
        for link in links_data:
            text = link.get('text', '').lower().strip()
            href = link.get('href', '')
            rel = link.get('rel', [])
            if isinstance(rel, str): rel = [rel]
            target = link.get('target', '')
            is_internal = link.get('is_internal', False)
            
            if is_internal and 'nofollow' in rel:
                internal_nofollow_count += 1
                
            if not text and not link.get('aria-label'): # Check for empty anchor
                # Check if it wraps an image with alt text? 
                # Simplistic check for now.
                empty_anchor_count += 1
                
            if is_internal and text in generic_anchors:
                non_descriptive_anchors += 1
                
            # Security: Unsafe Cross-Origin
            if target == '_blank':
                if not is_internal and 'noopener' not in rel and 'noreferrer' not in rel:
                     unsafe_cross_origin += 1

        if internal_nofollow_count > 0:
            issues.append({
                'url': url,
                'type': 'info',
                'category': 'Links',
                'issue': 'Links: Internal Nofollow Outlinks',
                'details': f'{internal_nofollow_count} internal links are marked nofollow'
            })

        if empty_anchor_count > 0:
             issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Links',
                'issue': 'Links: Internal Outlinks With No Anchor Text',
                'details': f'{empty_anchor_count} links have no anchor text'
            })

        if non_descriptive_anchors > 0:
             issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Links',
                'issue': 'Links: Non-Descriptive Anchor Text',
                'details': f'{non_descriptive_anchors} links use generic text like "click here"'
            })

        if unsafe_cross_origin > 0:
            # Low severity security best practice, not SEO ranking error
            # Track unique external domains to avoid inflation from shared headers/footers
            unsafe_domains = set()
            for link in links_data:
                target = link.get('target', '')
                rel = link.get('rel', [])
                if isinstance(rel, str): rel = [rel]
                is_internal = link.get('is_internal', False)
                href = link.get('href', '')
                
                if target == '_blank' and not is_internal and 'noopener' not in rel and 'noreferrer' not in rel:
                    try:
                        domain = urlparse(href).netloc
                        if domain:
                            unsafe_domains.add(domain)
                    except:
                        pass
            
            # Only report unique domains that haven't been reported site-wide
            for domain in unsafe_domains:
                issue_key = (domain, 'unsafe_cross_origin')
                if issue_key not in self.reported_sitewide_issues:
                    self.reported_sitewide_issues.add(issue_key)
                    issues.append({
                        'url': url,  # First page where this domain was found
                        'type': 'info',  # Low severity - best practice, not SEO error
                        'category': 'Security',
                        'issue': 'Security: Unsafe Cross-Origin Links',
                        'details': f'External domain {domain} opens in new tab without rel="noopener" (Best practice recommendation)'
                    })

    def _check_security_issues(self, result, issues):
        """Check for security issues"""
        url = result.get('url', '')
        headers = result.get('response_headers', {})
        links_data = result.get('links_data', [])
        images = result.get('images', [])
        
        # Headers keys are case-insensitive in requests usually, but let's normalize for safety
        headers_lower = {k.lower(): v for k, v in headers.items()}
        
        # 1. Content-Security-Policy (CSP)
        # SITE-WIDE issue, report only once per domain
        if 'content-security-policy' not in headers_lower:
            try:
                domain = urlparse(url).netloc
                issue_key = (domain, 'missing_csp')
                if issue_key not in self.reported_sitewide_issues:
                    self.reported_sitewide_issues.add(issue_key)
                    issues.append({
                        'url': f'{urlparse(url).scheme}://{domain}',
                        'type': 'info',
                        'category': 'Security',
                        'issue': 'Security: Missing Content-Security-Policy',
                        'details': 'Server does not send Content-Security-Policy header. This is a site-wide configuration issue.'
                    })
            except:
                pass

        # 2. Strict-Transport-Security (HSTS)
        # Only relevant for HTTPS pages
        if url.startswith('https://') and 'strict-transport-security' not in headers_lower:
             try:
                domain = urlparse(url).netloc
                issue_key = (domain, 'missing_hsts')
                if issue_key not in self.reported_sitewide_issues:
                    self.reported_sitewide_issues.add(issue_key)
                    issues.append({
                        'url': f'{urlparse(url).scheme}://{domain}',
                        'type': 'warning',
                        'category': 'Security',
                        'issue': 'Security: Missing HSTS Header',
                        'details': 'HTTP Strict Transport Security (HSTS) is not enabled. Users effectively can be downgraded to HTTP.'
                    })
             except:
                pass
                
        # 3. X-Frame-Options
        if 'x-frame-options' not in headers_lower:
             try:
                domain = urlparse(url).netloc
                issue_key = (domain, 'missing_xfo')
                if issue_key not in self.reported_sitewide_issues:
                    self.reported_sitewide_issues.add(issue_key)
                    issues.append({
                        'url': f'{urlparse(url).scheme}://{domain}',
                        'type': 'info',
                        'category': 'Security',
                        'issue': 'Security: Missing X-Frame-Options',
                        'details': 'Missing X-Frame-Options header can leave the site vulnerable to Clickjacking.'
                    })
             except:
                pass


        # 4. Mixed Content Detection (Active on HTTPS pages)
        if url.startswith('https://'):
            mixed_content_assets = []
            
            # Check Images
            for img in images:
                src = img.get('src', '')
                if src.startswith('http://'):
                    mixed_content_assets.append(f'Image: {src}')
            
            # Check Scripts (if accessible, though mainly we have links_data and images)
            # We can check links_data for external scripts if they were categorized as such, 
            # but currently links_data is mostly anchor tags.
            # However, we can check if any *resources* we tracked are HTTP.
            
            # Check for protocol-relative links (Legacy check, moved here)
            protocol_relative_count = 0
            for img in images:
                if img.get('src', '').startswith('//'):
                    protocol_relative_count += 1
            for link in links_data:
                if link.get('href', '').startswith('//'):
                    protocol_relative_count += 1
            
            if mixed_content_assets:
                # Limit details length
                details_list = mixed_content_assets[:5]
                details_str = ', '.join(details_list)
                if len(mixed_content_assets) > 5:
                    details_str += f', and {len(mixed_content_assets) - 5} more'
                    
                issues.append({
                    'url': url,
                    'type': 'error',
                    'category': 'Security',
                    'issue': 'Security: Mixed Content',
                    'details': f'Secure page loads insecure (HTTP) assets: {details_str}',
                    'mixed_content_assets': mixed_content_assets # Pass full list for frontend
                })

            if protocol_relative_count > 0:
                 issues.append({
                    'url': url,
                    'type': 'warning',
                    'category': 'Security',
                    'issue': 'Security: Protocol-Relative Resource Links',
                    'details': f'{protocol_relative_count} resources use protocol-relative URLs (//). Use explicit HTTPS instead.'
                })

    def detect_duplication_issues(self, all_results, similarity_threshold=0.85):
        """
        Detect content duplication across all crawled pages.
        Optimized with pre-processing and early-exit strategies.

        Args:
            all_results: List of all crawled result dictionaries
            similarity_threshold: Minimum similarity ratio to flag as duplicate (0.0-1.0)
        """
        issues = []
        
        # Pre-process results to avoid repeated string normalization
        # This is O(N) instead of O(N^2) work
        processed_data = []
        for result in all_results:
            processed_data.append(self._preprocess_for_duplication(result))

        # Weights for similarity calculation
        weights = {
            'title': 0.35,
            'desc': 0.35,
            'h1': 0.20,
            'word_count': 0.10
        }

        # Compare each result with all others
        # Uses strict upper-triangular loop to avoid redundant pairs and complex set lookups
        for i in range(len(processed_data)):
            data1 = processed_data[i]

            # Skip if URL should be excluded
            if self._should_exclude(data1['url']):
                continue

            for j in range(i + 1, len(processed_data)):
                data2 = processed_data[j]

                # Skip if URL should be excluded
                if self._should_exclude(data2['url']):
                    continue

                # --- FAST PATH: Early Exit based on Max Possible Score ---
                
                # 1. Word Count Check (Weight: 0.10)
                # Max contribution of word count is 0.10.
                if data1['word_count'] and data2['word_count']:
                    max_wc = max(data1['word_count'], data2['word_count'])
                    min_wc = min(data1['word_count'], data2['word_count'])
                    wc_sim = min_wc / max_wc if max_wc > 0 else 0
                else:
                    wc_sim = 0
                
                current_score_contribution = wc_sim * weights['word_count']
                max_potential_score = 1.0 - weights['word_count'] + current_score_contribution
                
                # Evaluation: If even with perfect matches on everything else, we can't reach threshold, SKIP.
                if max_potential_score < similarity_threshold:
                    continue

                # 2. Title Check (Weight: 0.35)
                # Use real_quick_ratio (very fast) as upper bound first to filter obvious non-matches
                if data1['title'] and data2['title']:
                    matcher = SequenceMatcher(None, data1['title'], data2['title'])
                    # quick_ratio() is an upper bound on ratio(). If quick_check fails, real check will definitely fail.
                    if matcher.real_quick_ratio() * weights['title'] + (max_potential_score - weights['title']) < similarity_threshold:
                         continue
                         
                    title_sim = matcher.ratio()
                else:
                    title_sim = 0
                
                current_score_contribution += title_sim * weights['title']
                max_potential_score = max_potential_score - weights['title'] + (title_sim * weights['title'])

                # Recalculate max potential check
                if max_potential_score < similarity_threshold:
                    continue

                # 3. Description Check (Weight: 0.35)
                if data1['desc'] and data2['desc']:
                     matcher = SequenceMatcher(None, data1['desc'], data2['desc'])
                     if matcher.real_quick_ratio() * weights['desc'] + (max_potential_score - weights['desc']) < similarity_threshold:
                         continue
                     desc_sim = matcher.ratio()
                else:
                    desc_sim = 0
                    
                current_score_contribution += desc_sim * weights['desc']
                max_potential_score = max_potential_score - weights['desc'] + (desc_sim * weights['desc'])
                
                if max_potential_score < similarity_threshold:
                    continue

                # 4. H1 Check (Weight: 0.20)
                if data1['h1'] and data2['h1']:
                    matcher = SequenceMatcher(None, data1['h1'], data2['h1'])
                    h1_sim = matcher.ratio()
                else:
                    h1_sim = 0
                    
                current_score_contribution += h1_sim * weights['h1']

                # Final Check
                if current_score_contribution >= similarity_threshold:
                    # Add issue for both URLs
                    issues.append({
                        'url': data1['url'],
                        'type': 'warning',
                        'category': 'Duplication',
                        'issue': 'Duplicate Content Detected',
                        'details': f'Content is {current_score_contribution*100:.1f}% similar to {data2["url"]}'
                    })
                    issues.append({
                        'url': data2['url'],
                        'type': 'warning',
                        'category': 'Duplication',
                        'issue': 'Duplicate Content Detected',
                        'details': f'Content is {current_score_contribution*100:.1f}% similar to {data1["url"]}'
                    })

        # Add all detected duplication issues
        with self.issues_lock:
            self.detected_issues.extend(issues)

    def _preprocess_for_duplication(self, result):
        """Extract and normalize fields for faster duplicate detection"""
        return {
            'url': result.get('url', ''),
            'title': result.get('title', '').lower().strip() if result.get('title') else '',
            'desc': result.get('meta_description', '').lower().strip() if result.get('meta_description') else '',
            'h1': result.get('h1', '').lower().strip() if result.get('h1') else '',
            'word_count': result.get('word_count', 0)
        }

    def _calculate_content_similarity(self, result1, result2):
        """
        Legacy method kept for compatibility, now wraps optimized logic.
        """
        p1 = self._preprocess_for_duplication(result1)
        p2 = self._preprocess_for_duplication(result2)
        
        sim_title = SequenceMatcher(None, p1['title'], p2['title']).ratio() if p1['title'] and p2['title'] else 0
        sim_desc = SequenceMatcher(None, p1['desc'], p2['desc']).ratio() if p1['desc'] and p2['desc'] else 0
        sim_h1 = SequenceMatcher(None, p1['h1'], p2['h1']).ratio() if p1['h1'] and p2['h1'] else 0
        
        if p1['word_count'] and p2['word_count']:
             sim_wc = min(p1['word_count'], p2['word_count']) / max(p1['word_count'], p2['word_count'])
        else:
             sim_wc = 0
             
        return (sim_title * 0.35) + (sim_desc * 0.35) + (sim_h1 * 0.20) + (sim_wc * 0.10)

    def _text_similarity(self, text1, text2):
        """Calculate similarity ratio between two text strings using SequenceMatcher"""
        if not text1 or not text2:
            return 0.0
        return SequenceMatcher(None, text1, text2).ratio()

    def _should_exclude(self, url):
        """Check if URL should be excluded from issue detection"""
        parsed = urlparse(url)
        path = parsed.path

        for pattern in self.exclusion_patterns:
            if '*' in pattern:
                if fnmatch(path, pattern):
                    return True
            elif path == pattern or path.startswith(pattern.rstrip('*')):
                return True

        return False

    def _get_status_code_message(self, status_code):
        """Get descriptive message for HTTP status codes"""
        messages = {
            400: 'Bad Request',
            401: 'Unauthorized',
            403: 'Forbidden',
            404: 'Not Found',
            405: 'Method Not Allowed',
            406: 'Not Acceptable',
            408: 'Request Timeout',
            410: 'Gone',
            429: 'Too Many Requests',
            500: 'Internal Server Error',
            501: 'Not Implemented',
            502: 'Bad Gateway',
            503: 'Service Unavailable',
            504: 'Gateway Timeout',
            505: 'HTTP Version Not Supported'
        }
        return messages.get(status_code, f'HTTP {status_code} Error')

    def detect_sitemap_issues(self, sitemap_urls, all_results):
        """
        Cross-reference sitemap URLs with crawled results to detect "Dirty Sitemap" issues.
        
        Args:
            sitemap_urls: List of URLs found in sitemaps
            all_results: List of crawled result dictionaries
            
        Returns:
            dict: Summary with categorized counts and issues list for frontend
        """
        if not sitemap_urls or not all_results:
            return {
                'total': 0,
                'valid': 0,
                'errors': 0,
                'noindex': 0,
                'non_canonical': 0,
                'redirects': 0,
                'not_crawled': 0,
                'issues': []
            }
        
        # Build lookup dict from crawled results (normalized URL -> result)
        results_lookup = {}
        for result in all_results:
            url = result.get('url', '')
            if url:
                normalized = self._normalize_url_for_comparison(url)
                results_lookup[normalized] = result

        # DEBUG: Print sample keys
        if sitemap_urls:
            print(f"DEBUG SITEMAP: Sitemap URLs count: {len(sitemap_urls)}")
            print(f"DEBUG SITEMAP: Crawled lookup keys count: {len(results_lookup)}")
            sample_sitemap = sitemap_urls[0]
            norm_sitemap = self._normalize_url_for_comparison(sample_sitemap)
            print(f"DEBUG SITEMAP: Sample Sitemap URL: '{sample_sitemap}' -> '{norm_sitemap}'")
            if results_lookup:
                sample_crawled = list(results_lookup.keys())[0]
                print(f"DEBUG SITEMAP: Sample Crawled Key: '{sample_crawled}'")
                if norm_sitemap not in results_lookup:
                    print(f"DEBUG SITEMAP: Mismatch! '{norm_sitemap}' not found in lookup.")
    
        # Track counts
        valid_count = 0
        error_count = 0
        noindex_count = 0
        non_canonical_count = 0
        redirect_count = 0
        not_crawled_count = 0
        sitemap_issues = []
        
        for sitemap_url in sitemap_urls:
            normalized_sitemap_url = self._normalize_url_for_comparison(sitemap_url)
            result = results_lookup.get(normalized_sitemap_url)
            
            if not result:
                # URL in sitemap but not crawled
                not_crawled_count += 1
                continue
            
            status_code = result.get('status_code', 0)
            robots_meta = result.get('robots', '').lower()
            x_robots_tag = result.get('x_robots_tag', '').lower()
            canonical_url = result.get('canonical_url', '')
            
            is_noindex = 'noindex' in robots_meta or 'noindex' in x_robots_tag
            
            # Check for non-self-canonical
            is_non_canonical = False
            if canonical_url:
                normalized_canonical = self._normalize_url_for_comparison(canonical_url)
                if normalized_canonical != normalized_sitemap_url:
                    is_non_canonical = True
            
            # Categorize the URL
            if status_code >= 400 or status_code == 0:
                # Error (4xx, 5xx, or connection failed)
                error_count += 1
                sitemap_issues.append({
                    'url': sitemap_url,
                    'type': 'error',
                    'category': 'Sitemap',
                    'issue': 'Sitemap: Broken URL',
                    'details': f'URL returns {self._get_status_code_message(status_code)} (Status {status_code})'
                })
            elif status_code >= 300 and status_code < 400:
                # Redirect
                redirect_count += 1
                sitemap_issues.append({
                    'url': sitemap_url,
                    'type': 'warning',
                    'category': 'Sitemap',
                    'issue': 'Sitemap: Redirecting URL',
                    'details': f'URL redirects ({status_code}) - update sitemap with final destination'
                })
            elif is_noindex:
                # Noindex
                noindex_count += 1
                sitemap_issues.append({
                    'url': sitemap_url,
                    'type': 'warning',
                    'category': 'Sitemap',
                    'issue': 'Sitemap: Noindexed URL',
                    'details': 'URL has noindex directive - remove from sitemap or remove noindex'
                })
            elif is_non_canonical:
                # Non-canonical
                non_canonical_count += 1
                sitemap_issues.append({
                    'url': sitemap_url,
                    'type': 'warning',
                    'category': 'Sitemap',
                    'issue': 'Sitemap: Non-Canonical URL',
                    'details': f'URL canonicalises to {canonical_url} - update sitemap with canonical URL'
                })
            else:
                # Valid (200 OK, indexable, self-canonical)
                valid_count += 1
        
        # Add issues to main detected_issues list
        with self.issues_lock:
            self.detected_issues.extend(sitemap_issues)
        
        return {
            'total': len(sitemap_urls),
            'valid': valid_count,
            'errors': error_count,
            'noindex': noindex_count,
            'non_canonical': non_canonical_count,
            'redirects': redirect_count,
            'not_crawled': not_crawled_count,
            'issues': sitemap_issues
        }

    def detect_links_to_redirects(self, all_results, all_links):
        """
        Detect internal links that point to URLs that redirect.
        This wastes crawl budget and should be fixed by updating the href.
        
        Args:
            all_results: List of all crawled result dictionaries
            all_links: List of all link dictionaries {source_url, target_url, is_internal, ...}
            
        Returns:
            dict: Summary with count and list of problematic links
        """
        if not all_results or not all_links:
            return {'total_links_to_redirects': 0, 'pages_affected': 0, 'links': []}
        
        # Build a lookup of URL -> status code and redirect info
        url_status_map = {}
        for result in all_results:
            url = result.get('url', '')
            if not url:
                continue
            normalized = self._normalize_url_for_comparison(url)
            url_status_map[normalized] = {
                'status_code': result.get('status_code', 0),
                'final_url': result.get('final_url', ''),
                'redirect_chain': result.get('redirect_chain', [])
            }
        
        # Track links pointing to redirects
        links_to_redirects = []
        pages_with_redirect_links = set()
        
        for link in all_links:
            source_url = link.get('source_url', '')
            target_url = link.get('target_url', '')
            is_internal = link.get('is_internal', False)
            
            if not source_url or not target_url or not is_internal:
                continue
            
            # Check target URL's status
            normalized_target = self._normalize_url_for_comparison(target_url)
            target_info = url_status_map.get(normalized_target)
            
            if target_info:
                status = target_info.get('status_code', 0)
                if 300 <= status < 400:
                    # This is a link to a redirect!
                    final_url = target_info.get('final_url', '')
                    links_to_redirects.append({
                        'source_url': source_url,
                        'target_url': target_url,
                        'target_status': status,
                        'final_url': final_url,
                        'anchor_text': link.get('text', ''),
                        'redirect_chain': target_info.get('redirect_chain', [])
                    })
                    pages_with_redirect_links.add(source_url)
        
        # Create issues for pages with links to redirects
        # Group by source URL to avoid duplicate issues per page
        links_by_source = {}
        for link in links_to_redirects:
            source = link['source_url']
            if source not in links_by_source:
                links_by_source[source] = []
            links_by_source[source].append(link)
        
        with self.issues_lock:
            for source_url, source_links in links_by_source.items():
                count = len(source_links)
                # Show first few examples
                examples = [f"{l['target_url']} ({l['target_status']})" for l in source_links[:3]]
                examples_str = ', '.join(examples)
                if count > 3:
                    examples_str += f', and {count - 3} more'
                
                self.detected_issues.append({
                    'url': source_url,
                    'type': 'warning',
                    'category': 'Links',
                    'issue': 'Links: Internal Links to Redirects',
                    'details': f'{count} internal links point to redirecting URLs: {examples_str}',
                    'links_to_redirects': source_links
                })
        
        return {
            'total_links_to_redirects': len(links_to_redirects),
            'pages_affected': len(pages_with_redirect_links),
            'links': links_to_redirects
        }

    def detect_broken_link_sources(self, all_results, all_links):
        """
        Find which pages contain links to broken URLs (4xx/5xx status).
        Enriches broken URL issues with source page information.
        
        Args:
            all_results: List of all crawled result dictionaries
            all_links: List of all link dictionaries {source_url, target_url, is_internal, ...}
            
        Returns:
            dict: Summary with broken URLs and their source pages
        """
        if not all_results or not all_links:
            return {'broken_urls': [], 'total_broken_links': 0}
        
        # Build a lookup of URL -> status code
        url_status_map = {}
        for result in all_results:
            url = result.get('url', '')
            if not url:
                continue
            normalized = self._normalize_url_for_comparison(url)
            url_status_map[normalized] = {
                'status_code': result.get('status_code', 0),
                'title': result.get('title', ''),
                'url': url
            }
        
        # Build a lookup of target URL -> list of source pages
        target_sources_map = {}
        for link in all_links:
            source_url = link.get('source_url', '')
            target_url = link.get('target_url', '')
            
            if not source_url or not target_url:
                continue
            
            normalized_target = self._normalize_url_for_comparison(target_url)
            if normalized_target not in target_sources_map:
                target_sources_map[normalized_target] = []
            
            target_sources_map[normalized_target].append({
                'source_url': source_url,
                'anchor_text': link.get('text', ''),
                'is_internal': link.get('is_internal', False)
            })
        
        # Find broken URLs and their sources
        broken_urls_with_sources = []
        
        for normalized_url, status_info in url_status_map.items():
            status = status_info.get('status_code', 0)
            original_url = status_info.get('url', normalized_url)
            
            if status >= 400 or status == 0:  # 4xx, 5xx, or no response
                sources = target_sources_map.get(normalized_url, [])
                
                if sources:
                    broken_urls_with_sources.append({
                        'broken_url': original_url,
                        'status_code': status,
                        'title': status_info.get('title', ''),
                        'source_pages': sources,
                        'source_count': len(sources)
                    })
        
        # Enhance existing 4xx/5xx issues with source page info
        # Also create new issues that highlight the source pages
        with self.issues_lock:
            for broken_item in broken_urls_with_sources:
                broken_url = broken_item['broken_url']
                status = broken_item['status_code']
                sources = broken_item['source_pages']
                source_count = broken_item['source_count']
                
                if source_count > 0:
                    # Get first few source URLs for the issue details
                    source_urls = [s['source_url'] for s in sources[:5]]
                    source_list = ', '.join(source_urls)
                    if source_count > 5:
                        source_list += f' and {source_count - 5} more'
                    
                    self.detected_issues.append({
                        'url': broken_url,
                        'type': 'error' if status >= 400 else 'warning',
                        'category': 'Links',
                        'issue': f'Broken Link Sources: {status} error linked from {source_count} pages',
                        'details': f'This broken URL is linked from: {source_list}',
                        'source_pages': sources,
                        'source_count': source_count
                    })
        
        return {
            'broken_urls': broken_urls_with_sources,
            'total_broken_links': sum(item['source_count'] for item in broken_urls_with_sources)
        }

    def detect_hreflang_issues(self, all_results):
        """
        Detect hreflang implementation issues across all crawled pages.
        
        Checks:
        1. Invalid ISO language/region codes
        2. Missing reciprocal hreflang links (if page A points to B, B should point back to A)
        3. Missing self-referencing hreflang
        4. Hreflang pointing to non-200 pages
        
        Returns a summary dict with hreflang data for frontend visualization.
        """
        # Valid ISO 639-1 language codes (common ones)
        valid_lang_codes = {
            'aa', 'ab', 'af', 'ak', 'am', 'ar', 'as', 'ay', 'az', 'ba', 'be', 'bg', 'bh', 'bi', 'bn', 'bo', 'br', 'bs',
            'ca', 'co', 'cs', 'cy', 'da', 'de', 'dz', 'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'fi', 'fj', 'fo', 'fr',
            'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'ha', 'he', 'hi', 'hr', 'hu', 'hy', 'ia', 'id', 'ie', 'ik', 'is', 'it',
            'iu', 'ja', 'jv', 'ka', 'kk', 'kl', 'km', 'kn', 'ko', 'ks', 'ku', 'ky', 'la', 'lb', 'ln', 'lo', 'lt', 'lv',
            'mg', 'mi', 'mk', 'ml', 'mn', 'mr', 'ms', 'mt', 'my', 'na', 'ne', 'nl', 'no', 'oc', 'om', 'or', 'pa', 'pl',
            'ps', 'pt', 'qu', 'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sd', 'sg', 'sh', 'si', 'sk', 'sl', 'sm', 'sn', 'so',
            'sq', 'sr', 'ss', 'st', 'su', 'sv', 'sw', 'ta', 'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts',
            'tt', 'tw', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wo', 'xh', 'yi', 'yo', 'za', 'zh', 'zu',
            'x-default'  # Special value for default/fallback
        }
        
        # Regex for valid hreflang format: lang or lang-region (e.g., en, en-US, zh-Hans-CN)
        hreflang_pattern = re.compile(r'^[a-z]{2,3}(-[A-Za-z]{2,4})?(-[A-Za-z]{2})?$|^x-default$', re.IGNORECASE)
        
        # Build URL -> hreflang map and status map
        url_hreflang_map = {}  # url -> list of {lang, target_url}
        url_status_map = {}    # url -> status_code
        
        for result in all_results:
            url = result.get('url', '')
            if not url:
                continue
            
            normalized_url = self._normalize_url_for_comparison(url)
            url_status_map[normalized_url] = result.get('status_code', 0)
            
            hreflang_list = result.get('hreflang', [])
            if hreflang_list:
                url_hreflang_map[normalized_url] = {
                    'original_url': url,
                    'hreflangs': hreflang_list
                }
        
        # Track all hreflang entries for frontend matrix
        hreflang_matrix = []
        
        # Detect issues
        for normalized_url, data in url_hreflang_map.items():
            source_url = data['original_url']
            hreflangs = data['hreflangs']
            
            has_self_reference = False
            
            for hreflang_entry in hreflangs:
                lang = hreflang_entry.get('lang', '')
                target_url = hreflang_entry.get('url', '')
                
                if not lang or not target_url:
                    continue
                
                normalized_target = self._normalize_url_for_comparison(target_url)
                
                # Check 1: Validate language code format
                lang_base = lang.split('-')[0].lower()
                if not hreflang_pattern.match(lang):
                    with self.issues_lock:
                        self.detected_issues.append({
                            'url': source_url,
                            'type': 'warning',
                            'category': 'International',
                            'issue': 'Hreflang: Invalid Language Code',
                            'details': f'Invalid hreflang code "{lang}" - should be ISO 639-1 format (e.g., en, en-US)'
                        })
                elif lang_base not in valid_lang_codes and lang.lower() != 'x-default':
                    with self.issues_lock:
                        self.detected_issues.append({
                            'url': source_url,
                            'type': 'warning',
                            'category': 'International',
                            'issue': 'Hreflang: Unknown Language Code',
                            'details': f'Unrecognized language code "{lang}" - verify it is a valid ISO 639-1 code'
                        })
                
                # Check for self-reference
                if normalized_target == normalized_url:
                    has_self_reference = True
                
                # Check 2: Reciprocal link validation
                reciprocal_status = 'unknown'
                if normalized_target in url_hreflang_map:
                    target_hreflangs = url_hreflang_map[normalized_target]['hreflangs']
                    # Check if target points back to source
                    points_back = any(
                        self._normalize_url_for_comparison(h.get('url', '')) == normalized_url
                        for h in target_hreflangs
                    )
                    if points_back:
                        reciprocal_status = 'valid'
                    else:
                        reciprocal_status = 'missing'
                        with self.issues_lock:
                            self.detected_issues.append({
                                'url': source_url,
                                'type': 'warning',
                                'category': 'International',
                                'issue': 'Hreflang: Missing Reciprocal Link',
                                'details': f'Page points to {target_url} ({lang}) but target does not point back'
                            })
                else:
                    # Target not crawled or doesn't have hreflang
                    reciprocal_status = 'not_crawled'
                
                # Check 3: Target page status
                target_status = url_status_map.get(normalized_target, 0)
                if target_status >= 400 or target_status == 0:
                    with self.issues_lock:
                        self.detected_issues.append({
                            'url': source_url,
                            'type': 'error',
                            'category': 'International',
                            'issue': 'Hreflang: Points to Non-200 Page',
                            'details': f'Hreflang ({lang}) points to {target_url} which returns status {target_status}'
                        })
                
                # Add to matrix for frontend
                hreflang_matrix.append({
                    'source_url': source_url,
                    'lang': lang,
                    'target_url': target_url,
                    'reciprocal_status': reciprocal_status,
                    'target_status': target_status
                })
            
            # Check 4: Missing self-reference
            if hreflangs and not has_self_reference:
                with self.issues_lock:
                    self.detected_issues.append({
                        'url': source_url,
                        'type': 'info',
                        'category': 'International',
                        'issue': 'Hreflang: Missing Self-Reference',
                        'details': 'Page has hreflang tags but no self-referencing hreflang'
                    })
        
        return {
            'hreflang_matrix': hreflang_matrix,
            'pages_with_hreflang': len(url_hreflang_map),
            'total_hreflang_entries': len(hreflang_matrix)
        }

    def get_issues(self):
        """Get all detected issues"""
        with self.issues_lock:
            return self.detected_issues.copy()

    def reset(self):
        """Reset detected issues"""
        with self.issues_lock:
            self.detected_issues.clear()
            self.reported_sitewide_issues.clear()

