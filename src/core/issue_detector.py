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
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': 'Missing Title Tag',
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
            
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': 'Meta Description: Missing',
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
            issues.append({
                'url': url,
                'type': severity,
                'category': 'SEO',
                'issue': 'Missing H1 Tag',
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
                     issues.append({
                        'url': url,
                        'type': severity,
                        'category': 'SEO',
                        'issue': f'H{level} appears before H1',
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
                 issues.append({
                    'url': url,
                    'type': severity,
                    'category': 'SEO',
                    'issue': 'H2: Duplicate',
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
        """Check for structured data issues"""
        url = result.get('url', '')

        if not result.get('json_ld') and not result.get('schema_org'):
            issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Structured Data',
                'issue': 'No Structured Data',
                'details': 'Page has no JSON-LD or Schema.org markup'
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
        
        # Missing Content-Security-Policy - SITE-WIDE issue, report only once per domain
        # Headers keys are case-insensitive in requests usually.
        has_csp = False
        for k in headers.keys():
            if k.lower() == 'content-security-policy':
                has_csp = True
                break
        
        if not has_csp:
            # Only report once per domain
            try:
                domain = urlparse(url).netloc
                issue_key = (domain, 'missing_csp')
                if issue_key not in self.reported_sitewide_issues:
                    self.reported_sitewide_issues.add(issue_key)
                    issues.append({
                        'url': f'{urlparse(url).scheme}://{domain}',  # Use domain as URL
                        'type': 'info',
                        'category': 'Security',
                        'issue': 'Security: Missing Content-Security-Policy Header',
                        'details': 'Server does not send Content-Security-Policy header. This is a site-wide configuration issue.'
                    })
            except:
                pass
            
        # Protocol-Relative Resource Links
        # Check images and links
        protocol_relative_count = 0
        for img in images:
            if img.get('src', '').startswith('//'):
                protocol_relative_count += 1
                
        # Note: Extracted links might be absolute already, need to check if raw href was protocol relative.
        # links_data stores 'href' as raw?
        for link in links_data:
            if link.get('href', '').startswith('//'):
                protocol_relative_count += 1
                
        if protocol_relative_count > 0:
             issues.append({
                'url': url,
                'type': 'warning',
                'category': 'Security',
                'issue': 'Security: Protocol-Relative Resource Links',
                'details': f'{protocol_relative_count} resources use protocol-relative URLs (//)'
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

    def get_issues(self):
        """Get all detected issues"""
        with self.issues_lock:
            return self.detected_issues.copy()

    def reset(self):
        """Reset detected issues"""
        with self.issues_lock:
            self.detected_issues.clear()
            self.reported_sitewide_issues.clear()

