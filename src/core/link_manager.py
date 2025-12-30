import re
import threading
from urllib.parse import urljoin, urlparse
from collections import deque


class LinkManager:
    """Manages link discovery, tracking, and extraction"""

    def __init__(self, base_domain, trap_threshold=100):
        self.base_domain = base_domain
        self.visited_urls = set()
        self.discovered_urls = deque()
        self.all_discovered_urls = set()
        self.all_links = []
        self.links_set = set()
        self.source_pages = {}  # Maps target_url -> list of source_urls
        
        # Trap detection
        self.url_pattern_counts = {}
        self.trap_patterns = {}
        self.TRAP_THRESHOLD = trap_threshold  # Configurable per crawl

        self.urls_lock = threading.Lock()
        self.links_lock = threading.Lock()
        
    def _get_url_signature(self, url):
        """Generate a signature for the URL by replacing dynamic segments"""
        try:
            parsed = urlparse(url)
            path = parsed.path
            
            # Replace digits with \d+
            path = re.sub(r'\d+', r'\\d+', path)
            
            # Replace UUIDs (simplistic)
            path = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', r'\\uuid', path)
            
            return path
        except:
            return url

    def extract_links(self, soup, current_url, depth, should_crawl_callback):
        """Extract links from HTML and add to discovery queue"""
        links = soup.find_all('a', href=True)

        for link in links:
            href = link['href'].strip()
            if not href or href.startswith('#') or href.startswith('mailto:') or href.startswith('tel:'):
                continue

            # Convert relative URLs to absolute
            absolute_url = urljoin(current_url, href)

            # Clean URL (remove fragment)
            parsed = urlparse(absolute_url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if parsed.query:
                clean_url += f"?{parsed.query}"

            # Thread-safe checking and adding
            with self.urls_lock:
                # Track source page for this URL
                if clean_url not in self.source_pages:
                    self.source_pages[clean_url] = []
                if current_url not in self.source_pages[clean_url]:
                    self.source_pages[clean_url].append(current_url)
                
                # Check trap detection BEFORE checking if discovered
                # This ensures we count patterns even across different discovery paths
                # But to avoid re-counting the SAME URL, we normally check if in discovered.
                # Here we only want to block NEW URLs.

                if (clean_url not in self.visited_urls and
                    clean_url not in self.all_discovered_urls and
                    clean_url != current_url):
                    
                    # Trap logic
                    signature = self._get_url_signature(clean_url)
                    count = self.url_pattern_counts.get(signature, 0)
                    
                    if count >= self.TRAP_THRESHOLD:
                         # It is a trap
                        if signature not in self.trap_patterns:
                            self.trap_patterns[signature] = {
                                'pattern': signature,
                                'example_url': clean_url,
                                'count': 0
                            }
                        self.trap_patterns[signature]['count'] += 1
                        # SKIP adding
                        continue

                    # Check if checks out with crawler policy
                    if should_crawl_callback(clean_url):
                        # Increment count only if we decide to crawl it
                        self.url_pattern_counts[signature] = count + 1
                        
                        self.all_discovered_urls.add(clean_url)
                        self.discovered_urls.append((clean_url, depth))

    def collect_all_links(self, soup, source_url, crawl_results, base_domain=None):
        """
        Extract all links from the page for reporting purposes (Internal vs External)
        Stores in self.all_links
        """
        if not soup:
            return

        for a_tag in soup.find_all('a', href=True):
            href = a_tag.get('href')
            anchor_text = a_tag.get_text(strip=True)[:100]  # Limit length
            
            absolute_url = urljoin(source_url, href)
            # Normalize - remove fragment
            if '#' in absolute_url:
                absolute_url = absolute_url.split('#')[0]
            
            # Skip empty or invalid
            if not absolute_url.startswith(('http://', 'https://')):
                continue
            
            try:
                # Parse target URL for domain info
                parsed_target = urlparse(absolute_url)
                
                # Determine scope if base_domain provided
                scope = 'external'
                if base_domain:
                    scope = self._determine_scope(absolute_url, base_domain)
                else:
                    # Fallback logic if no base_domain
                    is_int = self.is_internal(absolute_url)
                    scope = 'root' if is_int else 'external'

                # Define is_internal for DB compatibility (root or sub = internal)
                is_internal = scope in ['root', 'sub']
                
                # Find the status of the target URL if we've crawled it
                target_status = None
                for result in crawl_results:
                    if result['url'] == absolute_url:
                        target_status = result['status_code']
                        break

                # Determine placement (navigation, footer, body)
                placement = self._detect_link_placement(a_tag)

                # Determine nofollow attribute
                nofollow = 'nofollow' in a_tag.get('rel', [])

                link_data = {
                    'source_url': source_url,
                    'target_url': absolute_url,
                    'anchor_text': anchor_text or '(no text)',
                    'is_internal': is_internal,
                    'target_domain': parsed_target.netloc,
                    'target_status': target_status,
                    'placement': placement,
                    'nofollow': nofollow,
                    'scope': scope
                }

                # Track source page for this URL (for "Linked From" feature)
                with self.urls_lock:
                    if absolute_url not in self.source_pages:
                        self.source_pages[absolute_url] = []
                    if source_url not in self.source_pages[absolute_url]:
                        self.source_pages[absolute_url].append(source_url)

                # Thread-safe adding to links collection with duplicate checking
                with self.links_lock:
                    link_key = f"{link_data['source_url']}|{link_data['target_url']}"

                    if link_key not in self.links_set:
                        self.links_set.add(link_key)
                        self.all_links.append(link_data)

            except Exception:
                # Skip problematic links silently
                continue


    def _detect_link_placement(self, link_element):
        """Detect where on the page a link is placed"""
        # Check parent elements up the tree
        current = link_element.parent

        while current and current.name:
            # Check for footer
            if current.name == 'footer':
                return 'footer'

            # Check for footer by class/id
            classes = current.get('class', [])
            element_id = current.get('id', '')
            classes_str = ' '.join(classes).lower() if classes else ''

            if 'footer' in classes_str or 'footer' in element_id.lower():
                return 'footer'

            # Check for navigation
            if current.name in ['nav', 'header']:
                return 'navigation'

            # Check for navigation by class/id
            if any(keyword in classes_str or keyword in element_id.lower()
                   for keyword in ['nav', 'menu', 'header']):
                return 'navigation'

            current = current.parent

        # Default to body if not in nav or footer
        return 'body'

    def is_internal(self, url):
        """Check if URL is internal to the base domain"""
        parsed_url = urlparse(url)
        url_domain_clean = parsed_url.netloc.replace('www.', '', 1)
        base_domain_clean = self.base_domain.replace('www.', '', 1)
        return url_domain_clean == base_domain_clean

    def add_url(self, url, depth):
        """Add a URL to the discovery queue"""
        with self.urls_lock:
            if url not in self.all_discovered_urls and url not in self.visited_urls:
                self.all_discovered_urls.add(url)
                self.discovered_urls.append((url, depth))

    def mark_visited(self, url):
        """Mark a URL as visited"""
        with self.urls_lock:
            self.visited_urls.add(url)

    def get_next_url(self):
        """Get the next URL to crawl"""
        with self.urls_lock:
            if self.discovered_urls:
                return self.discovered_urls.popleft()
        return None

    def get_stats(self):
        """Get current statistics"""
        with self.urls_lock:
            return {
                'discovered': len(self.all_discovered_urls),
                'visited': len(self.visited_urls),
                'pending': len(self.discovered_urls)
            }

    def update_link_statuses(self, crawl_results):
        """Update target_status for all links based on crawl results"""
        # Build a fast lookup dict
        status_lookup = {result['url']: result['status_code'] for result in crawl_results}

        with self.links_lock:
            for link in self.all_links:
                target_url = link['target_url']
                if target_url in status_lookup:
                    link['target_status'] = status_lookup[target_url]

    def get_source_pages(self, url):
        """Get list of source pages that link to this URL"""
        with self.urls_lock:
            return self.source_pages.get(url, []).copy()

    def reset(self):
        """Reset all state"""
        with self.urls_lock:
            self.visited_urls.clear()
            self.discovered_urls.clear()
            self.all_discovered_urls.clear()
            self.source_pages.clear()

        with self.links_lock:
            self.all_links.clear()
            self.links_set.clear()

    def _determine_scope(self, url, base_domain):
        """
        Determine if a URL is root, subdomain, or external.
        root: example.com, www.example.com
        sub: blog.example.com
        external: google.com
        """
        parsed_url = urlparse(url)
        url_domain = parsed_url.netloc
        base_clean = base_domain.replace('www.', '')
        url_clean = url_domain.replace('www.', '')

        if url_clean == base_clean:
            return 'root'
        elif url_clean.endswith('.' + base_clean):
            return 'sub'
        else:
            return 'external'

    def get_traps(self):
        """Get list of detected crawl traps"""
        with self.urls_lock:
            return list(self.trap_patterns.values())
