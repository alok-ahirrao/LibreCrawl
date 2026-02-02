"""
GMB Crawler V2 - Additional Data Parser

Extracts supplementary business information:
- Q&A entries
- Business posts/updates
- Menu items
- Services
- Products
- Price range
- Booking/order links
- Eco certifications
"""

import re
from typing import Optional, List, Dict, Any
from .base_parser import BaseParser


class AdditionalDataParser(BaseParser):
    """
    Parser for additional business data.
    """
    
    # Price range indicators
    PRICE_LEVELS = ['$', '$$', '$$$', '$$$$']
    
    def parse(self) -> Dict[str, Any]:
        """
        Extract additional data.
        
        Returns:
            AdditionalData TypedDict
        """
        result = {
            'qa_count': self.extract_qa_count(),
            'qa_entries': self.extract_qa_entries(),
            'posts_count': self.extract_posts_count(),
            'posts': self.extract_posts(),
            'last_post_date': None,
            'menu_items': self.extract_menu_items(),
            'services': self.extract_services(),
            'products': self.extract_products(),
            'price_range': self.extract_price_range(),
            'booking_links': self.extract_booking_links(),
            'order_links': self.extract_order_links(),
            'years_in_business': self.extract_years_in_business(),
            'founded_year': None,
            'eco_certifications': self.extract_eco_certifications(),
        }
        
        # Set last post date from posts
        if result['posts']:
            for post in result['posts']:
                if post.get('date'):
                    result['last_post_date'] = post['date']
                    break
        
        return result
    
    def extract_qa_count(self) -> Optional[int]:
        """Extract Q&A count."""
        if not self.soup:
            return None
        
        # Look for Q&A button/tab
        qa_selectors = [
            'button[aria-label*="Question"]',
            'button[aria-label*="Q&A"]',
            '[data-item-id="qa"]',
        ]
        
        for selector in qa_selectors:
            element = self.soup.select_one(selector)
            if element:
                label = element.get('aria-label', '') + element.get_text()
                match = re.search(r'(\d+)\s*question', label, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        
        # Look for "X questions" text
        qa_text = self.find_element_by_text(r'\d+\s*question')
        if qa_text:
            text = qa_text.get_text() if hasattr(qa_text, 'get_text') else str(qa_text)
            match = re.search(r'(\d+)', text)
            if match:
                return int(match.group(1))
        
        return None
    
    def extract_qa_entries(self, max_entries: int = 5) -> List[Dict[str, Any]]:
        """Extract Q&A entries."""
        entries = []
        
        if not self.soup:
            return entries
        
        # Find Q&A section
        qa_section = self.soup.select_one('div[aria-label*="Questions"]')
        if not qa_section:
            qa_section = self.find_element_by_text('Questions & answers')
            if qa_section:
                qa_section = qa_section.find_parent('div')
        
        if not qa_section:
            return entries
        
        # Find individual Q&A items
        qa_items = qa_section.select('div[data-question-id], div[class*="question"]')
        
        for item in qa_items[:max_entries]:
            entry = self.parse_qa_entry(item)
            if entry:
                entries.append(entry)
        
        return entries
    
    def parse_qa_entry(self, element) -> Optional[Dict[str, Any]]:
        """Parse a single Q&A entry."""
        if not element:
            return None
        
        entry = {
            'question': None,
            'question_date': None,
            'question_author': None,
            'answer': None,
            'answer_date': None,
            'answer_author': None,
            'is_owner_answer': False,
            'upvotes': 0,
        }
        
        # Extract question
        q_el = element.select_one('span[class*="question"], div[class*="question-text"]')
        if q_el:
            entry['question'] = q_el.get_text(strip=True)
        
        # Extract answer
        a_el = element.select_one('span[class*="answer"], div[class*="answer-text"]')
        if a_el:
            entry['answer'] = a_el.get_text(strip=True)
        
        # Check for owner answer
        if element.select_one('[aria-label*="Owner"]'):
            entry['is_owner_answer'] = True
        
        return entry if entry['question'] else None
    
    def extract_posts_count(self) -> Optional[int]:
        """Extract business posts/updates count."""
        if not self.soup:
            return None
        
        # Look for posts/updates tab
        posts_selectors = [
            'button[aria-label*="Updates"]',
            'button[aria-label*="Posts"]',
            '[data-item-id="posts"]',
        ]
        
        for selector in posts_selectors:
            element = self.soup.select_one(selector)
            if element:
                label = element.get('aria-label', '') + element.get_text()
                match = re.search(r'(\d+)\s*(?:update|post)', label, re.IGNORECASE)
                if match:
                    return int(match.group(1))
        
        return None
    
    def extract_posts(self, max_posts: int = 5) -> List[Dict[str, Any]]:
        """Extract business posts/updates."""
        posts = []
        
        if not self.soup:
            return posts
        
        # Find posts section
        posts_section = self.soup.select_one('div[aria-label*="Updates"]')
        if not posts_section:
            posts_section = self.find_element_by_text("From the owner")
            if posts_section:
                posts_section = posts_section.find_parent('div')
        
        if not posts_section:
            return posts
        
        # Find individual posts
        post_elements = posts_section.select('div[data-post-id], div[class*="post"]')
        
        for element in post_elements[:max_posts]:
            post = self.parse_post(element)
            if post:
                posts.append(post)
        
        return posts
    
    def parse_post(self, element) -> Optional[Dict[str, Any]]:
        """Parse a single post."""
        if not element:
            return None
        
        post = {
            'post_id': element.get('data-post-id'),
            'content': None,
            'media_url': None,
            'media_type': None,
            'date': None,
            'post_type': 'UPDATE',
            'cta_text': None,
            'cta_url': None,
        }
        
        # Extract content
        content_el = element.select_one('span[class*="content"], div[class*="text"]')
        if content_el:
            post['content'] = content_el.get_text(strip=True)
        
        # Extract media
        img_el = element.select_one('img[src*="googleusercontent"]')
        if img_el:
            post['media_url'] = img_el.get('src')
            post['media_type'] = 'image'
        
        video_el = element.select_one('video')
        if video_el:
            post['media_url'] = video_el.get('src')
            post['media_type'] = 'video'
        
        # Extract date
        date_el = element.select_one('span[class*="date"], time')
        if date_el:
            post['date'] = date_el.get_text(strip=True)
        
        # Extract CTA button
        cta_el = element.select_one('a[class*="cta"], button[class*="action"]')
        if cta_el:
            post['cta_text'] = cta_el.get_text(strip=True)
            post['cta_url'] = cta_el.get('href')
        
        # Determine post type
        content_lower = (post['content'] or '').lower()
        if 'offer' in content_lower or 'discount' in content_lower:
            post['post_type'] = 'OFFER'
        elif 'event' in content_lower:
            post['post_type'] = 'EVENT'
        
        return post if post['content'] or post['media_url'] else None
    
    def extract_menu_items(self) -> List[Dict[str, Any]]:
        """Extract menu items for restaurants."""
        items = []
        
        if not self.soup:
            return items
        
        # Find menu section
        menu_section = self.soup.select_one('div[aria-label*="Menu"]')
        if not menu_section:
            return items
        
        # Find menu items
        menu_elements = menu_section.select('div[class*="item"]')
        
        for element in menu_elements[:20]:  # Limit
            item = {
                'name': None,
                'description': None,
                'price': None,
                'category': None,
                'image_url': None,
            }
            
            name_el = element.select_one('h3, h4, span[class*="name"]')
            if name_el:
                item['name'] = name_el.get_text(strip=True)
            
            price_el = element.select_one('span[class*="price"]')
            if price_el:
                item['price'] = price_el.get_text(strip=True)
            
            desc_el = element.select_one('span[class*="description"]')
            if desc_el:
                item['description'] = desc_el.get_text(strip=True)
            
            img_el = element.select_one('img')
            if img_el:
                item['image_url'] = img_el.get('src')
            
            if item['name']:
                items.append(item)
        
        return items
    
    def extract_services(self) -> List[Dict[str, Any]]:
        """Extract services for service businesses."""
        services = []
        
        if not self.soup:
            return services
        
        # Find services section
        services_section = self.soup.select_one('div[aria-label*="Services"]')
        if not services_section:
            return services
        
        # Find service items
        service_elements = services_section.select('div[class*="service"], li')
        
        for element in service_elements[:20]:
            service = {
                'name': None,
                'description': None,
                'price': None,
                'duration': None,
            }
            
            name_el = element.select_one('span[class*="name"], h4')
            if name_el:
                service['name'] = name_el.get_text(strip=True)
            
            price_el = element.select_one('span[class*="price"]')
            if price_el:
                service['price'] = price_el.get_text(strip=True)
            
            if service['name']:
                services.append(service)
        
        return services
    
    def extract_products(self) -> List[Dict[str, Any]]:
        """Extract products for retail businesses."""
        products = []
        
        if not self.soup:
            return products
        
        # Find products section
        products_section = self.soup.select_one('div[aria-label*="Products"]')
        if not products_section:
            return products
        
        # Find product items
        product_elements = products_section.select('div[class*="product"]')
        
        for element in product_elements[:20]:
            product = {
                'name': None,
                'description': None,
                'price': None,
                'category': None,
                'image_url': None,
                'in_stock': True,
            }
            
            name_el = element.select_one('span[class*="name"], h4')
            if name_el:
                product['name'] = name_el.get_text(strip=True)
            
            price_el = element.select_one('span[class*="price"]')
            if price_el:
                product['price'] = price_el.get_text(strip=True)
            
            img_el = element.select_one('img')
            if img_el:
                product['image_url'] = img_el.get('src')
            
            # Check stock status
            if element.select_one('[class*="out-of-stock"]'):
                product['in_stock'] = False
            
            if product['name']:
                products.append(product)
        
        return products
    
    def extract_price_range(self) -> Optional[str]:
        """Extract price range indicator ($, $$, $$$, $$$$)."""
        if not self.soup:
            return None
        
        # Look for price indicator near category
        price_patterns = [r'(\${1,4})', r'(Inexpensive|Moderate|Expensive|Very Expensive)']
        
        # Check aria-labels
        for element in self.soup.select('[aria-label]'):
            label = element.get('aria-label', '')
            for pattern in price_patterns:
                match = re.search(pattern, label)
                if match:
                    return match.group(1)
        
        # Check text content
        page_text = self.soup.get_text()
        for price_level in self.PRICE_LEVELS:
            if f' {price_level} ' in page_text or f'·{price_level}·' in page_text:
                return price_level
        
        return None
    
    def extract_booking_links(self) -> List[Dict[str, str]]:
        """Extract booking/reservation platform links."""
        links = []
        
        if not self.soup:
            return links
        
        booking_platforms = [
            'opentable', 'resy', 'bookatable', 'yelp', 'tock',
            'appointy', 'calendly', 'acuity', 'square'
        ]
        
        all_links = self.soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').lower()
            for platform in booking_platforms:
                if platform in href:
                    links.append({
                        'provider': platform,
                        'url': link.get('href')
                    })
                    break
        
        return links
    
    def extract_order_links(self) -> List[Dict[str, str]]:
        """Extract online ordering platform links."""
        links = []
        
        if not self.soup:
            return links
        
        order_platforms = [
            'doordash', 'ubereats', 'grubhub', 'postmates',
            'seamless', 'caviar', 'slice', 'chownow'
        ]
        
        all_links = self.soup.find_all('a', href=True)
        
        for link in all_links:
            href = link.get('href', '').lower()
            for platform in order_platforms:
                if platform in href:
                    links.append({
                        'provider': platform,
                        'url': link.get('href')
                    })
                    break
        
        return links
    
    def extract_years_in_business(self) -> Optional[int]:
        """Extract how long business has been operating."""
        if not self.soup:
            return None
        
        # Look for "In business since" or "Years in business"
        patterns = [
            r'(\d+)\s*years?\s*in\s*business',
            r'since\s*(\d{4})',
            r'established\s*(\d{4})',
            r'founded\s*(\d{4})',
        ]
        
        page_text = self.soup.get_text()
        
        for pattern in patterns:
            match = re.search(pattern, page_text, re.IGNORECASE)
            if match:
                value = int(match.group(1))
                if value > 1900:  # It's a year
                    from datetime import datetime
                    return datetime.now().year - value
                return value
        
        return None
    
    def extract_eco_certifications(self) -> List[str]:
        """Extract environmental/sustainability certifications."""
        certs = []
        
        if not self.soup:
            return certs
        
        eco_keywords = [
            'sustainable', 'eco-friendly', 'green certified',
            'carbon neutral', 'organic certified', 'farm-to-table',
            'locally sourced', 'recycling', 'renewable energy'
        ]
        
        # Check attributes section
        for keyword in eco_keywords:
            if self.find_element_by_text(keyword):
                certs.append(keyword.title())
        
        return list(set(certs))
