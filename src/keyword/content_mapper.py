"""
Content Mapper for Keyword Research
Maps keyword clusters to appropriate content types and pages.
"""

import logging
from typing import Optional, List, Dict
from collections import defaultdict

from .ai_service import GeminiKeywordAI

logger = logging.getLogger(__name__)


# Content type definitions with matching criteria
CONTENT_TYPES = {
    'service_page': {
        'name': 'Service Page',
        'description': 'Dedicated page for a specific service offering',
        'intents': ['transactional', 'commercial', 'local'],
        'patterns': ['services', 'hire', 'professional', 'expert', 'agency', 'company'],
        'priority': 1
    },
    'product_page': {
        'name': 'Product Page',
        'description': 'Page showcasing a specific product',
        'intents': ['transactional'],
        'patterns': ['buy', 'price', 'cost', 'shop', 'order', 'purchase'],
        'priority': 1
    },
    'location_page': {
        'name': 'Location Page',
        'description': 'Page targeting a specific geographic area',
        'intents': ['local'],
        'patterns': ['near me', 'in [city]', 'location', 'area', 'region'],
        'priority': 2
    },
    'comparison_page': {
        'name': 'Comparison Page',
        'description': 'Page comparing options or alternatives',
        'intents': ['commercial', 'comparison'],
        'patterns': ['vs', 'versus', 'compare', 'comparison', 'difference', 'better'],
        'priority': 2
    },
    'guide_article': {
        'name': 'Comprehensive Guide',
        'description': 'In-depth guide or tutorial article',
        'intents': ['informational'],
        'patterns': ['how to', 'guide', 'tutorial', 'step by step', 'complete', 'ultimate'],
        'priority': 3
    },
    'blog_post': {
        'name': 'Blog Post',
        'description': 'Standard blog article for informational keywords',
        'intents': ['informational', 'question'],
        'patterns': ['what is', 'why', 'when', 'tips', 'ideas', 'examples'],
        'priority': 4
    },
    'faq_page': {
        'name': 'FAQ Page',
        'description': 'Frequently asked questions page',
        'intents': ['question', 'informational'],
        'patterns': ['?', 'questions', 'faq', 'answers'],
        'priority': 4
    },
    'landing_page': {
        'name': 'Landing Page',
        'description': 'Conversion-focused landing page',
        'intents': ['transactional', 'commercial'],
        'patterns': ['free', 'trial', 'demo', 'quote', 'consultation', 'contact'],
        'priority': 2
    }
}

# Intent classification patterns - analyze keyword signals directly
INTENT_PATTERNS = {
    'transactional': {
        'signals': [
            'cost', 'price', 'pricing', 'fee', 'fees', 'rate', 'rates',
            'buy', 'purchase', 'order', 'book', 'schedule', 'appointment',
            'hire', 'rent', 'lease', 'treatment', 'procedure', 'surgery',
            'service', 'services', 'clinic', 'doctor', 'dentist', 'specialist'
        ],
        'weight': 40
    },
    'local': {
        'signals': [
            'near me', 'nearby', 'in my area', 'local', 'closest',
            # City/state patterns added dynamically
        ],
        'patterns': [
            r'\b(near me|nearby)\b',
            r'\bin\s+[A-Z][a-z]+',  # "in Boston", "in Miami"
            r'\b[A-Z][a-z]+,?\s*(MA|CA|NY|TX|FL|IL|PA|OH|GA|NC|MI|NJ|VA|WA|AZ|CO|TN|MO|MD|WI|MN|IN|SC)\b'
        ],
        'weight': 35
    },
    'commercial': {
        'signals': [
            'best', 'top', 'review', 'reviews', 'vs', 'versus', 'compare',
            'comparison', 'alternative', 'alternatives', 'recommended',
            'pros and cons', 'benefits', 'advantages'
        ],
        'weight': 30
    },
    'informational': {
        'signals': [
            'what is', 'how to', 'why', 'when', 'guide', 'tips',
            'tutorial', 'learn', 'understand', 'definition', 'meaning',
            'explained', 'overview', 'introduction', 'basics', 'faq'
        ],
        'weight': 20
    }
}

class ContentMapper:
    """
    Maps keyword clusters to content types and pages.
    
    Features:
    - Determine optimal content type for each keyword cluster
    - Check existing pages for coverage
    - Identify content gaps
    - Prioritize new content creation
    - Generate content briefs
    """
    
    def __init__(self, ai_service: Optional[GeminiKeywordAI] = None):
        """
        Initialize the content mapper.
        
        Args:
            ai_service: Optional GeminiKeywordAI for enhanced mapping
        """
        self.ai_service = ai_service or GeminiKeywordAI()
        self.content_types = CONTENT_TYPES
        self.intent_patterns = INTENT_PATTERNS
    
    def _classify_cluster_intent(self, keywords: List[str]) -> Dict:
        """
        Classify intent by analyzing keyword signals directly.
        Returns primary intent(s) with confidence score.
        
        Args:
            keywords: List of keywords to analyze
            
        Returns:
            Dict with 'primary', 'secondary', 'confidence', and 'signals_found'
        """
        import re
        
        combined_text = ' '.join([k.lower() if isinstance(k, str) else k.get('keyword', '').lower() for k in keywords])
        
        intent_scores = {}
        signals_found = {}
        
        for intent_type, config in self.intent_patterns.items():
            score = 0
            found_signals = []
            
            # Check signal words
            for signal in config.get('signals', []):
                if signal.lower() in combined_text:
                    score += config['weight']
                    found_signals.append(signal)
            
            # Check regex patterns (for local intent)
            for pattern in config.get('patterns', []):
                if re.search(pattern, combined_text, re.IGNORECASE):
                    score += config['weight']
                    found_signals.append(f"pattern:{pattern[:20]}")
            
            if score > 0:
                intent_scores[intent_type] = score
                signals_found[intent_type] = found_signals
        
        if not intent_scores:
            # Default to informational with moderate confidence
            return {
                'primary': 'informational',
                'secondary': None,
                'confidence': 50,
                'signals_found': {},
                'reason': 'No clear intent signals, defaulting to informational'
            }
        
        # Sort by score
        sorted_intents = sorted(intent_scores.items(), key=lambda x: x[1], reverse=True)
        primary_intent = sorted_intents[0][0]
        primary_score = sorted_intents[0][1]
        secondary_intent = sorted_intents[1][0] if len(sorted_intents) > 1 else None
        
        # Calculate confidence (scale score to 0-100)
        # Base confidence starts at 50, add based on signal strength
        confidence = min(95, 50 + primary_score)
        
        # Boost confidence for strong commercial signals
        if primary_intent in ['transactional', 'local'] and primary_score >= 40:
            confidence = min(95, confidence + 15)
        
        return {
            'primary': primary_intent,
            'secondary': secondary_intent,
            'confidence': confidence,
            'signals_found': signals_found,
            'reason': f"Found {len(signals_found.get(primary_intent, []))} {primary_intent} signals"
        }
    
    def _detect_content_gaps(self, topic: str, keywords: List[str], intent: str) -> List[Dict]:
        """
        Detect content gaps and suggest supporting content.
        
        Args:
            topic: Cluster topic
            keywords: Keywords in cluster
            intent: Primary intent
            
        Returns:
            List of content gap suggestions
        """
        gaps = []
        topic_clean = topic.lower().strip()
        
        # FAQ gaps - always useful for service topics
        if intent in ['transactional', 'commercial', 'local']:
            gaps.append({
                'type': 'faq',
                'title': f'{topic} FAQ',
                'suggested_questions': [
                    f'How much does {topic_clean} cost?',
                    f'How long does {topic_clean} take?',
                    f'Is {topic_clean} painful?',
                    f'What is the recovery time for {topic_clean}?'
                ],
                'priority': 'high'
            })
        
        # Cost/pricing gap
        has_cost_keyword = any('cost' in str(k).lower() or 'price' in str(k).lower() for k in keywords)
        if intent == 'transactional' and not has_cost_keyword:
            gaps.append({
                'type': 'blog',
                'title': f'{topic} Cost Guide',
                'description': f'Comprehensive guide to {topic_clean} pricing and factors',
                'priority': 'high'
            })
        
        # Comparison gap
        if intent == 'commercial':
            gaps.append({
                'type': 'comparison',
                'title': f'{topic} vs Alternatives',
                'description': f'Compare {topic_clean} with other options',
                'priority': 'medium'
            })
        
        # How-to / Guide gap
        if intent == 'informational':
            gaps.append({
                'type': 'guide',
                'title': f'Complete Guide to {topic}',
                'description': f'Everything you need to know about {topic_clean}',
                'priority': 'medium'
            })
        
        # Before/After or Results gap for medical/aesthetic services
        service_keywords = ['implant', 'whitening', 'braces', 'surgery', 'treatment', 'procedure']
        if any(sk in topic_clean for sk in service_keywords):
            gaps.append({
                'type': 'case_study',
                'title': f'{topic} Before & After',
                'description': f'Real patient results for {topic_clean}',
                'priority': 'medium'
            })
        
        return gaps


    def classify_content_type(self, keyword: str, intent: str = None, cluster_keywords: List[str] = None) -> Dict:
        """
        Determine the best content type for a keyword.
        
        Args:
            keyword: Primary keyword
            intent: Pre-classified intent (if available)
            cluster_keywords: Other keywords in the cluster
            
        Returns:
            Content type recommendation with confidence
        """
        keyword_lower = keyword.lower()
        all_keywords = [keyword_lower]
        if cluster_keywords:
            all_keywords.extend([k.lower() for k in cluster_keywords])
        
        combined_text = ' '.join(all_keywords)
        
        scores = {}
        
        for type_id, type_info in self.content_types.items():
            score = 0
            
            # Intent matching (highest weight) - increased from 50 to 60
            if intent and intent.lower() in type_info['intents']:
                score += 60
            
            # Pattern matching - increased from 30 to 35
            pattern_matches = 0
            for pattern in type_info['patterns']:
                if pattern in combined_text:
                    score += 35
                    pattern_matches += 1
            
            # Bonus for multiple pattern matches
            if pattern_matches >= 2:
                score += 15
            
            # Adjust by priority (lower priority = slightly lower score)
            score -= type_info['priority'] * 3  # Reduced penalty from 5 to 3
            
            if score > 0:
                scores[type_id] = score
        
        if not scores:
            # Default to blog post for unclassified keywords - with higher base confidence
            return {
                'content_type': 'blog_post',
                'content_type_name': CONTENT_TYPES['blog_post']['name'],
                'confidence': 55,  # Increased from 50
                'reason': 'Default content type for general keywords'
            }
        
        # Get best match
        best_type = max(scores, key=scores.get)
        max_score = scores[best_type]
        
        # Calculate confidence with base of 40 (not 0)
        # Base 40 + scaled score, capped at 95
        base_confidence = 40
        score_bonus = min(55, max_score * 0.8)  # Scale score to add up to 55 points
        confidence = min(95, base_confidence + score_bonus)
        
        # Ensure minimum confidence of 50 for any matched type
        confidence = max(50, confidence)
        
        return {
            'content_type': best_type,
            'content_type_name': self.content_types[best_type]['name'],
            'description': self.content_types[best_type]['description'],
            'confidence': int(confidence),
            'alternative_types': [
                t for t, s in sorted(scores.items(), key=lambda x: x[1], reverse=True)[1:3]
            ]
        }
    
    def map_clusters_to_content(
        self,
        clusters: List[Dict],
        intent_data: Dict = None
    ) -> List[Dict]:
        """
        Map keyword clusters to content types.
        
        Args:
            clusters: List of cluster dicts with 'topic' and 'keywords'
            intent_data: Optional dict mapping keywords to intents
            
        Returns:
            List of content mapping recommendations
        """
        mappings = []
        
        for cluster in clusters:
            topic = cluster.get('topic', '')
            keywords = cluster.get('keywords', [])
            
            if not keywords:
                continue
            
            # Get primary keyword
            primary_keyword = keywords[0] if isinstance(keywords[0], str) else keywords[0].get('keyword', '')
            
            # Try to get intent from intent_data first
            intent = None
            intent_confidence = 50
            intent_reason = ''
            
            if intent_data:
                # Check each intent category
                for intent_type, intent_keywords in intent_data.items():
                    kw_list = [k.lower() if isinstance(k, str) else k for k in intent_keywords]
                    if primary_keyword.lower() in kw_list:
                        intent = intent_type
                        intent_confidence = 70
                        intent_reason = 'Matched from intent_data'
                        break
            
            # SMART INTENT CLASSIFICATION - use if lookup failed
            if not intent or intent == 'unknown':
                # Use our new smart classification method
                intent_result = self._classify_cluster_intent(keywords)
                intent = intent_result['primary']
                intent_confidence = intent_result['confidence']
                intent_reason = intent_result['reason']
            
            # Classify content type
            content_info = self.classify_content_type(
                primary_keyword,
                intent=intent,
                cluster_keywords=keywords[1:] if len(keywords) > 1 else None
            )
            
            # Use higher of content confidence and intent confidence
            final_confidence = max(content_info['confidence'], intent_confidence)
            
            # Calculate cluster value (based on keyword count and diversity)
            cluster_value = len(keywords) * 10
            if intent in ['transactional', 'commercial', 'local']:
                cluster_value *= 1.5  # Higher value for commercial intent
            
            # Detect content gaps for this cluster
            content_gaps = self._detect_content_gaps(topic or primary_keyword, keywords, intent)
            
            # Format secondary keywords properly
            secondary_kws = []
            for kw in keywords[1:5] if len(keywords) > 1 else []:
                if isinstance(kw, str):
                    secondary_kws.append(kw)
                elif isinstance(kw, dict):
                    secondary_kws.append(kw.get('keyword', str(kw)))
            
            mappings.append({
                'cluster_topic': topic or primary_keyword.title(),
                'primary_keyword': primary_keyword,
                'secondary_keywords': secondary_kws,
                'total_keywords': len(keywords),
                'intent': intent,
                'intent_confidence': intent_confidence,
                'intent_reason': intent_reason,
                'recommended_content_type': content_info['content_type'],
                'content_type_name': content_info['content_type_name'],
                'content_description': content_info.get('description', ''),
                'confidence': final_confidence,
                'alternative_types': content_info.get('alternative_types', []),
                'cluster_value': round(cluster_value),
                'content_gaps': content_gaps,
                'status': 'new'  # Will be updated if existing page found
            })
        
        # Sort by cluster value
        mappings.sort(key=lambda x: x['cluster_value'], reverse=True)
        
        return mappings

    
    def check_existing_coverage(
        self,
        mappings: List[Dict],
        existing_pages: Dict[str, Dict]
    ) -> List[Dict]:
        """
        Check which keywords already have page coverage.
        
        Args:
            mappings: Content mappings from map_clusters_to_content
            existing_pages: Dict of URL -> page info (with 'keywords', 'title')
            
        Returns:
            Updated mappings with coverage status
        """
        # Build keyword-to-URL index from existing pages
        keyword_urls = {}
        for url, page_data in existing_pages.items():
            for kw in page_data.get('keywords', []):
                kw_text = kw.get('keyword', kw) if isinstance(kw, dict) else kw
                keyword_urls[kw_text.lower()] = {
                    'url': url,
                    'title': page_data.get('title', ''),
                    'density': kw.get('density', 0) if isinstance(kw, dict) else 0
                }
        
        for mapping in mappings:
            primary = mapping['primary_keyword'].lower()
            
            if primary in keyword_urls:
                mapping['status'] = 'covered'
                mapping['existing_page'] = keyword_urls[primary]
            else:
                # Check secondary keywords
                covered_secondary = []
                for sec in mapping.get('secondary_keywords', []):
                    sec_lower = sec.lower() if isinstance(sec, str) else sec.get('keyword', '').lower()
                    if sec_lower in keyword_urls:
                        covered_secondary.append({
                            'keyword': sec_lower,
                            'page': keyword_urls[sec_lower]
                        })
                
                if covered_secondary:
                    mapping['status'] = 'partial'
                    mapping['partial_coverage'] = covered_secondary
                else:
                    mapping['status'] = 'gap'
        
        return mappings
    
    def prioritize_content_creation(
        self,
        mappings: List[Dict],
        business_goal: str = 'traffic'
    ) -> List[Dict]:
        """
        Prioritize which content to create first.
        
        Args:
            mappings: Content mappings with coverage status
            business_goal: 'traffic', 'conversions', or 'authority'
            
        Returns:
            Prioritized list of content to create
        """
        # Filter to gaps only
        gaps = [m for m in mappings if m['status'] == 'gap']
        
        # Score based on business goal
        for gap in gaps:
            priority_score = gap['cluster_value']
            
            if business_goal == 'conversions':
                # Prioritize transactional/commercial content
                if gap['intent'] in ['transactional', 'commercial']:
                    priority_score *= 2
                if gap['recommended_content_type'] in ['service_page', 'product_page', 'landing_page']:
                    priority_score *= 1.5
                    
            elif business_goal == 'traffic':
                # Prioritize high-volume broad topics
                priority_score *= gap['total_keywords'] / 5
                if gap['recommended_content_type'] in ['guide_article', 'blog_post']:
                    priority_score *= 1.3
                    
            elif business_goal == 'authority':
                # Prioritize comprehensive guides
                if gap['recommended_content_type'] in ['guide_article', 'comparison_page']:
                    priority_score *= 1.5
            
            gap['priority_score'] = round(priority_score)
            gap['priority_tier'] = self._get_priority_tier(priority_score, len(gaps))
        
        # Sort by priority score
        gaps.sort(key=lambda x: x['priority_score'], reverse=True)
        
        return gaps
    
    def _get_priority_tier(self, score: float, total_gaps: int) -> str:
        """Assign priority tier based on relative score."""
        if total_gaps == 0:
            return 'none'
        
        # Top 20% = A, 20-50% = B, rest = C
        if score >= 80:
            return 'A'
        elif score >= 40:
            return 'B'
        else:
            return 'C'
    
    def generate_content_brief(self, mapping: Dict) -> Dict:
        """
        Generate a content brief for a keyword cluster.
        
        Args:
            mapping: Content mapping dict
            
        Returns:
            Content brief with structure and recommendations
        """
        content_type = mapping['recommended_content_type']
        primary = mapping['primary_keyword']
        secondary = mapping.get('secondary_keywords', [])
        
        # Base structure by content type
        structures = {
            'service_page': {
                'sections': [
                    'Hero with CTA',
                    'Service Overview',
                    'Key Benefits',
                    'How It Works',
                    'Pricing/Packages (optional)',
                    'Testimonials',
                    'FAQ',
                    'Contact/CTA'
                ],
                'word_count': '800-1500',
                'cta_focus': 'Contact/Quote/Book'
            },
            'product_page': {
                'sections': [
                    'Product Title & Images',
                    'Key Features',
                    'Specifications',
                    'Pricing',
                    'Customer Reviews',
                    'Related Products'
                ],
                'word_count': '500-1000',
                'cta_focus': 'Add to Cart/Buy Now'
            },
            'guide_article': {
                'sections': [
                    'Introduction & Hook',
                    'What/Why Overview',
                    'Step-by-Step Process',
                    'Tips & Best Practices',
                    'Common Mistakes',
                    'Tools/Resources',
                    'FAQ',
                    'Conclusion & Next Steps'
                ],
                'word_count': '2000-4000',
                'cta_focus': 'Newsletter/Related Content'
            },
            'blog_post': {
                'sections': [
                    'Introduction',
                    'Main Content (3-5 sections)',
                    'Examples/Case Studies',
                    'Key Takeaways',
                    'Conclusion'
                ],
                'word_count': '1000-2000',
                'cta_focus': 'Related Posts/Newsletter'
            },
            'comparison_page': {
                'sections': [
                    'Overview of Options',
                    'Comparison Table',
                    'Detailed Comparison',
                    'Pros & Cons',
                    'Verdict/Recommendation',
                    'FAQ'
                ],
                'word_count': '1500-3000',
                'cta_focus': 'Recommended Option CTA'
            },
            'location_page': {
                'sections': [
                    'Local Service Overview',
                    'Service Areas',
                    'Local Testimonials',
                    'Google Map Embed',
                    'Contact Info',
                    'Local FAQ'
                ],
                'word_count': '800-1500',
                'cta_focus': 'Local Contact/Directions'
            },
            'faq_page': {
                'sections': [
                    '10-20 Q&A Items',
                    'Categories if needed',
                    'Schema Markup'
                ],
                'word_count': '1000-2000',
                'cta_focus': 'Contact for More Questions'
            },
            'landing_page': {
                'sections': [
                    'Hero with Value Prop',
                    'Pain Points',
                    'Solution/Benefits',
                    'Social Proof',
                    'CTA Section',
                    'FAQ'
                ],
                'word_count': '500-1000',
                'cta_focus': 'Single Clear CTA'
            }
        }
        
        structure = structures.get(content_type, structures['blog_post'])
        
        return {
            'title_suggestions': [
                f"{primary.title()}",
                f"Complete Guide to {primary.title()}",
                f"Best {primary.title()} - Expert Tips"
            ],
            'primary_keyword': primary,
            'secondary_keywords': secondary,
            'content_type': mapping['content_type_name'],
            'recommended_structure': structure['sections'],
            'target_word_count': structure['word_count'],
            'primary_cta': structure['cta_focus'],
            'seo_checklist': [
                f"Include '{primary}' in title, H1, and meta description",
                f"Use '{primary}' naturally in first 100 words",
                "Include secondary keywords in H2 subheadings",
                "Add internal links to related content",
                "Optimize images with alt text",
                "Add schema markup appropriate for content type"
            ],
            'intent': mapping.get('intent', 'informational')
        }
    
    def create_content_calendar(
        self,
        prioritized_gaps: List[Dict],
        posts_per_week: int = 2
    ) -> List[Dict]:
        """
        Create a content calendar from prioritized gaps.
        
        Args:
            prioritized_gaps: Prioritized content gaps
            posts_per_week: Target posts per week
            
        Returns:
            Content calendar entries
        """
        calendar = []
        week = 1
        post_in_week = 0
        
        for gap in prioritized_gaps:
            if post_in_week >= posts_per_week:
                week += 1
                post_in_week = 0
            
            calendar.append({
                'week': week,
                'position': post_in_week + 1,
                'topic': gap['cluster_topic'],
                'primary_keyword': gap['primary_keyword'],
                'content_type': gap['content_type_name'],
                'priority_tier': gap.get('priority_tier', 'B'),
                'estimated_effort': self._estimate_effort(gap['recommended_content_type'])
            })
            
            post_in_week += 1
        
        return calendar
    
    def _estimate_effort(self, content_type: str) -> str:
        """Estimate effort for content type."""
        high_effort = ['guide_article', 'comparison_page']
        medium_effort = ['service_page', 'product_page', 'landing_page']
        
        if content_type in high_effort:
            return 'High (4-8 hours)'
        elif content_type in medium_effort:
            return 'Medium (2-4 hours)'
        else:
            return 'Low (1-2 hours)'
