"""
Competitor Analyzer
Calculates deficits between user's business profile and competitor averages.
"""
import json
from math import ceil
from typing import List, Dict, Optional
from statistics import mean


class CompetitorAnalyzer:
    """
    Analyzes competitor data to identify gaps and generate actionable recommendations.
    """
    
    # Priority weights for different deficit types
    PRIORITY_WEIGHTS = {
        'reviews': 1.0,      # Highest impact on rankings
        'rating': 0.9,       # Critical for conversions
        'photos': 0.7,       # Visual appeal
        'categories': 0.6,   # Relevance signals
        'posts': 0.5,        # Freshness signals
        'q_and_a': 0.4,      # Engagement signals
        'attributes': 0.3    # Completeness
    }
    
    def __init__(self):
        pass
    
    def calculate_deficits(
        self, 
        user_profile: Dict, 
        competitors: List[Dict]
    ) -> List[Dict]:
        """
        Calculate what user needs to improve to match or exceed competitors.
        
        Args:
            user_profile: Dict with user's business profile data
            competitors: List of competitor profile dicts
            
        Returns:
            Sorted list of deficit dicts with type, priority, current, target, gap, action
        """
        if not competitors:
            return []
        
        deficits = []
        
        # Calculate competitor averages
        avg_reviews = self._safe_mean([c.get('review_count', 0) for c in competitors])
        avg_rating = self._safe_mean([c.get('rating', 0) for c in competitors if c.get('rating')])
        avg_photos = self._safe_mean([c.get('photo_count', 0) for c in competitors])
        avg_posts = self._safe_mean([c.get('post_count', 0) for c in competitors])
        avg_qna = self._safe_mean([c.get('q_and_a_count', 0) for c in competitors])
        
        # Get competitor maximums for stretch goals (filter out None)
        review_values = [c.get('review_count') for c in competitors]
        rating_values = [c.get('rating') for c in competitors]
        max_reviews = max([v for v in review_values if v is not None], default=0)
        max_rating = max([v for v in rating_values if v is not None], default=0)
        
        # Review deficit
        user_reviews = user_profile.get('review_count', 0) or 0
        if user_reviews < avg_reviews:
            gap = ceil(avg_reviews) - user_reviews
            deficits.append({
                'type': 'reviews',
                'priority': self._calculate_priority('reviews', gap, avg_reviews),
                'current': user_reviews,
                'target': ceil(avg_reviews),
                'stretch_target': max_reviews,
                'gap': gap,
                'action': f"Get {gap} more reviews to match competitor average",
                'impact': "Reviews are the #1 ranking factor for Local Pack"
            })
        
        # Rating deficit
        user_rating = user_profile.get('rating', 0) or 0
        if user_rating < avg_rating and avg_rating > 0:
            gap = round(avg_rating - user_rating, 1)
            deficits.append({
                'type': 'rating',
                'priority': self._calculate_priority('rating', gap * 10, 5),  # Scale gap
                'current': user_rating,
                'target': round(avg_rating, 1),
                'stretch_target': max_rating,
                'gap': gap,
                'action': f"Improve rating by {gap} stars (focus on customer satisfaction)",
                'impact': "Higher ratings improve click-through rates by 25%"
            })
        
        # Photo deficit
        user_photos = user_profile.get('photo_count', 0) or 0
        if user_photos < avg_photos:
            gap = ceil(avg_photos) - user_photos
            deficits.append({
                'type': 'photos',
                'priority': self._calculate_priority('photos', gap, avg_photos),
                'current': user_photos,
                'target': ceil(avg_photos),
                'gap': gap,
                'action': f"Add {gap} more photos (interior, exterior, team, products)",
                'impact': "Businesses with photos get 42% more direction requests"
            })
        
        # Category gaps
        category_deficit = self._analyze_category_gaps(user_profile, competitors)
        if category_deficit:
            deficits.append(category_deficit)
        
        # Post activity deficit
        user_posts = user_profile.get('post_count', 0) or 0
        if user_posts < avg_posts and avg_posts > 0:
            gap = ceil(avg_posts) - user_posts
            deficits.append({
                'type': 'posts',
                'priority': self._calculate_priority('posts', gap, avg_posts),
                'current': user_posts,
                'target': ceil(avg_posts),
                'gap': gap,
                'action': f"Create {gap} Google Posts this month (offers, events, updates)",
                'impact': "Regular posting signals an active, engaged business"
            })
        
        # Q&A deficit
        user_qna = user_profile.get('q_and_a_count', 0) or 0
        if user_qna < avg_qna and avg_qna > 0:
            gap = ceil(avg_qna) - user_qna
            deficits.append({
                'type': 'q_and_a',
                'priority': self._calculate_priority('q_and_a', gap, avg_qna),
                'current': user_qna,
                'target': ceil(avg_qna),
                'gap': gap,
                'action': f"Seed {gap} Q&As (common questions customers ask)",
                'impact': "Q&A improves engagement and provides keyword signals"
            })
        
        # Attribute gaps
        attribute_deficit = self._analyze_attribute_gaps(user_profile, competitors)
        if attribute_deficit:
            deficits.append(attribute_deficit)
        
        # Sort by priority (high = 0, medium = 1, low = 2)
        priority_order = {'high': 0, 'medium': 1, 'low': 2}
        return sorted(deficits, key=lambda x: priority_order.get(x['priority'], 3))
    
    def _analyze_category_gaps(
        self, 
        user_profile: Dict, 
        competitors: List[Dict]
    ) -> Optional[Dict]:
        """Analyze category usage gaps between user and competitors."""
        # Collect all competitor categories
        competitor_categories = set()
        for c in competitors:
            primary = c.get('primary_category')
            if primary:
                competitor_categories.add(primary)
            
            additional = c.get('additional_categories', [])
            if isinstance(additional, str):
                try:
                    additional = json.loads(additional)
                except:
                    additional = []
            competitor_categories.update(additional)
        
        # Get user categories
        user_categories = set()
        if user_profile.get('primary_category'):
            user_categories.add(user_profile['primary_category'])
        
        user_additional = user_profile.get('additional_categories', [])
        if isinstance(user_additional, str):
            try:
                user_additional = json.loads(user_additional)
            except:
                user_additional = []
        user_categories.update(user_additional)
        
        # Find missing categories
        missing = competitor_categories - user_categories
        
        if missing:
            missing_list = list(missing)[:5]  # Top 5 missing
            return {
                'type': 'categories',
                'priority': 'medium' if len(missing) >= 2 else 'low',
                'current': len(user_categories),
                'target': len(user_categories) + len(missing_list),
                'gap': len(missing),
                'missing': missing_list,
                'action': f"Add categories: {', '.join(missing_list)}",
                'impact': "Additional categories improve visibility for related searches"
            }
        
        return None
    
    def _analyze_attribute_gaps(
        self, 
        user_profile: Dict, 
        competitors: List[Dict]
    ) -> Optional[Dict]:
        """Analyze business attribute gaps."""
        # Collect competitor attributes
        competitor_attributes = set()
        for c in competitors:
            attrs = c.get('attributes', [])
            if isinstance(attrs, str):
                try:
                    attrs = json.loads(attrs)
                except:
                    attrs = []
            competitor_attributes.update(attrs)
        
        # Get user attributes
        user_attrs = user_profile.get('attributes', [])
        if isinstance(user_attrs, str):
            try:
                user_attrs = json.loads(user_attrs)
            except:
                user_attrs = []
        user_attributes = set(user_attrs)
        
        # Find missing attributes
        missing = competitor_attributes - user_attributes
        
        if missing:
            missing_list = list(missing)[:5]
            return {
                'type': 'attributes',
                'priority': 'low',
                'current': len(user_attributes),
                'target': len(user_attributes) + len(missing_list),
                'gap': len(missing),
                'missing': missing_list,
                'action': f"Enable attributes: {', '.join(missing_list)}",
                'impact': "Complete profiles rank higher and convert better"
            }
        
        return None
    
    def _calculate_priority(self, deficit_type: str, gap: float, baseline: float) -> str:
        """
        Calculate priority level based on gap severity and type weight.
        
        Returns: 'high', 'medium', or 'low'
        """
        if baseline == 0:
            return 'low'
        
        weight = self.PRIORITY_WEIGHTS.get(deficit_type, 0.5)
        gap_ratio = gap / baseline if baseline > 0 else 0
        
        score = weight * gap_ratio
        
        if score > 0.5:
            return 'high'
        elif score > 0.2:
            return 'medium'
        else:
            return 'low'
    
    def _safe_mean(self, values: List) -> float:
        """Calculate mean handling empty lists and None values."""
        valid = [v for v in values if v is not None]
        valid = [v for v in valid if isinstance(v, (int, float)) and v > 0]
        return mean(valid) if valid else 0
    
    def generate_comparison_matrix(
        self, 
        user_profile: Dict, 
        competitors: List[Dict]
    ) -> Dict:
        """
        Generate a comparison matrix for UI display.
        
        Returns:
            Dict with rows for each metric and columns for user + competitors
        """
        metrics = [
            {'key': 'review_count', 'label': 'Reviews', 'format': 'number'},
            {'key': 'rating', 'label': 'Rating', 'format': 'decimal'},
            {'key': 'photo_count', 'label': 'Photos', 'format': 'number'},
            {'key': 'post_count', 'label': 'Posts (30d)', 'format': 'number'},
            {'key': 'q_and_a_count', 'label': 'Q&A', 'format': 'number'},
            {'key': 'category_count', 'label': 'Categories', 'format': 'number'}
        ]
        
        rows = []
        
        for metric in metrics:
            key = metric['key']
            
            # Handle category count specially
            if key == 'category_count':
                user_val = self._count_categories(user_profile)
                comp_vals = [self._count_categories(c) for c in competitors]
            else:
                user_val = user_profile.get(key, 0) or 0
                comp_vals = [c.get(key, 0) or 0 for c in competitors]
            
            avg_comp = self._safe_mean(comp_vals)
            gap = user_val - avg_comp
            
            row = {
                'metric': metric['label'],
                'key': key,
                'format': metric['format'],
                'user_value': user_val,
                'competitor_values': comp_vals,
                'competitor_avg': round(avg_comp, 1),
                'gap': round(gap, 1),
                'status': 'winning' if gap >= 0 else 'losing'
            }
            rows.append(row)
        
        return {
            'metrics': rows,
            'user_name': user_profile.get('name', 'Your Business'),
            'competitor_names': [c.get('name', f'Competitor {i+1}') for i, c in enumerate(competitors)]
        }
    
    def _count_categories(self, profile: Dict) -> int:
        """Count total categories for a profile."""
        count = 1 if profile.get('primary_category') else 0
        
        additional = profile.get('additional_categories', [])
        if isinstance(additional, str):
            try:
                additional = json.loads(additional)
            except:
                additional = []
        
        return count + len(additional)
