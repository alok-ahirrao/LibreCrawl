"""
Enhanced GMB Health Calculator
Calculates profile completeness and health scores based on all available data.
"""
import json


def calculate_location_health(data):
    """
    Calculate health score (0-100) based on location data completeness and quality.
    
    Args:
        data: Dictionary containing location data from database
        
    Returns:
        dict: detailed health report with component scores
    """
    
    # 1. PROFILE SCORE (0-100) - Basic business info completeness
    profile_score = 0
    profile_missing = []
    
    if data.get('location_name') or data.get('name'): 
        profile_score += 15
    else:
        profile_missing.append('business name')
        
    if data.get('address_lines') or data.get('address'): 
        profile_score += 15
    else:
        profile_missing.append('address')
        
    if data.get('phone_number') or data.get('phone'): 
        profile_score += 15
    else:
        profile_missing.append('phone number')
        
    if data.get('website_url') or data.get('website'): 
        profile_score += 15
    else:
        profile_missing.append('website')
        
    if data.get('primary_category') or data.get('category'): 
        profile_score += 15
    else:
        profile_missing.append('category')
        
    if data.get('description'): 
        profile_score += 15
    else:
        profile_missing.append('description')
        
    # Business hours - check if hours data exists
    hours = data.get('business_hours')
    if hours:
        if isinstance(hours, str):
            try:
                hours = json.loads(hours)
            except:
                hours = None
        if hours and isinstance(hours, dict) and len(hours) >= 1:
            profile_score += 10
        else:
            profile_missing.append('business hours')
    else:
        profile_missing.append('business hours')
    
    profile_score = min(profile_score, 100)
    
    # 2. PHOTOS SCORE (0-100)
    photos = int(data.get('photo_count') or 0)
    if photos >= 25:
        photos_score = 100
    elif photos >= 15:
        photos_score = 80
    elif photos >= 10:
        photos_score = 60
    elif photos >= 5:
        photos_score = 40
    elif photos >= 1:
        photos_score = 20
    else:
        photos_score = 0
    
    # 3. REVIEWS SCORE (0-100)
    rating = float(data.get('rating') or 0)
    reviews = int(data.get('total_reviews') or data.get('review_count') or 0)
    reviews_score = 0
    
    # Rating component (up to 50 points)
    if rating >= 4.5:
        reviews_score += 50
    elif rating >= 4.0:
        reviews_score += 40
    elif rating >= 3.5:
        reviews_score += 25
    elif rating >= 3.0:
        reviews_score += 15
    elif rating > 0:
        reviews_score += 5
    
    # Review count component (up to 50 points)
    if reviews >= 100:
        reviews_score += 50
    elif reviews >= 50:
        reviews_score += 40
    elif reviews >= 20:
        reviews_score += 30
    elif reviews >= 10:
        reviews_score += 20
    elif reviews >= 5:
        reviews_score += 10
    elif reviews >= 1:
        reviews_score += 5
    
    reviews_score = min(reviews_score, 100)
    
    # 4. POSTS SCORE (0-100) - Based on post activity
    posts_count = int(data.get('post_count') or 0)
    last_post = data.get('last_post_date')
    
    posts_score = 0
    if posts_count >= 4:
        posts_score = 100
    elif posts_count >= 2:
        posts_score = 60
    elif posts_count >= 1:
        posts_score = 30
    
    # Boost if recent post
    if last_post and last_post == 'recent':
        posts_score = max(posts_score, 50)
    
    # 5. Q&A SCORE (0-100) - Based on Q&A engagement
    qa_count = int(data.get('qa_count') or 0)
    
    if qa_count >= 10:
        qa_score = 100
    elif qa_count >= 5:
        qa_score = 70
    elif qa_count >= 2:
        qa_score = 40
    elif qa_count >= 1:
        qa_score = 20
    else:
        qa_score = 0
    
    # 6. ATTRIBUTES SCORE (bonus) - check attributes completeness
    attributes = data.get('attributes')
    if attributes:
        if isinstance(attributes, str):
            try:
                attributes = json.loads(attributes)
            except:
                attributes = []
        if isinstance(attributes, list):
            attr_count = len(attributes)
            if attr_count >= 10:
                # Boost profile score for having many attributes
                profile_score = min(profile_score + 10, 100)
    
    # Calculate OVERALL score (weighted average)
    # Profile: 25%, Photos: 20%, Reviews: 35%, Posts: 10%, Q&A: 10%
    overall = int(
        (profile_score * 0.25) +
        (photos_score * 0.20) +
        (reviews_score * 0.35) +
        (posts_score * 0.10) +
        (qa_score * 0.10)
    )
    
    return {
        'scores': {
            'overall': overall,
            'profile': profile_score,
            'photos': photos_score,
            'reviews': reviews_score,
            'posts': posts_score,
            'qa': qa_score
        },
        'missing': profile_missing,
        'details': []
    }


def get_improvement_recommendations(data, scores):
    """
    Generate actionable improvement recommendations based on scores and data.
    
    Args:
        data: Location data dict
        scores: Scores dict from calculate_location_health
        
    Returns:
        List of recommendation dicts with priority and action
    """
    recommendations = []
    
    profile = scores.get('profile', 0)
    photos = scores.get('photos', 0)
    reviews = scores.get('reviews', 0)
    posts = scores.get('posts', 0)
    qa = scores.get('qa', 0)
    
    # High priority recommendations
    if profile < 70:
        if not data.get('business_hours'):
            recommendations.append({
                'priority': 'high',
                'category': 'profile',
                'action': 'Add business hours - profiles with hours get 25% more engagement'
            })
        if not data.get('description'):
            recommendations.append({
                'priority': 'high',
                'category': 'profile',
                'action': 'Add a business description (150-300 characters recommended)'
            })
    
    if reviews < 50:
        rating = float(data.get('rating') or 0)
        review_count = int(data.get('total_reviews') or data.get('review_count') or 0)
        
        if review_count < 20:
            recommendations.append({
                'priority': 'high',
                'category': 'reviews',
                'action': f'Increase reviews - currently {review_count}, aim for 20+ for better visibility'
            })
        if rating < 4.0 and rating > 0:
            recommendations.append({
                'priority': 'high',
                'category': 'reviews',
                'action': f'Improve rating from {rating:.1f} to 4.0+ by addressing customer feedback'
            })
    
    # Medium priority
    if photos < 60:
        photo_count = int(data.get('photo_count') or 0)
        recommendations.append({
            'priority': 'medium',
            'category': 'photos',
            'action': f'Add more photos - currently {photo_count}, aim for 15+ covering interior, exterior, and team'
        })
    
    if posts < 30:
        recommendations.append({
            'priority': 'medium',
            'category': 'posts',
            'action': 'Post updates regularly - aim for at least 1 post per week'
        })
    
    # Lower priority
    if qa < 30:
        recommendations.append({
            'priority': 'low',
            'category': 'qa',
            'action': 'Respond to customer questions to build trust and engagement'
        })
    
    # Check attributes
    attributes = data.get('attributes')
    if attributes:
        if isinstance(attributes, str):
            try:
                attributes = json.loads(attributes)
            except:
                attributes = []
        if isinstance(attributes, list) and len(attributes) < 5:
            recommendations.append({
                'priority': 'medium',
                'category': 'profile',
                'action': 'Add more service attributes (5+ recommended)'
            })
    else:
        recommendations.append({
            'priority': 'medium',
            'category': 'profile',
            'action': 'Add service attributes like Wi-Fi, parking, accessibility options'
        })
    
    if not recommendations:
        recommendations.append({
            'priority': 'info',
            'category': 'general',
            'action': 'Your profile is well-optimized! Keep monitoring for changes.'
        })
    
    return recommendations
