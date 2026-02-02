"""
GMB Crawler V2 - Type Definitions

Complete TypedDict definitions for all data structures extracted from Google Maps.
Covers all 10+ attribute categories for comprehensive data extraction.
"""

from typing import TypedDict, List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


# ==================== 1. Basic Details ====================

class BasicDetails(TypedDict, total=False):
    """Core business identification data."""
    title: str                      # Business name
    cid: str                        # Customer ID (ludocid)
    place_id: str                   # Google Place ID
    primary_category: str           # Main business category
    subcategories: List[str]        # Additional categories
    description: str                # Business description (from About)
    is_claimed: bool                # Whether business is claimed/verified
    claim_status: str               # 'CLAIMED', 'UNCLAIMED', 'UNKNOWN'


# ==================== 2. Location Data ====================

class AddressComponents(TypedDict, total=False):
    """Parsed address components."""
    street_number: str
    street_name: str
    street_address: str             # Full street (number + name)
    city: str
    state: str                      # State/Province/Region
    postal_code: str
    country: str
    country_code: str               # ISO code (e.g., "US", "GB")
    neighborhood: str               # Borough/Neighborhood
    sublocality: str


class LocationData(TypedDict, total=False):
    """Geographic and address information."""
    full_address: str               # Complete formatted address
    address_components: AddressComponents
    latitude: float
    longitude: float
    plus_code: str                  # Open Location Code
    google_maps_url: str            # Direct link to place


# ==================== 3. Contact Information ====================

class SocialMediaLink(TypedDict, total=False):
    """Social media profile link."""
    platform: str                   # 'facebook', 'instagram', 'twitter', etc.
    url: str
    handle: str                     # @username if extractable


class ContactInfo(TypedDict, total=False):
    """Business contact details."""
    primary_phone: str
    additional_phones: List[str]
    primary_email: str
    additional_emails: List[str]
    website_url: str
    domain: str                     # Extracted from website URL
    social_media: List[SocialMediaLink]
    menu_url: str                   # For restaurants
    order_url: str                  # Online ordering link
    reservation_url: str            # Booking/reservation link


# ==================== 4. Media Assets ====================

class PhotoDetails(TypedDict, total=False):
    """Detailed photo information."""
    photo_id: str
    url: str
    high_res_url: str
    category: str                   # 'All', 'Latest', 'Food & drink', etc.
    uploaded_by: str                # 'owner' or 'user'
    upload_date: str


class MediaAssets(TypedDict, total=False):
    """Visual media from the business listing."""
    logo_url: str
    cover_image_url: str
    total_photo_count: int
    photo_urls: List[str]           # Sample photo URLs
    photo_details: List[PhotoDetails]
    video_count: int
    video_urls: List[str]
    street_view_available: bool
    street_view_url: str
    has_360_photos: bool
    photo_categories: List[str]     # Available photo filter categories


# ==================== 5. Ratings & Reviews ====================

class ReviewDetails(TypedDict, total=False):
    """Individual review data."""
    review_id: str
    author_name: str
    author_photo_url: str
    author_review_count: int        # Total reviews by this author
    author_is_local_guide: bool
    rating: float                   # 1-5
    date: str                       # Review date
    relative_date: str              # "2 weeks ago"
    text: str                       # Full review text
    language: str
    has_owner_response: bool
    owner_response_text: str
    owner_response_date: str
    photos: List[str]               # Photos attached to review


class RatingDistribution(TypedDict, total=False):
    """Star rating breakdown."""
    five_star: int
    four_star: int
    three_star: int
    two_star: int
    one_star: int


class ReviewsRatings(TypedDict, total=False):
    """Complete review and rating data."""
    overall_rating: float           # Average rating (1-5)
    total_reviews: int              # Total vote/review count
    rating_distribution: RatingDistribution
    review_summaries: List[str]     # Top keyword summaries
    place_topics: List[str]         # Common mentioned topics
    recent_reviews: List[ReviewDetails]
    reviews_per_rating: Dict[int, int]  # {5: 100, 4: 50, ...}


# ==================== 6. Business Attributes ====================

class ServiceOptions(TypedDict, total=False):
    """Dining/Service options."""
    dine_in: bool
    takeout: bool
    delivery: bool
    curbside_pickup: bool
    no_contact_delivery: bool
    outdoor_seating: bool
    drive_through: bool


class Accessibility(TypedDict, total=False):
    """Accessibility features."""
    wheelchair_accessible_entrance: bool
    wheelchair_accessible_seating: bool
    wheelchair_accessible_restroom: bool
    wheelchair_accessible_parking: bool


class Offerings(TypedDict, total=False):
    """Menu/Product offerings."""
    serves_alcohol: bool
    serves_beer: bool
    serves_cocktails: bool
    serves_wine: bool
    serves_coffee: bool
    serves_vegan: bool
    serves_vegetarian: bool
    halal_food: bool
    organic_dishes: bool
    small_plates: bool
    late_night_food: bool
    happy_hour: bool


class DiningOptions(TypedDict, total=False):
    """Meal service options."""
    serves_breakfast: bool
    serves_brunch: bool
    serves_lunch: bool
    serves_dinner: bool
    serves_dessert: bool
    has_seating: bool
    has_catering: bool
    counter_service: bool


class Amenities(TypedDict, total=False):
    """Facility amenities."""
    has_bar_onsite: bool
    has_restroom: bool
    has_wifi: bool
    free_wifi: bool
    dogs_allowed: bool
    live_music: bool
    live_performances: bool
    rooftop_seating: bool
    fireplace: bool
    outdoor_seating: bool
    private_dining: bool


class Atmosphere(TypedDict, total=False):
    """Venue atmosphere descriptors."""
    casual: bool
    cozy: bool
    upscale: bool
    trendy: bool
    romantic: bool
    historic: bool
    modern: bool


class Crowd(TypedDict, total=False):
    """Target audience indicators."""
    family_friendly: bool
    lgbtq_friendly: bool
    transgender_safe_space: bool
    good_for_groups: bool
    popular_with_tourists: bool
    college_crowd: bool


class Payments(TypedDict, total=False):
    """Payment methods accepted."""
    accepts_credit_cards: bool
    accepts_debit_cards: bool
    accepts_nfc: bool
    accepts_cash_only: bool
    accepts_checks: bool


class Planning(TypedDict, total=False):
    """Reservation/planning info."""
    accepts_reservations: bool
    reservations_required: bool
    walk_ins_welcome: bool
    appointment_required: bool


class Children(TypedDict, total=False):
    """Child-friendly features."""
    good_for_kids: bool
    has_kids_menu: bool
    has_high_chairs: bool
    has_changing_tables: bool


class BusinessAttributes(TypedDict, total=False):
    """Complete business attributes collection."""
    service_options: ServiceOptions
    accessibility: Accessibility
    offerings: Offerings
    dining_options: DiningOptions
    amenities: Amenities
    atmosphere: Atmosphere
    crowd: Crowd
    payments: Payments
    planning: Planning
    children: Children
    raw_attributes: List[str]       # Unparsed attribute strings


# ==================== 7. Operating Hours ====================

class DayHours(TypedDict, total=False):
    """Hours for a single day."""
    open: str                       # "09:00"
    close: str                      # "18:00"
    is_closed: bool
    is_24_hours: bool
    periods: List[Dict[str, str]]   # Multiple open/close periods


class OperatingHours(TypedDict, total=False):
    """Complete operating hours data."""
    monday: DayHours
    tuesday: DayHours
    wednesday: DayHours
    thursday: DayHours
    friday: DayHours
    saturday: DayHours
    sunday: DayHours
    current_status: str             # 'OPEN', 'CLOSED', 'CLOSING_SOON', etc.
    opens_at: str                   # Next opening time
    closes_at: str                  # Current day closing time
    is_24_hours: bool               # Always open
    is_temporarily_closed: bool
    is_permanently_closed: bool
    special_hours: List[Dict[str, Any]]  # Holiday hours


# ==================== 8. Popular Times ====================

class HourlyTraffic(TypedDict, total=False):
    """Hourly busyness data."""
    hour: int                       # 0-23
    popularity: int                 # 0-100 percentage
    label: str                      # 'Not busy', 'A little busy', etc.


class PopularTimes(TypedDict, total=False):
    """Traffic and busyness analysis."""
    monday: List[HourlyTraffic]
    tuesday: List[HourlyTraffic]
    wednesday: List[HourlyTraffic]
    thursday: List[HourlyTraffic]
    friday: List[HourlyTraffic]
    saturday: List[HourlyTraffic]
    sunday: List[HourlyTraffic]
    live_busyness: str              # "Usually not too busy"
    live_busyness_percent: int      # Real-time if available
    typical_time_spent: str         # "People typically spend 30 min to 1 hr"
    best_times_to_visit: List[str]  # Calculated low-traffic periods


# ==================== 9. Competitive Data ====================

class CompetitorInfo(TypedDict, total=False):
    """Data about a competitor business."""
    name: str
    place_id: str
    rating: float
    review_count: int
    category: str
    address: str
    distance: str                   # "0.5 mi" if available


class CompetitiveData(TypedDict, total=False):
    """Competition and related business data."""
    people_also_search: List[CompetitorInfo]
    similar_places: List[CompetitorInfo]
    nearby_businesses: List[CompetitorInfo]


# ==================== 10. Additional Data ====================

class QAEntry(TypedDict, total=False):
    """Question and answer entry."""
    question: str
    question_date: str
    question_author: str
    answer: str
    answer_date: str
    answer_author: str
    is_owner_answer: bool
    upvotes: int


class BusinessPost(TypedDict, total=False):
    """Business update/post."""
    post_id: str
    content: str
    media_url: str
    media_type: str                 # 'image', 'video'
    date: str
    post_type: str                  # 'UPDATE', 'OFFER', 'EVENT', 'PRODUCT'
    cta_text: str                   # Call to action button text
    cta_url: str


class MenuItem(TypedDict, total=False):
    """Menu item (for restaurants)."""
    name: str
    description: str
    price: str
    category: str                   # 'Appetizers', 'Main Course', etc.
    image_url: str


class ServiceItem(TypedDict, total=False):
    """Service offered (for service businesses)."""
    name: str
    description: str
    price: str
    duration: str


class ProductItem(TypedDict, total=False):
    """Product sold (for retail)."""
    name: str
    description: str
    price: str
    category: str
    image_url: str
    in_stock: bool


class AdditionalData(TypedDict, total=False):
    """Supplementary business data."""
    qa_count: int
    qa_entries: List[QAEntry]
    posts_count: int
    posts: List[BusinessPost]
    last_post_date: str
    menu_items: List[MenuItem]
    services: List[ServiceItem]
    products: List[ProductItem]
    price_range: str                # '$', '$$', '$$$', '$$$$'
    booking_links: List[Dict[str, str]]  # [{provider: url}]
    order_links: List[Dict[str, str]]
    years_in_business: int
    founded_year: int
    eco_certifications: List[str]


# ==================== Complete Business Data ====================

class GMBBusinessData(TypedDict, total=False):
    """
    Complete Google Maps Business Data.
    Contains all 10 attribute categories.
    """
    # Metadata
    extraction_id: str
    extraction_date: str
    extraction_source: str          # URL used for extraction
    extraction_duration_ms: int
    
    # Data Categories
    basic_details: BasicDetails
    location_data: LocationData
    contact_info: ContactInfo
    media_assets: MediaAssets
    reviews_ratings: ReviewsRatings
    business_attributes: BusinessAttributes
    operating_hours: OperatingHours
    popular_times: PopularTimes
    competitive_data: CompetitiveData
    additional_data: AdditionalData
    
    # Raw data for debugging
    raw_html: str                   # Original HTML (optional)
    parse_errors: List[str]         # Any extraction errors


# ==================== Extraction Request/Response ====================

class ExtractionRequest(TypedDict, total=False):
    """Request for data extraction."""
    url: str
    place_id: str
    categories: List[str]           # Which categories to extract
    include_raw_html: bool
    include_photos: bool
    max_reviews: int
    location_context: Dict[str, float]  # {lat, lng} for geo context


class ExtractionResponse(TypedDict, total=False):
    """Response from extraction."""
    success: bool
    data: GMBBusinessData
    errors: List[str]
    warnings: List[str]
    duration_ms: int
