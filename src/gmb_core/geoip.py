"""
IP-based Geolocation Utility
Uses ip-api.com free tier for IP to location resolution.
"""
import requests


def get_location_from_ip(ip_address: str) -> dict:
    """
    Resolve an IP address to geographic location using ip-api.com (free tier).
    
    Args:
        ip_address: The IP address to resolve (e.g., "8.8.8.8")
        
    Returns:
        Dictionary with location data:
        {
            'city': 'Mountain View',
            'region': 'California', 
            'country': 'United States',
            'country_code': 'US',
            'lat': 37.386,
            'lng': -122.0838,
            'timezone': 'America/Los_Angeles'
        }
        Returns None if resolution fails or IP is private/localhost.
    """
    # Skip localhost/private IPs - they can't be geolocated
    if not ip_address or ip_address in ('127.0.0.1', 'localhost', '::1'):
        print(f"[GeoIP] Skipping localhost/private IP: {ip_address}")
        return None
    
    # Check for private IP ranges
    if ip_address.startswith(('10.', '172.16.', '172.17.', '172.18.', '172.19.',
                               '172.20.', '172.21.', '172.22.', '172.23.', '172.24.',
                               '172.25.', '172.26.', '172.27.', '172.28.', '172.29.',
                               '172.30.', '172.31.', '192.168.')):
        print(f"[GeoIP] Skipping private IP: {ip_address}")
        return None
    
    try:
        # ip-api.com free tier - 45 requests/minute, no API key needed
        # Use HTTP (not HTTPS) for free tier
        url = f"http://ip-api.com/json/{ip_address}?fields=status,message,country,countryCode,region,regionName,city,lat,lon,timezone"
        
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('status') == 'success':
            result = {
                'city': data.get('city', ''),
                'region': data.get('regionName', ''),
                'country': data.get('country', ''),
                'country_code': data.get('countryCode', ''),
                'lat': data.get('lat'),
                'lng': data.get('lon'),
                'timezone': data.get('timezone', '')
            }
            print(f"[GeoIP] Resolved {ip_address} -> {result['city']}, {result['region']}, {result['country']}")
            return result
        else:
            print(f"[GeoIP] Failed to resolve {ip_address}: {data.get('message', 'Unknown error')}")
            return None
            
    except requests.RequestException as e:
        print(f"[GeoIP] Request error for {ip_address}: {e}")
        return None
    except Exception as e:
        print(f"[GeoIP] Unexpected error for {ip_address}: {e}")
        return None


def get_client_ip_from_request(request) -> str:
    """
    Extract the real client IP from a Flask request object.
    Handles proxy headers (X-Forwarded-For, X-Real-IP).
    
    Args:
        request: Flask request object
        
    Returns:
        Client IP address string
    """
    # Check for proxy headers (common with reverse proxies/load balancers)
    forwarded_for = request.headers.get('X-Forwarded-For')
    if forwarded_for:
        # X-Forwarded-For can contain multiple IPs: "client, proxy1, proxy2"
        # The first one is the original client
        return forwarded_for.split(',')[0].strip()
    
    # Check X-Real-IP (used by Nginx)
    real_ip = request.headers.get('X-Real-IP')
    if real_ip:
        return real_ip.strip()
    
    # Fall back to direct connection IP
    return request.remote_addr or ''


def geocode_location(location_name: str) -> dict:
    """
    Resolve a location name (e.g., 'Boston, MA') to coordinates.
    Uses a built-in cache for common locations and falls back to API if needed.
    
    Args:
        location_name: Location string to geocode (e.g., "Boston, MA", "Paris, France")
        
    Returns:
        Dictionary with location data:
        {
            'lat': 42.3601,
            'lng': -71.0589,
            'display_name': 'Boston, Massachusetts, United States',
            'country': 'United States',
            'country_code': 'us'
        }
        Returns None if geocoding fails.
    """
    if not location_name or not location_name.strip():
        return None
    
    # Built-in cache for common locations (avoids API calls for popular cities)
    LOCATION_CACHE = {
        # Boston Metro Area
        'boston': {'lat': 42.3601, 'lng': -71.0589, 'country': 'United States', 'country_code': 'us'},
        'boston, ma': {'lat': 42.3601, 'lng': -71.0589, 'country': 'United States', 'country_code': 'us'},
        'boston, massachusetts': {'lat': 42.3601, 'lng': -71.0589, 'country': 'United States', 'country_code': 'us'},
        'cambridge': {'lat': 42.3736, 'lng': -71.1097, 'country': 'United States', 'country_code': 'us'},
        'cambridge, ma': {'lat': 42.3736, 'lng': -71.1097, 'country': 'United States', 'country_code': 'us'},
        'brookline': {'lat': 42.3318, 'lng': -71.1212, 'country': 'United States', 'country_code': 'us'},
        'somerville': {'lat': 42.3876, 'lng': -71.0995, 'country': 'United States', 'country_code': 'us'},
        'newton': {'lat': 42.3370, 'lng': -71.2092, 'country': 'United States', 'country_code': 'us'},
        'quincy': {'lat': 42.2529, 'lng': -71.0023, 'country': 'United States', 'country_code': 'us'},
        'worcester': {'lat': 42.2626, 'lng': -71.8023, 'country': 'United States', 'country_code': 'us'},
        'springfield, ma': {'lat': 42.1015, 'lng': -72.5898, 'country': 'United States', 'country_code': 'us'},
        'lowell': {'lat': 42.6334, 'lng': -71.3162, 'country': 'United States', 'country_code': 'us'},
        'salem, ma': {'lat': 42.5195, 'lng': -70.8967, 'country': 'United States', 'country_code': 'us'},
        
        # Major US Cities
        'new york': {'lat': 40.7128, 'lng': -74.0060, 'country': 'United States', 'country_code': 'us'},
        'new york, ny': {'lat': 40.7128, 'lng': -74.0060, 'country': 'United States', 'country_code': 'us'},
        'manhattan': {'lat': 40.7831, 'lng': -73.9712, 'country': 'United States', 'country_code': 'us'},
        'brooklyn': {'lat': 40.6782, 'lng': -73.9442, 'country': 'United States', 'country_code': 'us'},
        'los angeles': {'lat': 34.0522, 'lng': -118.2437, 'country': 'United States', 'country_code': 'us'},
        'los angeles, ca': {'lat': 34.0522, 'lng': -118.2437, 'country': 'United States', 'country_code': 'us'},
        'chicago': {'lat': 41.8781, 'lng': -87.6298, 'country': 'United States', 'country_code': 'us'},
        'chicago, il': {'lat': 41.8781, 'lng': -87.6298, 'country': 'United States', 'country_code': 'us'},
        'houston': {'lat': 29.7604, 'lng': -95.3698, 'country': 'United States', 'country_code': 'us'},
        'houston, tx': {'lat': 29.7604, 'lng': -95.3698, 'country': 'United States', 'country_code': 'us'},
        'phoenix': {'lat': 33.4484, 'lng': -112.0740, 'country': 'United States', 'country_code': 'us'},
        'philadelphia': {'lat': 39.9526, 'lng': -75.1652, 'country': 'United States', 'country_code': 'us'},
        'san antonio': {'lat': 29.4241, 'lng': -98.4936, 'country': 'United States', 'country_code': 'us'},
        'san diego': {'lat': 32.7157, 'lng': -117.1611, 'country': 'United States', 'country_code': 'us'},
        'san francisco': {'lat': 37.7749, 'lng': -122.4194, 'country': 'United States', 'country_code': 'us'},
        'san francisco, ca': {'lat': 37.7749, 'lng': -122.4194, 'country': 'United States', 'country_code': 'us'},
        'san jose': {'lat': 37.3382, 'lng': -121.8863, 'country': 'United States', 'country_code': 'us'},
        'seattle': {'lat': 47.6062, 'lng': -122.3321, 'country': 'United States', 'country_code': 'us'},
        'seattle, wa': {'lat': 47.6062, 'lng': -122.3321, 'country': 'United States', 'country_code': 'us'},
        'miami': {'lat': 25.7617, 'lng': -80.1918, 'country': 'United States', 'country_code': 'us'},
        'miami, fl': {'lat': 25.7617, 'lng': -80.1918, 'country': 'United States', 'country_code': 'us'},
        'denver': {'lat': 39.7392, 'lng': -104.9903, 'country': 'United States', 'country_code': 'us'},
        'austin': {'lat': 30.2672, 'lng': -97.7431, 'country': 'United States', 'country_code': 'us'},
        'dallas': {'lat': 32.7767, 'lng': -96.7970, 'country': 'United States', 'country_code': 'us'},
        'atlanta': {'lat': 33.7490, 'lng': -84.3880, 'country': 'United States', 'country_code': 'us'},
        'washington': {'lat': 38.9072, 'lng': -77.0369, 'country': 'United States', 'country_code': 'us'},
        'washington, dc': {'lat': 38.9072, 'lng': -77.0369, 'country': 'United States', 'country_code': 'us'},
        'las vegas': {'lat': 36.1699, 'lng': -115.1398, 'country': 'United States', 'country_code': 'us'},
        'portland': {'lat': 45.5152, 'lng': -122.6784, 'country': 'United States', 'country_code': 'us'},
        'detroit': {'lat': 42.3314, 'lng': -83.0458, 'country': 'United States', 'country_code': 'us'},
        'minneapolis': {'lat': 44.9778, 'lng': -93.2650, 'country': 'United States', 'country_code': 'us'},
        'charlotte': {'lat': 35.2271, 'lng': -80.8431, 'country': 'United States', 'country_code': 'us'},
        'orlando': {'lat': 28.5383, 'lng': -81.3792, 'country': 'United States', 'country_code': 'us'},
        'tampa': {'lat': 27.9506, 'lng': -82.4572, 'country': 'United States', 'country_code': 'us'},
        'pittsburgh': {'lat': 40.4406, 'lng': -79.9959, 'country': 'United States', 'country_code': 'us'},
        'cleveland': {'lat': 41.4993, 'lng': -81.6944, 'country': 'United States', 'country_code': 'us'},
        'nashville': {'lat': 36.1627, 'lng': -86.7816, 'country': 'United States', 'country_code': 'us'},
        'salt lake city': {'lat': 40.7608, 'lng': -111.8910, 'country': 'United States', 'country_code': 'us'},
        'raleigh': {'lat': 35.7796, 'lng': -78.6382, 'country': 'United States', 'country_code': 'us'},
        
        # Indian Cities  
        'mumbai': {'lat': 19.0760, 'lng': 72.8777, 'country': 'India', 'country_code': 'in'},
        'delhi': {'lat': 28.6139, 'lng': 77.2090, 'country': 'India', 'country_code': 'in'},
        'bangalore': {'lat': 12.9716, 'lng': 77.5946, 'country': 'India', 'country_code': 'in'},
        'hyderabad': {'lat': 17.3850, 'lng': 78.4867, 'country': 'India', 'country_code': 'in'},
        'chennai': {'lat': 13.0827, 'lng': 80.2707, 'country': 'India', 'country_code': 'in'},
        'pune': {'lat': 18.5204, 'lng': 73.8567, 'country': 'India', 'country_code': 'in'},
        'nashik': {'lat': 19.9975, 'lng': 73.7898, 'country': 'India', 'country_code': 'in'},
        'nashik, maharashtra': {'lat': 19.9975, 'lng': 73.7898, 'country': 'India', 'country_code': 'in'},
        'kolkata': {'lat': 22.5726, 'lng': 88.3639, 'country': 'India', 'country_code': 'in'},
        'ahmedabad': {'lat': 23.0225, 'lng': 72.5714, 'country': 'India', 'country_code': 'in'},
        'jaipur': {'lat': 26.9124, 'lng': 75.7873, 'country': 'India', 'country_code': 'in'},
        
        # UK Cities
        'london': {'lat': 51.5074, 'lng': -0.1278, 'country': 'United Kingdom', 'country_code': 'gb'},
        'manchester': {'lat': 53.4808, 'lng': -2.2426, 'country': 'United Kingdom', 'country_code': 'gb'},
        'birmingham': {'lat': 52.4862, 'lng': -1.8904, 'country': 'United Kingdom', 'country_code': 'gb'},
        'leeds': {'lat': 53.8008, 'lng': -1.5491, 'country': 'United Kingdom', 'country_code': 'gb'},
        'glasgow': {'lat': 55.8642, 'lng': -4.2518, 'country': 'United Kingdom', 'country_code': 'gb'},
        'edinburgh': {'lat': 55.9533, 'lng': -3.1883, 'country': 'United Kingdom', 'country_code': 'gb'},
        
        # European Cities
        'paris': {'lat': 48.8566, 'lng': 2.3522, 'country': 'France', 'country_code': 'fr'},
        'berlin': {'lat': 52.5200, 'lng': 13.4050, 'country': 'Germany', 'country_code': 'de'},
        'munich': {'lat': 48.1351, 'lng': 11.5820, 'country': 'Germany', 'country_code': 'de'},
        'rome': {'lat': 41.9028, 'lng': 12.4964, 'country': 'Italy', 'country_code': 'it'},
        'madrid': {'lat': 40.4168, 'lng': -3.7038, 'country': 'Spain', 'country_code': 'es'},
        'barcelona': {'lat': 41.3851, 'lng': 2.1734, 'country': 'Spain', 'country_code': 'es'},
        'amsterdam': {'lat': 52.3676, 'lng': 4.9041, 'country': 'Netherlands', 'country_code': 'nl'},
        
        # Other major cities
        'tokyo': {'lat': 35.6762, 'lng': 139.6503, 'country': 'Japan', 'country_code': 'jp'},
        'sydney': {'lat': -33.8688, 'lng': 151.2093, 'country': 'Australia', 'country_code': 'au'},
        'melbourne': {'lat': -37.8136, 'lng': 144.9631, 'country': 'Australia', 'country_code': 'au'},
        'toronto': {'lat': 43.6532, 'lng': -79.3832, 'country': 'Canada', 'country_code': 'ca'},
        'vancouver': {'lat': 49.2827, 'lng': -123.1207, 'country': 'Canada', 'country_code': 'ca'},
        'montreal': {'lat': 45.5017, 'lng': -73.5673, 'country': 'Canada', 'country_code': 'ca'},
        'sao paulo': {'lat': -23.5505, 'lng': -46.6333, 'country': 'Brazil', 'country_code': 'br'},
        'mexico city': {'lat': 19.4326, 'lng': -99.1332, 'country': 'Mexico', 'country_code': 'mx'},
        
        # Countries (fallback to capital/major city)
        'united states': {'lat': 37.0902, 'lng': -95.7129, 'country': 'United States', 'country_code': 'us'},
        'usa': {'lat': 37.0902, 'lng': -95.7129, 'country': 'United States', 'country_code': 'us'},
        'india': {'lat': 20.5937, 'lng': 78.9629, 'country': 'India', 'country_code': 'in'},
        'united kingdom': {'lat': 51.5074, 'lng': -0.1278, 'country': 'United Kingdom', 'country_code': 'gb'},
        'uk': {'lat': 51.5074, 'lng': -0.1278, 'country': 'United Kingdom', 'country_code': 'gb'},
        'canada': {'lat': 56.1304, 'lng': -106.3468, 'country': 'Canada', 'country_code': 'ca'},
        'australia': {'lat': -25.2744, 'lng': 133.7751, 'country': 'Australia', 'country_code': 'au'},
        'germany': {'lat': 51.1657, 'lng': 10.4515, 'country': 'Germany', 'country_code': 'de'},
        'france': {'lat': 46.2276, 'lng': 2.2137, 'country': 'France', 'country_code': 'fr'},
        'japan': {'lat': 36.2048, 'lng': 138.2529, 'country': 'Japan', 'country_code': 'jp'},
        'brazil': {'lat': -14.2350, 'lng': -51.9253, 'country': 'Brazil', 'country_code': 'br'},
        'mexico': {'lat': 23.6345, 'lng': -102.5528, 'country': 'Mexico', 'country_code': 'mx'},
    }
    
    # Normalize location name for cache lookup
    normalized = location_name.lower().strip()
    
    # Check cache first
    if normalized in LOCATION_CACHE:
        cached = LOCATION_CACHE[normalized]
        result = {
            'lat': cached['lat'],
            'lng': cached['lng'],
            'display_name': location_name,
            'country': cached['country'],
            'country_code': cached['country_code']
        }
        print(f"[Geocode] Cache hit for '{location_name}' -> ({result['lat']}, {result['lng']})")
        return result
    
    # Try to match partial location names (e.g., "Boston, MA, United States")
    for key, value in LOCATION_CACHE.items():
        if key in normalized or normalized.startswith(key):
            result = {
                'lat': value['lat'],
                'lng': value['lng'],
                'display_name': location_name,
                'country': value['country'],
                'country_code': value['country_code']
            }
            print(f"[Geocode] Partial match '{key}' for '{location_name}' -> ({result['lat']}, {result['lng']})")
            return result
    
    # Fallback to API if not in cache
    try:
        from urllib.parse import quote_plus
        import time
        
        # Add a small delay to respect rate limits
        time.sleep(0.5)
        
        # Try Nominatim with better User-Agent
        url = f"https://nominatim.openstreetmap.org/search?q={quote_plus(location_name)}&format=json&limit=1&addressdetails=1"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data and len(data) > 0:
            result = data[0]
            address = result.get('address', {})
            
            geocoded = {
                'lat': float(result.get('lat', 0)),
                'lng': float(result.get('lon', 0)),
                'display_name': result.get('display_name', ''),
                'country': address.get('country', ''),
                'country_code': address.get('country_code', '').lower()
            }
            print(f"[Geocode] API resolved '{location_name}' -> ({geocoded['lat']}, {geocoded['lng']}) in {geocoded['country']}")
            return geocoded
        else:
            print(f"[Geocode] No API results for '{location_name}'")
            return None
            
    except requests.RequestException as e:
        print(f"[Geocode] API error for '{location_name}': {e}")
        return None
    except Exception as e:
        print(f"[Geocode] Unexpected error for '{location_name}': {e}")
        return None


def parse_query_location(query: str) -> dict:
    """
    Parse a search query to extract the business/keyword and location parts.
    
    Handles patterns like:
    - "Tusk Berry in Boston, Massachusetts" -> {"keyword": "Tusk Berry", "location": "Boston, Massachusetts"}
    - "pizza near Times Square NYC" -> {"keyword": "pizza", "location": "Times Square NYC"}
    - "coffee shops at Downtown Seattle" -> {"keyword": "coffee shops", "location": "Downtown Seattle"}
    - "best restaurants Boston MA" -> {"keyword": "best restaurants", "location": "Boston MA"}
    
    Args:
        query: The full search query string
        
    Returns:
        Dictionary with:
        {
            'keyword': 'extracted keyword/business name',
            'location': 'extracted location' or None,
            'original_query': 'the original query',
            'has_location_intent': True/False
        }
    """
    if not query or not query.strip():
        return {
            'keyword': query,
            'location': None,
            'original_query': query,
            'has_location_intent': False
        }
    
    original = query.strip()
    query_lower = original.lower()
    
    # Known location patterns with prepositions
    location_prepositions = [' in ', ' near ', ' at ', ' around ']
    
    for prep in location_prepositions:
        if prep in query_lower:
            # Split on the preposition (case-insensitive)
            import re
            parts = re.split(prep, query_lower, maxsplit=1)
            if len(parts) == 2:
                # Find the split index in original query for proper casing
                split_idx = query_lower.find(prep)
                keyword = original[:split_idx].strip()
                location = original[split_idx + len(prep):].strip()
                
                if keyword and location:
                    # Validate that location looks like a real location
                    # (has city/state patterns or is a known location)
                    location_result = geocode_location(location)
                    if location_result and location_result.get('lat'):
                        print(f"[QueryParser] Extracted: keyword='{keyword}', location='{location}'")
                        return {
                            'keyword': keyword,
                            'location': location,
                            'original_query': original,
                            'has_location_intent': True,
                            'geocoded': location_result
                        }
    
    # Check for state abbreviations at the end (e.g., "restaurants Boston MA")
    us_state_abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                         'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                         'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                         'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                         'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC']
    
    words = original.split()
    if len(words) >= 2:
        last_word = words[-1].upper()
        if last_word in us_state_abbrevs:
            # Check if second-to-last word could be a city
            potential_location = ' '.join(words[-2:])  # "Boston MA"
            location_result = geocode_location(potential_location)
            if location_result and location_result.get('lat'):
                keyword = ' '.join(words[:-2]).strip()
                if keyword:
                    print(f"[QueryParser] Extracted via state abbrev: keyword='{keyword}', location='{potential_location}'")
                    return {
                        'keyword': keyword,
                        'location': potential_location,
                        'original_query': original,
                        'has_location_intent': True,
                        'geocoded': location_result
                    }
    
    # Check for "near me" patterns
    near_me_patterns = ['near me', 'nearby', 'close to me', 'around me', 'in my area']
    for pattern in near_me_patterns:
        if pattern in query_lower:
            keyword = query_lower.replace(pattern, '').strip()
            # Restore original casing for keyword
            keyword_idx = query_lower.find(keyword)
            keyword = original[keyword_idx:keyword_idx + len(keyword)].strip()
            
            print(f"[QueryParser] 'Near me' query: keyword='{keyword}'")
            return {
                'keyword': keyword if keyword else original,
                'location': None,
                'original_query': original,
                'has_location_intent': True,
                'requires_user_location': True
            }
    
    # No location pattern found
    return {
        'keyword': original,
        'location': None,
        'original_query': original,
        'has_location_intent': False
    }

