import requests
import json

BASE_URL = "http://localhost:5000"

def test_business_search():
    print("Testing /api/gmb/business/search...")
    url = f"{BASE_URL}/api/gmb/business/search"
    payload = {
        "query": "pizza",
        "location": "Chicago",
        "lat": 41.8781,
        "lng": -87.6298
    }
    # Note: Authorization might be needed if login_required is active
    # But let's try without first as local mode might skip it or we can simulate session
    # Actually, main.py says login_required. In local mode, we need a session.
    # So we should login first.

    session = requests.Session()
    
    # 1. Login
    print("Logging in...")
    api_login = f"{BASE_URL}/api/login"
    session.post(api_login, json={"username": "local", "password": "password"}) # Local mode skips password check usually or auto-logins?
    # Actually main.py: "In local mode, auto-login if not already logged in" for routes
    # But for API, we might need to hit an endpoint that sets the session cookie.
    # '/login' page does auto-login.
    
    session.get(f"{BASE_URL}/login")
    
    # 2. Search
    print("Sending Search Request...")
    try:
        response = session.post(url, json=payload)
        print(f"Status: {response.status_code}")
        # print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")

def test_competitor_find():
    print("\nTesting /api/competitor/find...")
    url = f"{BASE_URL}/api/competitor/find"
    payload = {
        "keyword": "coffee",
        "lat": 51.5074,
        "lng": -0.1278,
        "max_results": 1
    }
    
    session = requests.Session()
    session.get(f"{BASE_URL}/login") # Auto-login
    
    try:
        response = session.post(url, json=payload)
        print(f"Status: {response.status_code}")
        # print(f"Response: {response.text[:200]}...")
    except Exception as e:
        print(f"Error: {e}")

def test_serp_check():
    print("\nTesting /api/gmb/serp/check (SERP Crawler)...")
    url = f"{BASE_URL}/api/gmb/serp/check"
    payload = {
        "keyword": "Dentist near me",
        "location": "Nashik, Maharashtra",
        "lat": 19.9975,
        "lng": 73.7898
    }
    
    session = requests.Session()
    session.get(f"{BASE_URL}/login") # Auto-login

    try:
        response = session.post(url, json=payload)
        print(f"Status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                print("✅ Success! Check logs for location verification.")
            else:
                print(f"❌ API Error: {data.get('error')}")
        else:
            print(f"❌ HTTP Error: {response.text[:200]}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # test_business_search()
    # test_competitor_find()
    test_serp_check()
