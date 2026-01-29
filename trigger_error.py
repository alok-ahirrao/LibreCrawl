
import requests
import json

url = "http://localhost:5000/api/gmb/serp/check"
payload = {
    "keyword": "coffee shop",
    "location": "New York, NY",
    "device": "desktop",
    "depth": 10,
    "fast_mode": True
}
headers = {
    'Content-Type': 'application/json'
}

try:
    response = requests.post(url, json=payload, headers=headers)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Request failed: {e}")
