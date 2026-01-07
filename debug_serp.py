import requests

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}

r = requests.get('https://www.google.com/search?q=test&hl=en&gl=us', headers=headers)
t = r.text

print(f"Status: {r.status_code}")
print(f"Length: {len(t)}")
print(f"'rso' in text: {'rso' in t}")
print(f"'search' in text: {'search' in t.lower()}")
print(f"'Please click' in text: {'Please click' in t}")  # JS redirect indicator

# Save HTML for inspection
with open('debug_serp.html', 'w', encoding='utf-8') as f:
    f.write(t)
print("\nSaved HTML to debug_serp.html for inspection")
