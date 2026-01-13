import asyncio
import os
import sys
import requests
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
# Load env
load_dotenv()

def main():
    api_key = os.getenv('GEMINI_API_KEY')
    if not api_key:
        print("GEMINI_API_KEY not set")
        return

    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        models = response.json().get('models', [])
        print("Available Models:")
        for m in models:
            print(m['name'])
    else:
        print(f"Error fetching models: {response.status_code} - {response.text}")

if __name__ == "__main__":
    main()
