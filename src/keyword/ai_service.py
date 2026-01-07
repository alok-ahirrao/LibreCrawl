"""
Google Gemini AI Service for Keyword Research
Provides AI-powered keyword analysis, expansion, and recommendations.
Uses the free tier of Google Gemini API (1500 requests/day).
"""

import os
import json
import re
import asyncio
import functools
from typing import Optional, List, Dict
import logging

logger = logging.getLogger(__name__)

# Try to import google.generativeai
GEMINI_AVAILABLE = False
genai = None

try:
    import google.generativeai as _genai
    genai = _genai
    GEMINI_AVAILABLE = True
except ImportError:
    logger.warning("google-generativeai not installed. Run: pip install google-generativeai")


class GeminiKeywordAI:
    """
    AI-powered keyword analysis using Google Gemini.
    
    Features:
    - Keyword expansion (suggest related keywords)
    - Intent classification (informational, transactional, etc.)
    - Keyword grouping (cluster by topic)
    - Difficulty estimation (based on SERP data)
    - Content gap analysis
    - SEO recommendations
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Gemini AI service.
        
        Args:
            api_key: Google Gemini API key. If not provided, reads from GEMINI_API_KEY env var.
        """
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.model = None
        self.model_name = None
        self.available = False
        self.use_legacy_api = False
        self.use_rest_api = False
        
        # Note: We rely on requests for REST API, which is a standard dependency
        
        if not self.api_key:
            logger.warning("Gemini AI not available - GEMINI_API_KEY not set")
            return
        
        try:
            # OPTION 1: Try new library API (GenerativeModel)
            if GEMINI_AVAILABLE and genai:
                try:
                    genai.configure(api_key=self.api_key)
                    if hasattr(genai, 'GenerativeModel'):
                        logger.info("GenerativeModel class found - using library API")
                        model_names = ['gemini-1.5-flash', 'gemini-pro']
                        for model_name in model_names:
                            try:
                                self.model = genai.GenerativeModel(model_name)
                                self.model_name = model_name
                                self.available = True
                                logger.info(f"Gemini AI initialized with GenerativeModel: {model_name}")
                                break
                            except Exception as e:
                                logger.debug(f"Model {model_name} failed: {e}")
                                continue
                except Exception as e:
                    logger.warning(f"Library initialization failed: {e}")
            
            # OPTION 2: Fallback to REST API (Bypasses library issues)
            if not self.available:
                logger.info("Falling back to REST API (requests)")
                self.use_rest_api = True
                self.available = True
                
        except Exception as e:
            logger.error(f"Failed to initialize Gemini AI: {e}")
            # Last ditch attempt - still try REST
            self.use_rest_api = True
            self.available = True
    
    def is_available(self) -> bool:
        """Check if Gemini AI is available and configured."""
        return self.available
    
    async def _generate_content(self, prompt: str) -> str:
        """Generate content using appropriate API (Python 3.8 compatible)."""
        import functools
        import requests
        
        loop = asyncio.get_event_loop()
        
        # Method 1: REST API (Most robust fallback)
        # Method 1: REST API (Most robust fallback)
        if self.use_rest_api:
            # Try multiple models in order of preference (updated for 2026 availability)
            rest_models = [
                'gemini-2.5-flash', 
                'gemini-2.5-pro', 
                'gemini-flash-latest', 
                'gemini-pro-latest',
                'gemini-2.0-flash',
                'gemini-1.5-flash',  # Fallback
                'gemini-pro'
            ]
            
            for model in rest_models:
                try:
                    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self.api_key}"
                    headers = {'Content-Type': 'application/json'}
                    data = {
                        "contents": [{
                            "parts": [{"text": prompt}]
                        }]
                    }
                    
                    def call_rest(u, d):
                        response = requests.post(u, headers=headers, json=d, timeout=60)
                        # If 404, raise specific error to try next model
                        if response.status_code == 404:
                            raise ValueError(f"Model {model} not found (404)")
                        if response.status_code == 429:
                             raise ValueError(f"Model {model} rate limited (429)")
                        response.raise_for_status()
                        return response.json()
                    
                    result = await loop.run_in_executor(None, functools.partial(call_rest, url, data))
                    
                    # Parse response
                    try:
                        return result['candidates'][0]['content']['parts'][0]['text']
                    except (KeyError, IndexError):
                        logger.warning(f"Unexpected REST API response format from {model}: {result}")
                        continue # Try next model if response format is weird
                        
                except Exception as e:
                    logger.warning(f"REST API call failed for {model}: {e}")
                    import time
                    time.sleep(1) # Short delay before failing over to next model
                    continue
            
            # If all models failed, try to list available models found via REST to debug
            try:
                list_url = f"https://generativelanguage.googleapis.com/v1beta/models?key={self.api_key}"
                list_response = requests.get(list_url, timeout=10)
                if list_response.status_code == 200:
                    available = list_response.json().get('models', [])
                    model_names = [m['name'] for m in available]
                    logger.error(f"All specific models failed. Available models for this key: {model_names}")
                    
                    # Try to use the first available generateContent model (excluding embedding/imagen)
                    for m in available:
                        m_name = m['name']
                        # Loose check for generation capabilities if supportedGenerationMethods is missing or strict
                        methods = m.get('supportedGenerationMethods', [])
                        
                        is_generative = 'generateContent' in methods
                        # Fallback heuristic: name suggests it's a text model and not embedding/image-only
                        if not is_generative and ('flash' in m_name or 'pro' in m_name or 'ultra' in m_name):
                             if 'embedding' not in m_name and 'imagen' not in m_name and 'veo' not in m_name:
                                 is_generative = True
                                 
                        if is_generative:
                            model_name = m_name.replace('models/', '')
                            logger.info(f"Attempting auto-discovered model: {model_name}")
                            try:
                                url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={self.api_key}"
                                headers = {'Content-Type': 'application/json'}
                                data = {
                                    "contents": [{"parts": [{"text": prompt}]}]
                                }
                                response = requests.post(url, headers=headers, json=data, timeout=30)
                                response.raise_for_status()
                                result = response.json()
                                return result['candidates'][0]['content']['parts'][0]['text']
                            except Exception as e:
                                logger.error(f"Auto-discovered model {model_name} failed: {e}")
                else:
                    logger.error(f"Could not list models via REST: {list_response.status_code} {list_response.text}")
            except Exception as e:
                logger.error(f"Failed to list models via REST: {e}")

            logger.error("All REST API models failed")
            return ""

        # Method 2: Legacy Library API (generate_text)
        elif self.use_legacy_api:
            # ... existing legacy code ...
            func = functools.partial(
                genai.generate_text,
                model=self.model_name,
                prompt=prompt
            )
            response = await loop.run_in_executor(None, func)
            return response.result if response and response.result else ""

        # Method 3: New Library API (GenerativeModel)
        else:
            func = functools.partial(self.model.generate_content, prompt)
            response = await loop.run_in_executor(None, func)
            return response.text if response else ""
    
    def _parse_json_response(self, text: str) -> dict:
        """Parse JSON from Gemini response, handling markdown code blocks."""
        text = text.strip()
        if text.startswith('```json'):
            text = text[7:]
        elif text.startswith('```'):
            text = text[3:]
        if text.endswith('```'):
            text = text[:-3]
        text = text.strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\{[\s\S]*\}', text)
            if match:
                try:
                    return json.loads(match.group())
                except:
                    pass
            return {}
    
    async def expand_keywords(self, seed_keywords: List[str], count: int = 20) -> dict:
        """Expand seed keywords with AI suggestions."""
        if not self.is_available():
            return {"keywords": [], "error": "Gemini AI not available"}
        
        prompt = f"""You are an SEO keyword research expert.
Given these seed keywords: {json.dumps(seed_keywords)}

Suggest {count} additional related keywords that:
1. Are semantically related
2. Include long-tail variations (3-5 word phrases)
3. Cover different search intents
4. Include question-based keywords

Return ONLY valid JSON:
{{
    "keywords": [
        {{"keyword": "example keyword", "type": "long-tail", "intent": "informational"}}
    ]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            result = self._parse_json_response(response_text)
            return result if result.get("keywords") else {"keywords": []}
        except Exception as e:
            logger.error(f"Keyword expansion failed: {e}")
            return {"keywords": [], "error": str(e)}
    
    async def classify_intent(self, keywords: List[str]) -> dict:
        """Classify keywords by search intent."""
        if not self.is_available():
            return {"error": "Gemini AI not available"}
        
        prompt = f"""Classify these keywords by search intent:
Keywords: {json.dumps(keywords)}

Return ONLY valid JSON:
{{
    "informational": ["keyword1"],
    "transactional": ["keyword2"],
    "commercial": ["keyword3"],
    "navigational": ["keyword4"]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Intent classification failed: {e}")
            return {"error": str(e)}
    
    async def group_keywords(self, keywords: List[str]) -> dict:
        """Group keywords into topic clusters."""
        if not self.is_available():
            return {"groups": [], "error": "Gemini AI not available"}
        
        prompt = f"""Group these keywords into topic clusters:
Keywords: {json.dumps(keywords)}

Return ONLY valid JSON:
{{
    "groups": [
        {{"topic": "Group Name", "keywords": ["kw1", "kw2"]}}
    ]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Keyword grouping failed: {e}")
            return {"groups": [], "error": str(e)}
    
    async def estimate_difficulty(self, keyword: str, serp_data: dict = None) -> dict:
        """Estimate keyword difficulty."""
        if not self.is_available():
            return {"error": "Gemini AI not available"}
        
        context = f"\nSERP Data: {json.dumps(serp_data)}" if serp_data else ""
        
        prompt = f"""Estimate SEO difficulty for: "{keyword}"{context}

Return ONLY valid JSON:
{{
    "keyword": "{keyword}",
    "difficulty_score": 50,
    "difficulty_level": "medium",
    "factors": ["factor1", "factor2"],
    "recommendation": "Summary recommendation"
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Difficulty estimation failed: {e}")
            return {"error": str(e)}
    
    async def analyze_content_gap(self, your_keywords: List[str], competitor_keywords: List[str]) -> dict:
        """Analyze content gaps between your site and competitors."""
        if not self.is_available():
            return {"error": "Gemini AI not available"}
        
        prompt = f"""Analyze content gap:
Your keywords: {json.dumps(your_keywords[:20])}
Competitor keywords: {json.dumps(competitor_keywords[:20])}

Return ONLY valid JSON:
{{
    "gaps": ["gap1", "gap2"],
    "opportunities": ["opp1", "opp2"],
    "quick_wins": ["win1", "win2"],
    "priority_actions": ["action1", "action2"]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Content gap analysis failed: {e}")
            return {"error": str(e)}
    
    async def generate_recommendations(self, analysis_data: dict, business_type: str = None) -> dict:
        """Generate SEO recommendations based on analysis."""
        if not self.is_available():
            return {"recommendations": [], "error": "Gemini AI not available"}
        
        context = f"Business Type: {business_type}\n" if business_type else ""
        
        prompt = f"""{context}Based on this analysis: {json.dumps(analysis_data)}

Generate SEO recommendations. Return ONLY valid JSON:
{{
    "recommendations": [
        {{"priority": "high", "action": "Action description", "impact": "Expected impact"}}
    ],
    "summary": "Overall strategy summary"
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Recommendation generation failed: {e}")
            return {"recommendations": [], "error": str(e)}
    
    async def analyze_keyword_density(self, url: str, keywords: List[dict], total_words: int) -> dict:
        """Analyze keyword density and provide recommendations."""
        if not self.is_available():
            return {"error": "Gemini AI not available"}
        
        top_keywords = keywords[:30]
        
        prompt = f"""Analyze this keyword density report:
URL: {url}
Total Words: {total_words}
Top Keywords: {json.dumps(top_keywords)}

Return ONLY valid JSON:
{{
    "primary_topic": "Main topic",
    "keyword_health": "good/warning/poor",
    "issues": [{{"keyword": "word", "issue": "Issue description", "fix": "Fix suggestion"}}],
    "semantic_groups": [{{"topic": "Group name", "keywords": ["kw1", "kw2"]}}],
    "missing_keywords": ["keyword1", "keyword2"],
    "recommendations": ["Recommendation 1", "Recommendation 2"]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Density analysis failed: {e}")
            return {"error": str(e)}
    
    async def suggest_high_traffic_keywords(
        self,
        seed_keywords: List[str],
        niche: str = None,
        location: str = None,
        count: int = 30
    ) -> dict:
        """Suggest high-traffic keywords based on seed keywords and context."""
        if not self.is_available():
            return {"keywords": [], "error": "Gemini AI not available"}
        
        context = ""
        if niche:
            context += f"Business Niche: {niche}\n"
        if location:
            context += f"Target Location: {location}\n"
        
        prompt = f"""You are an expert SEO strategist.

{context}
Seed Keywords: {json.dumps(seed_keywords)}

Suggest {count} keywords with HIGH SEARCH VOLUME and TRAFFIC POTENTIAL.

For each keyword, estimate:
1. Traffic potential (very_high, high, medium, low)
2. Competition level (low, medium, high)
3. Search intent (informational, transactional, commercial, navigational)

Return ONLY valid JSON:
{{
    "keywords": [
        {{
            "keyword": "example high traffic keyword",
            "traffic_potential": "high",
            "competition": "medium",
            "intent": "transactional",
            "type": "long-tail",
            "monthly_searches_estimate": "10K-100K",
            "recommendation": "Good opportunity"
        }}
    ],
    "strategy_summary": "Brief overall strategy",
    "quick_wins": ["Keyword 1", "Keyword 2"],
    "high_value_targets": ["Keyword 3", "Keyword 4"]
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            result = self._parse_json_response(response_text)
            return result if result.get("keywords") else {"keywords": []}
        except Exception as e:
            logger.error(f"High-traffic keyword suggestion failed: {e}")
            return {"keywords": [], "error": str(e)}
    
    async def prioritize_keywords(
        self,
        keywords: List[Dict],
        trends_data: Dict = None,
        business_goal: str = "traffic"
    ) -> dict:
        """Prioritize keywords based on traffic potential and trends data."""
        if not self.is_available():
            return {"keywords": keywords, "error": "Gemini AI not available"}
        
        trends_context = ""
        if trends_data:
            rising = trends_data.get('rising_queries', [])[:10]
            if rising:
                trends_context = f"\nTrending Keywords: {json.dumps([q['keyword'] for q in rising])}"
        
        prompt = f"""Prioritize these keywords for {business_goal}:

Keywords: {json.dumps(keywords[:50])}
{trends_context}

Score each from 1-100 based on traffic potential, feasibility, and business value.

Return ONLY valid JSON:
{{
    "prioritized_keywords": [
        {{
            "keyword": "keyword text",
            "score": 85,
            "traffic_potential": "high",
            "priority_tier": "A",
            "action": "Create pillar content"
        }}
    ],
    "tier_a": ["Top priority keywords"],
    "tier_b": ["Secondary keywords"],
    "recommended_focus": "Summary of where to focus"
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Keyword prioritization failed: {e}")
            return {"keywords": keywords, "error": str(e)}
    
    async def analyze_keyword_opportunity(self, keyword: str, competitor_data: List[Dict] = None) -> dict:
        """Deep analysis of a specific keyword's opportunity."""
        if not self.is_available():
            return {"error": "Gemini AI not available"}
        
        competitor_context = ""
        if competitor_data:
            competitor_context = f"\nCompetitors: {json.dumps(competitor_data[:5])}"
        
        prompt = f"""Analyze SEO opportunity for: "{keyword}"
{competitor_context}

Provide detailed analysis. Return ONLY valid JSON:
{{
    "keyword": "{keyword}",
    "search_volume_estimate": "1K-10K",
    "competition_score": 65,
    "competition_level": "medium",
    "opportunity_score": 75,
    "best_content_type": "comprehensive guide",
    "related_keywords": ["keyword1", "keyword2"],
    "content_recommendations": ["Recommendation 1", "Recommendation 2"],
    "time_to_rank": "3-6 months",
    "overall_verdict": "Strong opportunity"
}}"""
        
        try:
            response_text = await self._generate_content(prompt)
            return self._parse_json_response(response_text)
        except Exception as e:
            logger.error(f"Keyword opportunity analysis failed: {e}")
            return {"error": str(e)}
