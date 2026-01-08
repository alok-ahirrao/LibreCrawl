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
                'gemini-1.5-flash',
                'gemini-1.5-pro',
                'gemini-1.0-pro'
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
    
    def calculate_opportunity_score(
        self,
        keyword: Dict,
        traffic_weight: float = 0.35,
        competition_weight: float = 0.30,
        intent_weight: float = 0.20,
        trend_weight: float = 0.15
    ) -> int:
        """
        Calculate a composite opportunity score for a keyword.
        
        Formula:
        Score = (Traffic × 0.35) + (1 - Competition × 0.30) + (Intent × 0.20) + (Trend × 0.15)
        
        Args:
            keyword: Keyword dict with traffic_potential, competition, intent, etc.
            
        Returns:
            Integer score from 0-100
        """
        # Traffic potential mapping (higher is better)
        traffic_map = {
            'very_high': 100,
            'high': 80,
            'medium': 50,
            'low': 25,
            'very_low': 10
        }
        
        # Competition mapping (lower is better for opportunity)
        competition_map = {
            'very_low': 100,
            'low': 75,
            'medium': 50,
            'high': 25,
            'very_high': 10
        }
        
        # Intent value mapping (transactional = higher commercial value)
        intent_map = {
            'transactional': 100,
            'commercial': 85,
            'comparison': 70,
            'local': 65,
            'informational': 50,
            'navigational': 30,
            'question': 55
        }
        
        # Get scores (with defaults)
        traffic = keyword.get('traffic_potential', 'medium')
        competition = keyword.get('competition', 'medium')
        intent = keyword.get('intent', keyword.get('type', 'related'))
        is_trending = keyword.get('is_breakout', False) or keyword.get('growth', '')
        
        traffic_score = traffic_map.get(traffic, 50)
        competition_score = competition_map.get(competition, 50)
        intent_score = intent_map.get(intent, 50)
        trend_score = 100 if is_trending else 50
        
        # Calculate weighted score
        final_score = (
            traffic_score * traffic_weight +
            competition_score * competition_weight +
            intent_score * intent_weight +
            trend_score * trend_weight
        )
        
        return min(100, max(0, int(final_score)))
    
    def enrich_keywords_with_scores(self, keywords: List[Dict]) -> List[Dict]:
        """
        Add opportunity scores to a list of keywords.
        
        Args:
            keywords: List of keyword dicts
            
        Returns:
            Same list with 'opportunity_score' field added
        """
        for kw in keywords:
            kw['opportunity_score'] = self.calculate_opportunity_score(kw)
        
        # Sort by opportunity score (highest first)
        return sorted(keywords, key=lambda x: x.get('opportunity_score', 0), reverse=True)

    async def generate_actionable_insights(
        self,
        keyword_data: Dict,
        business_goal: str = "bookings",
        business_type: str = None,
        monthly_budget: str = None
    ) -> Dict:
        """
        Generate AI-powered actionable insights from keyword discovery data.
        
        This method analyzes all discovered keywords and provides:
        - Quick wins (easy to rank, high conversion potential)
        - High-value targets (worth the effort despite competition)
        - Keywords to avoid (high authority competition, low ROI)
        - Strategic recommendations for increasing bookings/revenue
        - Prioritized action plan with difficulty and impact scores
        
        Args:
            keyword_data: Full keyword discovery results dict
            business_goal: Primary goal - 'bookings', 'revenue', 'traffic', 'brand_awareness'
            business_type: Type of business (e.g., 'dental clinic', 'law firm')
            monthly_budget: Optional budget context (e.g., 'low', 'medium', 'high')
            
        Returns:
            Comprehensive actionable insights with prioritized recommendations
        """
        # If AI is not available, use rule-based fallback
        if not self.is_available():
            return self._generate_fallback_insights(keyword_data, business_goal)
        
        # Extract key data for analysis
        all_keywords = keyword_data.get('discovered_keywords', [])[:50]
        trending = keyword_data.get('trending_keywords', [])[:15]
        questions = keyword_data.get('questions', [])[:15]
        local_kws = keyword_data.get('local_keywords', [])[:15]
        niche_kws = keyword_data.get('niche_keywords', [])[:15]
        competitor_kws = keyword_data.get('competitor_keywords', [])[:15]
        stats = keyword_data.get('stats', {})
        
        # Build context
        context_parts = []
        if business_type:
            context_parts.append(f"Business Type: {business_type}")
        if monthly_budget:
            context_parts.append(f"SEO Budget Level: {monthly_budget}")
        context_parts.append(f"Primary Goal: {business_goal.upper()}")
        context = "\n".join(context_parts)
        
        prompt = f"""You are a senior SEO strategist and conversion optimization expert. 
Your task is to analyze keyword data and provide ACTIONABLE INSIGHTS that will directly increase {business_goal}.

{context}

=== KEYWORD DATA ===
Total Keywords Discovered: {stats.get('total_unique', len(all_keywords))}
High Opportunity Keywords (Score 70+): {stats.get('high_opportunity', 0)}

Top 20 Keywords (by opportunity score):
{json.dumps([{'keyword': k.get('keyword'), 'score': k.get('opportunity_score', 0), 'type': k.get('type'), 'intent': k.get('intent', 'unknown'), 'source': k.get('source')} for k in all_keywords[:20]], indent=2)}

Trending/Rising Keywords:
{json.dumps([k.get('keyword') for k in trending[:10]])}

Question Keywords (Content Opportunities):
{json.dumps([k.get('keyword') for k in questions[:10]])}

Local SEO Keywords:
{json.dumps([k.get('keyword') for k in local_kws[:10]])}

Niche-Specific Keywords:
{json.dumps([k.get('keyword') for k in niche_kws[:10]])}

Competitor-Style Keywords:
{json.dumps([k.get('keyword') for k in competitor_kws[:10]])}

=== YOUR ANALYSIS TASK ===
Provide strategic recommendations focusing on:
1. QUICK WINS - Keywords that are easy to rank for (low competition) AND have high booking/conversion potential
2. HIGH-VALUE TARGETS - Keywords worth investing in despite higher competition (high revenue potential)
3. AVOID LIST - Keywords dominated by high-authority sites (waste of resources)
4. CONTENT STRATEGY - What type of content to create for each keyword category
5. PRIORITIZED ACTION PLAN - Step-by-step actions ranked by impact and effort

For each recommendation, assess:
- Difficulty (1-10, where 1 is easiest)
- Impact on {business_goal} (1-10, where 10 is highest impact)
- Estimated time to see results
- Specific action to take

Return ONLY valid JSON:
{{
    "summary": "2-3 sentence strategic summary focused on {business_goal}",
    
    "quick_wins": [
        {{
            "keyword": "example easy keyword",
            "difficulty": 3,
            "impact": 8,
            "action": "Create a dedicated landing page with clear CTA",
            "content_type": "service page",
            "time_to_rank": "2-4 weeks",
            "why": "Low competition in local market, high booking intent"
        }}
    ],
    
    "high_value_targets": [
        {{
            "keyword": "example competitive keyword",
            "difficulty": 7,
            "impact": 9,
            "action": "Create comprehensive pillar content with backlink strategy",
            "content_type": "ultimate guide",
            "time_to_rank": "3-6 months",
            "why": "High search volume, strong commercial intent",
            "investment_required": "Medium - requires quality backlinks"
        }}
    ],
    
    "avoid_keywords": [
        {{
            "keyword": "example hard keyword",
            "reason": "Dominated by WebMD, Mayo Clinic - unrealistic to compete",
            "alternative": "Focus on local variant instead"
        }}
    ],
    
    "content_strategy": [
        {{
            "priority": 1,
            "content_type": "Service landing pages",
            "target_keywords": ["keyword1", "keyword2"],
            "description": "Create conversion-optimized service pages for local keywords",
            "expected_impact": "Direct {business_goal} increase"
        }}
    ],
    
    "action_plan": [
        {{
            "week": "Week 1-2",
            "priority": "HIGH",
            "action": "Create 3 quick-win landing pages",
            "target_keywords": ["keyword1", "keyword2", "keyword3"],
            "expected_outcome": "Quick visibility for low-competition terms"
        }}
    ],
    
    "metrics_to_track": [
        "Ranking position for quick-win keywords",
        "Organic traffic to service pages",
        "Conversion rate from organic traffic"
    ],
    
    "overall_difficulty_assessment": "Medium - Good opportunities exist for local and long-tail terms",
    
    "booking_conversion_tips": [
        "Add clear CTAs to all landing pages",
        "Include phone number prominently for local searches",
        "Add booking forms above the fold"
    ],
    
    "estimated_results_timeline": {{
        "quick_wins": "2-4 weeks for initial rankings",
        "moderate_terms": "2-3 months for page 1",
        "competitive_terms": "4-6+ months with consistent effort"
    }}
}}"""

        try:
            response_text = await self._generate_content(prompt)
            result = self._parse_json_response(response_text)
            
            if not result or not result.get('quick_wins'):
                logger.warning("AI insights response was empty or malformed, providing fallback")
                return self._generate_fallback_insights(keyword_data, business_goal)
            
            # Add metadata
            result['generated_at'] = 'AI Analysis'
            result['business_goal'] = business_goal
            result['keywords_analyzed'] = len(all_keywords)
            
            return result
            
        except Exception as e:
            logger.error(f"Actionable insights generation failed: {e}")
            return self._generate_fallback_insights(keyword_data, business_goal)
    
    def _generate_fallback_insights(self, keyword_data: Dict, business_goal: str) -> Dict:
        """Generate rule-based fallback insights when AI is unavailable."""
        try:
            # Safer defaults
            if not keyword_data:
                keyword_data = {}
                
            all_keywords = keyword_data.get('discovered_keywords', []) or []
            local_kws = keyword_data.get('local_keywords', []) or []
            questions = keyword_data.get('questions', []) or []
            
            # Ensure lists contain dicts
            all_keywords = [k for k in all_keywords if isinstance(k, dict)]
            local_kws = [k for k in local_kws if isinstance(k, dict)]
        
            # Simple rule-based categorization
            quick_wins = []
            high_value = []
            
            for kw in all_keywords[:30]:
                score = kw.get('opportunity_score', 0)
                kw_type = kw.get('type', '')
                intent = kw.get('intent', '')
                
                # Quick wins: high score + local or transactional
                if score >= 75 and (kw_type == 'local' or intent == 'transactional'):
                    quick_wins.append({
                        'keyword': kw.get('keyword'),
                        'difficulty': 3,
                        'impact': 8,
                        'action': 'Create targeted landing page',
                        'content_type': 'service page',
                        'time_to_rank': '2-4 weeks',
                        'why': 'High opportunity score with commercial intent'
                    })
                # High value: good score + trending or high traffic potential
                elif score >= 60 and kw.get('traffic_potential') in ['high', 'very_high']:
                    high_value.append({
                        'keyword': kw.get('keyword'),
                        'difficulty': 6,
                        'impact': 8,
                        'action': 'Create comprehensive content',
                        'content_type': 'guide',
                        'time_to_rank': '2-3 months',
                        'why': 'Strong traffic potential'
                    })
            
            return {
                'summary': f'Based on {len(all_keywords)} keywords analyzed, focus on local and transactional keywords for fastest {business_goal} impact.',
                'quick_wins': quick_wins[:5],
                'high_value_targets': high_value[:5],
                'avoid_keywords': [],
                'content_strategy': [
                    {
                        'priority': 1,
                        'content_type': 'Local landing pages',
                        'target_keywords': [k['keyword'] for k in local_kws[:3]],
                        'description': 'Create location-specific service pages',
                        'expected_impact': f'Direct {business_goal} increase'
                    }
                ],
                'action_plan': [
                    {
                        'week': 'Week 1-2',
                        'priority': 'HIGH',
                        'action': 'Create pages for top 3 quick-win keywords',
                        'target_keywords': [k['keyword'] for k in quick_wins[:3]],
                        'expected_outcome': 'Quick visibility gains'
                    }
                ],
                'metrics_to_track': [
                    'Keyword ranking positions',
                    'Organic traffic growth',
                    f'{business_goal.capitalize()} from organic search'
                ],
                'overall_difficulty_assessment': 'Moderate - Focus on local and long-tail keywords first',
                'booking_conversion_tips': [
                    'Add clear call-to-action buttons',
                    'Include contact forms on all pages',
                    'Display phone number prominently'
                ],
                'estimated_results_timeline': {
                    'quick_wins': '2-4 weeks',
                    'moderate_terms': '2-3 months',
                    'competitive_terms': '4-6 months'
                },
                'generated_at': 'Rule-Based Fallback',
                'business_goal': business_goal,
                'keywords_analyzed': len(all_keywords)
            }
        except Exception as e:
            logger.error(f"Fallback generation failed: {e}")
            return {
                'summary': f"Could not generate insights for '{business_goal}'. Please try exploring keywords manually.",
                'quick_wins': [],
                'high_value_targets': [],
                'avoid_keywords': [],
                'content_strategy': [],
                'action_plan': [],
                'metrics_to_track': [],
                'generated_at': 'Error Fallback',
                'error': str(e)
            }
