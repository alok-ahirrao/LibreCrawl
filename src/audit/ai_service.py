
import os
import json
import logging
import asyncio
import functools
from typing import Optional, List, Dict, Any

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

class AuditAIService:
    """
    AI Service for chatting with Audit Data using Google Gemini.
    Uses Gemini 1.5 Flash for large context window capability.
    """
    
    _global_semaphore = asyncio.Semaphore(1)
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv('GEMINI_API_KEY')
        self.available = bool(self.api_key)
        self.model_name = 'gemini-2.5-flash-lite' # Free preview model

    def is_available(self) -> bool:
        return self.available

    async def _generate_content(self, prompt: str) -> str:
        """Generate content using REST API with retries for resilience."""
        if not self.is_available():
            return "AI service is not available. Please check API key."

        import requests
        
        loop = asyncio.get_event_loop()
        
        # Retry configuration
        max_retries = 5
        base_delay = 3

        for attempt in range(max_retries):
            try:
                # Use standard Gemini API endpoint
                url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent?key={self.api_key}"
                headers = {'Content-Type': 'application/json'}
                data = {"contents": [{"parts": [{"text": prompt}]}]}
                
                func = functools.partial(requests.post, url, headers=headers, json=data, timeout=60)
                response = await loop.run_in_executor(None, func)
                
                if response.status_code == 200:
                    result = response.json()
                    candidates = result.get('candidates', [])
                    if candidates and candidates[0].get('content'):
                        return candidates[0]['content']['parts'][0]['text']
                    return "No response generated."
                elif response.status_code in [429, 503]:
                    # Rate limited or Overloaded - Retry
                    wait_time = base_delay * (2 ** attempt)
                    logger.warning(f"Gemini API {response.status_code}. Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Gemini API Error: {response.status_code} - {response.text}")
                    return f"Error: AI Service returned status {response.status_code}"

            except Exception as e:
                logger.error(f"AI Generation failed (Attempt {attempt+1}): {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(base_delay * (2 ** attempt))
                else:
                    return f"Error generating response: {str(e)}"
        
        return "Error: Maximum retries exceeded for AI service."

    async def chat_with_audit(self, question: str, audit_data: Dict[str, Any]) -> str:
        """
        Chat with the audit data.
        
        Args:
            question: User's question
            audit_data: Dictionary containing crawl results (urls, issues, stats, etc.)
        """
        if not self.is_available():
            return "AI service is not available."

        # Prepare context from audit data
        # We need to serialize this efficiently.
        # Focus on Issues, Stats, and a summary of URLs.
        
        stats = audit_data.get('stats', {})
        issues = audit_data.get('issues', [])
        urls = audit_data.get('urls', [])
        
        # Summarize Issues
        issue_summary = {}
        for issue in issues:
            cat = issue.get('category', 'other')
            if cat not in issue_summary:
                issue_summary[cat] = []
            issue_summary[cat].append(f"{issue.get('issue')}: {issue.get('url')}")
            
        # Summarize URLs (just stats)
        url_status_counts = {}
        for url in urls:
            status = url.get('status', 'unknown')
            url_status_counts[status] = url_status_counts.get(status, 0) + 1

        context = f"""
Audit Statistics:
{json.dumps(stats, indent=2)}

URL Status Counts:
{json.dumps(url_status_counts, indent=2)}

Detected Issues (Grouped by Category):
{json.dumps(issue_summary, indent=2)}

Total URLs Crawled: {len(urls)}
Total Issues Found: {len(issues)}
"""

        prompt = f"""You are an expert SEO Audit Assistant. You have access to the crawl data of a website.
Use the following context to answer the user's question.

FORMATTING INSTRUCTIONS:
- Use proper Markdown formatting.
- Use ### (H3) for section headers.
- Use **bold** for key metrics or important terms.
- Use bullet points for lists.
- Be concise but comprehensive.

If the answer is not in the data, say "I cannot find that information in the audit data".

Context Data:
{context}

User Question: "{question}"

Answer:"""

    def _validate_audit_data(self, audit_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Validate audit data before sending to AI.
        Returns error dict if validation fails, None if valid.
        """
        stats = audit_data.get('stats', {})
        issues = audit_data.get('issues', [])
        urls = audit_data.get('urls', [])
        
        # Check 1: Empty crawl (no URLs)
        if not urls:
            return {
                "error": True,
                "error_code": "NO_DATA",
                "error_message": "No pages were crawled. The URL may be unreachable or blocked.",
                "executive_summary": "Analysis failed: No crawl data available.",
                "site_goal": "Unknown"
            }
        
        # Check 2: All URLs are 4xx/5xx errors
        error_count = sum(1 for u in urls if u.get('status_code', 0) >= 400 or u.get('status_code', 0) == 0)
        if error_count == len(urls):
            return {
                "error": True,
                "error_code": "ALL_ERRORS",
                "error_message": f"All {len(urls)} crawled pages returned errors (4xx/5xx). The site may be down or blocked.",
                "executive_summary": "Analysis failed: All pages returned HTTP errors.",
                "site_goal": "Unknown"
            }

        # Check 3: Check if homepage is a non-HTML file (pdf, image, etc.)
        first_url = urls[0] if urls else {}
        content_type = first_url.get('content_type', '').lower() if first_url.get('content_type') else 'text/html'
        
        non_html_types = ['application/pdf', 'image/', 'text/plain', 'application/json', 'application/xml']
        is_non_html = any(nt in content_type for nt in non_html_types)
        
        if is_non_html:
            return {
                "error": True,
                "error_code": "FORMAT_NOT_SUPPORTED",
                "error_message": f"The crawled URL returned a non-HTML content type ({content_type}). SEO analysis requires HTML pages.",
                "executive_summary": "Analysis failed: Non-HTML content detected.",
                "site_goal": "Unknown"
            }
        
        # Check 4: Plain HTML placeholder
        # If we have 0 issues and minimal content, warn about potential plain HTML file
        if len(issues) == 0 and len(urls) == 1:
            word_count = first_url.get('word_count', 0)
            if word_count is None or word_count < 20:
                return {
                    "error": True,
                    "error_code": "THIN_CONTENT", 
                    "error_message": "The page has very little content (less than 20 words). This may be a placeholder or under-construction page.",
                    "executive_summary": "Limited analysis available: Page has minimal content.",
                    "site_goal": "Unknown"
                }
        
        return None  # Validation passed

    async def generate_insights(self, audit_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate strategic insights from audit data.
        Returns a structured JSON object.
        """
        if not self.is_available():
            return {"error": True, "error_code": "AI_UNAVAILABLE", "error_message": "AI service not available. Please check your API key configuration."}

        # Pre-validation
        validation_error = self._validate_audit_data(audit_data)
        if validation_error:
            return validation_error

        stats = audit_data.get('stats', {})
        issues = audit_data.get('issues', [])
        urls = audit_data.get('urls', [])

        # Calculate high-level metrics for context
        total_urls = len(urls)
        total_issues = len(issues)
        critical_issues = len([i for i in issues if i.get('severity') == 'high' or 'critical' in i.get('severity', '').lower()])

        # Group issues by severity
        errors = [i for i in issues if i.get('type') == 'error']
        warnings = [i for i in issues if i.get('type') == 'warning']
        infos = [i for i in issues if i.get('type') == 'info']

        # Construct prioritized issue list for AI (limit to maintain context)
        # Prioritize showing Errors, then Warnings, then Infos
        prioritized_issues = errors[:25] + warnings[:20] + infos[:10]

        # Define JSON structure separately to avoid f-string nesting depth errors
        json_template = """
        {
            "executive_summary": "2-3 sentences summarizing the overall health and main opportunity.",
            "site_goal": "Inferred business goal (e.g., 'Lead Gen', 'E-commerce', 'Brand Awareness').",
            "tech_stack": [
                { "name": "Technology Name", "category": "Framework/CMS/Analytics", "confidence": "High/Medium" }
            ],
            "focus_scores": [
                { "subject": "Content", "current": 4, "recommended": 8 },
                { "subject": "Technical", "current": 6, "recommended": 9 },
                { "subject": "UX/UI", "current": 5, "recommended": 7 },
                { "subject": "Backlinks", "current": 3, "recommended": 6 },
                { "subject": "Performance", "current": 7, "recommended": 10 }
            ],
            "widgets": {
                "health_score": {
                    "total_pages": 48,
                    "avg_response_time": "1.85s",
                    "critical_errors": 12,
                    "accessibility_score": "Low"
                }
            },
            "content_strategy": [
                {
                   "topic": "Suggested Topic",
                   "keyword": "focus keyword",
                   "rationale": "High volume, low competition gap."
                }
            ],
            "roadmap": [
                {
                    "phase": "Phase 1: Foundation (Fix Critical Errors)",
                    "tasks": [
                        { "title": "Task Name", "impact": "High", "effort": "Low", "description": "Brief actionable description." }
                    ]
                },
                 {
                    "phase": "Phase 2: Optimization (Warnings & Growth)",
                    "tasks": [
                        { "title": "Task Name", "impact": "Medium", "effort": "Medium", "description": "Description." }
                    ]
                }
            ]
        }
        """

        prompt = f"""
        You are a World-Class SEO Consultant & Growth Hacker. Analyze the technical audit data below to create a high-impact strategic roadmap.
        
        **Website Context:**
        - URL: {audit_data['stats'].get('base_url')}
        - Pages Crawled: {audit_data['stats'].get('crawled')}
        - Score: {audit_data['stats'].get('score')}
        
        **Issue Summary:**
        - Critical Errors: {len(errors)} 
        - Warnings: {len(warnings)}
        
        **Top Issues (Prioritized):**
        {json.dumps(prioritized_issues, indent=2)}
        
        **Top URLs:**
        {json.dumps(audit_data['urls'][:10], indent=2)}
        
        ---
        **YOUR TASK:**
        Generate a JSON object strictly following the structure below. Do not include markdown keys or extra text.

        {json_template}
        
        **GENERATION RULES:**

        1. **Executive Summary**: 
           - Don't just list errors. Explain the *business impact* (e.g., "Critical load time issues are likely killing conversion rates").
           - Be direct and professional.
        
        2. **Site Goal**: 
           - Infer the business model (SaaS, E-commerce, Local Service, Blog) based on the URL and findings.
        
        3. **Focus Scores (0-10)**:
           - **Content**: Low if word counts are low or meta tags missing.
           - **Technical**: 10 - (Critical Errors / 2). Max 10.
           - **Performance**: Infer from response times (if available) or set to 7 as baseline.
        
        4. **Content Strategy**:
           - Suggest 3 concrete topics with **High Commercial Intent**.
           - Avoid generic advice like "Write a blog". Be specific: "Create a comparison guide for X vs Y".
        
        5. **Roadmap**:
           - **Phase 1 (Foundation)**: Address the "Bleeding Neck" issues (Critical errors, 404s, 500s) that stop indexing/ranking.
           - **Phase 2 (Growth)**: Optimization tasks (Meta tags, content expansion) to improve visibility.
        """


        response_text = await self._generate_content(prompt)
        
        # Clean up response to ensure valid JSON
        cleaned_text = response_text.replace('```json', '').replace('```', '').strip()
        
        if cleaned_text.startswith("Error:"):
            logger.error(f"AI Service explicit error: {cleaned_text}")
            return {
                "executive_summary": f"Could not generate insights: {cleaned_text}",
                "site_goal": "Unknown",
                "focus_areas": [],
                "strategic_roadmap": [],
                "error": cleaned_text 
            }

        try:
            result = json.loads(cleaned_text)
            # Inject URL for frontend use (e.g., Keyword Density)
            result['url'] = audit_data['stats'].get('base_url')
            return result
        except json.JSONDecodeError:
            logger.error(f"Failed to parse AI insights JSON: {cleaned_text[:100]}...")
            return {
                "executive_summary": "Could not generate insights at this time.",
                "site_goal": "Unknown",
                "focus_scores": [],
                "roadmap": [],
                "url": audit_data['stats'].get('base_url') # Ensure URL is returned even on error
            }

    async def chat_with_audit(self, question, audit_data):
        """
        Chat with the audit data to answer user questions.
        """
        if not self.model:
             return {"answer": "AI service is not initialized.", "status": "error"}

        # Construct a condensed context to fit in prompt
        stats = audit_data.get('stats', {})
        issues = audit_data.get('issues', [])
        
        # Summarize top issues
        top_issues = [i for i in issues if i.get('severity') == 'high'][:10]
        issue_summary = "\n".join([f"- {i.get('title')}: {i.get('description')}" for i in top_issues])
        
        prompt = f"""
        You are an SEO Expert Assistant. You are analyzing a website audit for {stats.get('base_url', 'this site')}.
        
        **Audit Context:**
        - Total Pages: {stats.get('crawled')}
        - Score: {stats.get('score')}
        
        **Critical Issues Found:**
        {issue_summary}
        
        **User Question:**
        "{question}"
        
        **Your Answer:**
        Provide a helpful, data-backed answer based on the context above. Use bullet points if listing items. Keep it concise (under 150 words).
        If the question is about specific pages, admit if you don't have that granular detail but offer general advice.
        """
        
        try:
            response = await self._generate_content(prompt)
            return {"answer": response, "status": "success"}
        except Exception as e:
            logger.error(f"Chat error: {str(e)}")
            return {"answer": "I'm having trouble analyzing that right now.", "status": "error"}

    async def generate_llms_txt(self, base_url: str, sitemap_urls: List[str]) -> str:
        """
        Generate an llms.txt file based on sitemap URLs.
        Standard: https://llmstxt.org/
        """
        if not self.is_available():
            return "# llms.txt\n\n> AI Service unavailable to generate content."

        # Limit urls to avoid overflow
        top_urls = sitemap_urls[:100]
        site_name = base_url.replace('https://', '').replace('http://', '').replace('www.', '').split('/')[0]
        
        # Analyze URL patterns for subdomains
        from urllib.parse import urlparse
        subdomains = set()
        main_pages = []
        for url in top_urls:
            parsed = urlparse(url)
            host_parts = parsed.netloc.split('.')
            if len(host_parts) > 2:
                subdomains.add(parsed.netloc)
            else:
                main_pages.append(url)
        
        subdomain_list = list(subdomains)[:20]
        main_page_list = main_pages[:30]
        
        from datetime import date
        today = date.today().isoformat()

        prompt = f"""
You are an AI assistant that creates comprehensive llms.txt files for websites.
These files help LLMs (AI assistants, search agents) understand a website's structure and purpose.

**Target Website:** {base_url}
**Site Name:** {site_name}

**Available URLs from Sitemap:**
{json.dumps(main_page_list, indent=2)}

**Detected Subdomains:**
{json.dumps(subdomain_list, indent=2) if subdomain_list else "None detected"}

**Create a comprehensive llms.txt file using this EXACT format:**

```
# llms.txt — {site_name}
# Purpose: Help LLMs (AI assistants, search agents) understand {site_name} and its structure.
# Site Type: [Infer from URLs - e.g., E-commerce, Blog, Multi-site platform, Corporate, etc.]
# Primary Domain: {base_url}

site_name: {site_name}
site_url: {base_url}
site_type: [Describe the type of website based on URL patterns]
publisher: [Infer or use site name]
region: [Infer from domain or content, e.g., India, USA, Global]
language: [Primary language, usually English]

description:
[Write a 2-4 sentence description of what the site is about based on the URLs and structure]

primary_pages:
- homepage: {base_url}
- sitemap: {base_url}sitemap.xml
[Add other key pages discovered]

subdomain_structure:
[If subdomains exist, explain the pattern. If not, state "No subdomains detected"]

main_sections:
[List main sections/categories found in the URLs with their URLs]

content_types:
[Describe what types of content the site likely contains based on URL patterns]

navigation_and_priority:
- Use the sitemap for complete page discovery
- [Add 2-3 more navigation tips]

guidelines_for_llms:
- [Add 4-5 specific guidelines for how LLMs should use this site's content]
- Be factual and cite sources
- [More guidelines based on site type]

recommended_citation:
When referencing information from {site_name}, cite the exact page URL.
Example: "According to {base_url}..., ..."

last_updated: {today}
```

**IMPORTANT INSTRUCTIONS:**
1. Generate ONLY the raw text content - NO markdown code blocks (no ```).
2. Analyze the URL patterns to understand the site structure.
3. Be specific and detailed in each section.
4. Use the exact format shown above with proper indentation.
5. If subdomains are detected, include a detailed subdomain_structure section.
6. Make the guidelines_for_llms section practical and specific to this site type.
7. Infer as much as possible from the URL patterns.
"""
        
        try:
            content = await self._generate_content(prompt)
            # Cleanup potential markdown fences if model ignores instruction
            content = content.replace('```markdown', '').replace('```', '').replace('```text', '').strip()
            return content
        except Exception as e:
            logger.error(f"Failed to generate llms.txt: {e}")
            return f"# {site_name}\n\n> Error generating content: {str(e)}"
