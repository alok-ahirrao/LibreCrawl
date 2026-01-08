"""
Keyword Research Workflow Orchestrator
Runs the complete 9-step keyword research process.
"""

import asyncio
import logging
from typing import Optional, List, Dict
from datetime import datetime

from .ai_service import GeminiKeywordAI
from .keyword_data import KeywordDataService
from .cannibalization import KeywordCannibalizationDetector
from .content_mapper import ContentMapper

logger = logging.getLogger(__name__)


class KeywordResearchWorkflow:
    """
    Orchestrates the complete keyword research workflow.
    
    9-Step Process:
    1. Define seed keywords (business, service, location)
    2. Collect keyword ideas (primary, secondary, long-tail)
    3. Identify search intent (informational, commercial, transactional)
    4. Analyze keyword difficulty and competition
    5. Estimate search volume and opportunity
    6. Group related keywords into clusters
    7. Detect keyword cannibalization risks
    8. Map keyword clusters to pages or content types
    9. Finalize priority keywords for execution
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        language: str = 'en',
        geo: str = ''
    ):
        """
        Initialize the workflow.
        
        Args:
            api_key: Gemini API key (optional, uses env var)
            language: Language code
            geo: Geographic region code
        """
        self.ai_service = GeminiKeywordAI(api_key)
        self.keyword_data = KeywordDataService(language=language, geo=geo)
        self.cannibalization_detector = KeywordCannibalizationDetector(self.ai_service)
        self.content_mapper = ContentMapper(self.ai_service)
        self.geo = geo
        self.language = language
    
    async def close(self):
        """Close all resources."""
        await self.cannibalization_detector.close()
    
    async def run_full_workflow(
        self,
        business_type: str,
        services: List[str],
        location: str = None,
        domain: str = None,
        business_goal: str = 'traffic',
        max_keywords: int = 100
    ) -> Dict:
        """
        Execute the complete 9-step keyword research workflow.
        
        Args:
            business_type: Type of business (e.g., "dental clinic", "plumber")
            services: List of services offered
            location: Target location (optional)
            domain: Website domain for cannibalization check (optional)
            business_goal: 'traffic', 'conversions', or 'authority'
            max_keywords: Maximum keywords to process
            
        Returns:
            Complete workflow results with all 9 steps
        """
        results = {
            'workflow_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'input': {
                'business_type': business_type,
                'services': services,
                'location': location,
                'domain': domain,
                'business_goal': business_goal
            },
            'steps': {},
            'summary': None,
            'execution_time': None
        }
        
        start_time = datetime.now()
        
        try:
            # ========== STEP 1: Define Seed Keywords ==========
            logger.info("Step 1: Defining seed keywords...")
            seed_keywords = self._define_seed_keywords(business_type, services, location)
            results['steps']['1_seed_keywords'] = {
                'status': 'complete',
                'keywords': seed_keywords,
                'count': len(seed_keywords)
            }
            
            # ========== STEP 2: Collect Keyword Ideas ==========
            logger.info("Step 2: Collecting keyword ideas...")
            discovered = self.keyword_data.discover_keywords(
                seed_keywords=seed_keywords[:5],
                geo=self.geo,
                include_trends=True,
                include_autocomplete=True,
                include_questions=True
            )
            
            # Generate long-tail variations for top seeds
            long_tail_all = []
            for seed in seed_keywords[:3]:
                long_tails = self.keyword_data.generate_long_tail_keywords(
                    seed,
                    include_alphabet_soup=False,  # Skip for speed
                    include_patterns=True,
                    include_modifiers=True
                )
                long_tail_all.extend(long_tails)
            
            # Combine all keywords
            all_keywords = []
            seen = set()
            
            # Add discovered keywords
            for kw in discovered.get('discovered_keywords', []):
                kw_text = kw.get('keyword', '').lower()
                if kw_text and kw_text not in seen:
                    seen.add(kw_text)
                    all_keywords.append(kw)
            
            # Add autocomplete
            for kw in discovered.get('autocomplete_suggestions', []):
                kw_text = kw.get('keyword', '').lower()
                if kw_text and kw_text not in seen:
                    seen.add(kw_text)
                    all_keywords.append(kw)
            
            # Add trending
            for kw in discovered.get('trending_keywords', []):
                kw_text = kw.get('keyword', '').lower()
                if kw_text and kw_text not in seen:
                    seen.add(kw_text)
                    all_keywords.append(kw)
            
            # Add questions
            for kw in discovered.get('questions', []):
                kw_text = kw.get('keyword', '').lower()
                if kw_text and kw_text not in seen:
                    seen.add(kw_text)
                    all_keywords.append(kw)
            
            # Add long-tail
            for kw in long_tail_all:
                kw_text = kw.get('keyword', '').lower()
                if kw_text and kw_text not in seen:
                    seen.add(kw_text)
                    all_keywords.append(kw)
            
            # Limit total keywords
            all_keywords = all_keywords[:max_keywords]
            
            results['steps']['2_keyword_collection'] = {
                'status': 'complete',
                'total_collected': len(all_keywords),
                'from_autocomplete': len(discovered.get('autocomplete_suggestions', [])),
                'from_trends': len(discovered.get('trending_keywords', [])),
                'questions': len(discovered.get('questions', [])),
                'long_tail': len(long_tail_all),
                'keywords': all_keywords[:50]  # Sample
            }
            
            # ========== STEP 3: Identify Search Intent ==========
            logger.info("Step 3: Classifying search intent...")
            keyword_texts = [kw.get('keyword', '') for kw in all_keywords if kw.get('keyword')]
            intent_classification = await self.ai_service.classify_intent(keyword_texts)
            
            # Add intent to each keyword
            for kw in all_keywords:
                kw_text = kw.get('keyword', '')
                for intent_type, intent_keywords in intent_classification.items():
                    if kw_text in intent_keywords:
                        kw['intent'] = intent_type
                        break
                if 'intent' not in kw:
                    kw['intent'] = 'informational'  # Default
            
            results['steps']['3_intent_classification'] = {
                'status': 'complete',
                'distribution': {
                    intent: len(kws) for intent, kws in intent_classification.items()
                },
                'sample': {
                    intent: kws[:5] for intent, kws in intent_classification.items()
                }
            }
            
            # ========== STEP 4: Analyze Difficulty & Competition ==========
            logger.info("Step 4: Analyzing difficulty and competition...")
            # Use rule-based scoring (AI for individual keywords is too slow)
            enriched_keywords = self.ai_service.enrich_keywords_with_scores(all_keywords)
            
            results['steps']['4_difficulty_analysis'] = {
                'status': 'complete',
                'high_opportunity': len([k for k in enriched_keywords if k.get('opportunity_score', 0) >= 70]),
                'medium_opportunity': len([k for k in enriched_keywords if 40 <= k.get('opportunity_score', 0) < 70]),
                'low_opportunity': len([k for k in enriched_keywords if k.get('opportunity_score', 0) < 40]),
                'top_opportunities': enriched_keywords[:10]
            }
            
            # ========== STEP 5: Estimate Search Volume & Opportunity ==========
            logger.info("Step 5: Estimating search volume and opportunity...")
            # Volume is estimated through traffic_potential labels
            volume_distribution = {
                'very_high': 0,
                'high': 0,
                'medium': 0,
                'low': 0,
                'very_low': 0
            }
            
            for kw in enriched_keywords:
                potential = kw.get('traffic_potential', 'medium')
                if potential in volume_distribution:
                    volume_distribution[potential] += 1
            
            results['steps']['5_volume_estimation'] = {
                'status': 'complete',
                'volume_distribution': volume_distribution,
                'high_volume_keywords': [
                    k for k in enriched_keywords 
                    if k.get('traffic_potential') in ['very_high', 'high']
                ][:15]
            }
            
            # ========== STEP 6: Group Keywords into Clusters ==========
            logger.info("Step 6: Clustering keywords...")
            clustering_result = await self.ai_service.group_keywords(keyword_texts)
            clusters = clustering_result.get('groups', [])
            
            results['steps']['6_keyword_clustering'] = {
                'status': 'complete',
                'total_clusters': len(clusters),
                'clusters': clusters[:20]  # Top 20 clusters
            }
            
            # ========== STEP 7: Detect Cannibalization ==========
            logger.info("Step 7: Checking for cannibalization...")
            cannibalization_results = None
            
            if domain:
                try:
                    cannibalization_results = await self.cannibalization_detector.analyze_domain(
                        domain,
                        max_pages=20
                    )
                except Exception as e:
                    logger.error(f"Cannibalization check failed: {e}")
                    cannibalization_results = {'error': str(e)}
            else:
                cannibalization_results = {
                    'skipped': True,
                    'reason': 'No domain provided for cannibalization check'
                }
            
            results['steps']['7_cannibalization_detection'] = {
                'status': 'complete' if cannibalization_results else 'skipped',
                'results': cannibalization_results
            }
            
            # ========== STEP 8: Map Clusters to Content Types ==========
            logger.info("Step 8: Mapping clusters to content types...")
            content_mappings = self.content_mapper.map_clusters_to_content(
                clusters,
                intent_data=intent_classification
            )
            
            # Prioritize content creation
            prioritized_content = self.content_mapper.prioritize_content_creation(
                content_mappings,
                business_goal=business_goal
            )
            
            # Generate content calendar
            calendar = self.content_mapper.create_content_calendar(
                prioritized_content[:12],  # Next 12 pieces
                posts_per_week=2
            )
            
            # Count total content gaps across all mappings
            total_content_gaps = sum(len(m.get('content_gaps', [])) for m in content_mappings)
            weeks_of_content = max([c.get('week', 0) for c in calendar]) if calendar else 0
            
            results['steps']['8_content_mapping'] = {
                'status': 'complete',
                'total_mappings': len(content_mappings),
                'content_gaps': total_content_gaps,  # Now counts actual content gaps
                'weeks_of_content': weeks_of_content,
                'mappings': content_mappings[:15],
                'priority_content': prioritized_content[:10],
                'content_calendar': calendar
            }
            
            # ========== STEP 9: Finalize Priority Keywords ==========
            logger.info("Step 9: Finalizing priority keywords...")
            
            # Tier A: High opportunity + commercial/transactional/local intent
            # Lowered threshold from 70 to 65 to capture more keywords
            tier_a = [
                k for k in enriched_keywords
                if k.get('opportunity_score', 0) >= 65 
                and k.get('intent') in ['transactional', 'commercial', 'local']
            ][:15]
            
            # Tier B: Good opportunity + any intent
            # Lowered threshold from 60 to 55
            tier_b = [
                k for k in enriched_keywords
                if k.get('opportunity_score', 0) >= 55
                and k not in tier_a
            ][:20]
            
            # Tier C: Medium opportunity
            tier_c = [
                k for k in enriched_keywords
                if 40 <= k.get('opportunity_score', 0) < 55
                and k not in tier_a and k not in tier_b
            ][:25]
            
            # Quick wins: Easy keywords with decent traffic OR question keywords
            quick_wins = [
                k for k in enriched_keywords
                if (k.get('competition', 'medium') in ['low', 'very_low']
                    and k.get('traffic_potential', 'medium') in ['high', 'very_high', 'medium'])
                or (k.get('intent') == 'question' and k.get('opportunity_score', 0) >= 50)
                or any(q in k.get('keyword', '').lower() for q in ['how to', 'what is', 'why'])
            ][:10]
            
            results['steps']['9_priority_finalization'] = {
                'status': 'complete',
                'tier_a': {
                    'description': 'Highest priority - High opportunity with commercial intent',
                    'count': len(tier_a),
                    'keywords': tier_a
                },
                'tier_b': {
                    'description': 'Secondary priority - Good opportunity across intents',
                    'count': len(tier_b),
                    'keywords': tier_b
                },
                'tier_c': {
                    'description': 'Tertiary priority - Medium opportunity',
                    'count': len(tier_c),
                    'keywords': tier_c
                },
                'quick_wins': {
                    'description': 'Easy to rank with decent traffic',
                    'count': len(quick_wins),
                    'keywords': quick_wins
                }
            }
            
            # ========== Generate Summary ==========
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            results['execution_time'] = f"{execution_time:.1f} seconds"
            results['summary'] = self._generate_summary(results)
            results['status'] = 'complete'
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            results['status'] = 'failed'
            results['error'] = str(e)
        
        return results
    
    def _define_seed_keywords(
        self,
        business_type: str,
        services: List[str],
        location: str = None
    ) -> List[str]:
        """
        Define seed keywords from business info.
        
        Step 1 of the workflow.
        """
        seeds = []
        
        # Normalize inputs
        business = business_type.lower().strip()
        
        # Normalize location (remove commas, extra spaces)
        loc = None
        if location:
            loc = ' '.join(location.replace(',', ' ').split()).lower()
        
        # Base business keywords
        seeds.append(business)
        
        # Service-based seeds
        for service in services:
            svc = service.lower().strip()
            if svc and svc != business:  # Avoid duplicating business type
                seeds.append(svc)
                # Only add combo if different from base
                combo = f"{business} {svc}"
                if combo != business and combo != svc:
                    seeds.append(combo)
        
        # Location-based seeds
        if loc:
            seeds.append(f"{business} {loc}")
            seeds.append(f"{business} near me")
            for service in services[:3]:  # Limit location combos
                svc = service.lower().strip()
                if svc:
                    seeds.append(f"{svc} {loc}")
        
        # Intent-based seeds
        intent_modifiers = ['best', 'top', 'professional', 'affordable']
        for mod in intent_modifiers:
            seeds.append(f"{mod} {business}")
        
        # Deduplicate while preserving order
        seen = set()
        unique_seeds = []
        for s in seeds:
            # Normalize whitespace
            s = ' '.join(s.split())
            if s and s not in seen and len(s) > 2:
                seen.add(s)
                unique_seeds.append(s)
        
        return unique_seeds
    
    def _generate_summary(self, results: Dict) -> Dict:
        """Generate executive summary of the workflow."""
        steps = results.get('steps', {})
        
        return {
            'total_keywords_discovered': steps.get('2_keyword_collection', {}).get('total_collected', 0),
            'high_opportunity_keywords': steps.get('4_difficulty_analysis', {}).get('high_opportunity', 0),
            'clusters_created': steps.get('6_keyword_clustering', {}).get('total_clusters', 0),
            'content_gaps_identified': steps.get('8_content_mapping', {}).get('content_gaps', 0),
            'tier_a_keywords': steps.get('9_priority_finalization', {}).get('tier_a', {}).get('count', 0),
            'quick_wins': steps.get('9_priority_finalization', {}).get('quick_wins', {}).get('count', 0),
            'cannibalization_issues': steps.get('7_cannibalization_detection', {}).get('results', {}).get('cannibalization_issues', {}).get('total', 0),
            'recommended_next_steps': [
                'Start creating content for Tier A keywords',
                'Follow the content calendar for systematic publishing',
                'Address any high-severity cannibalization issues',
                'Target quick-win keywords for early momentum'
            ]
        }


# Convenience function for running workflow
async def run_keyword_research(
    business_type: str,
    services: List[str],
    location: str = None,
    domain: str = None,
    business_goal: str = 'traffic',
    geo: str = '',
    api_key: str = None
) -> Dict:
    """
    Convenience function to run the full keyword research workflow.
    
    Args:
        business_type: Type of business
        services: List of services
        location: Target location
        domain: Website domain
        business_goal: 'traffic', 'conversions', or 'authority'
        geo: Geographic region code
        api_key: Optional Gemini API key
        
    Returns:
        Complete workflow results
    """
    workflow = KeywordResearchWorkflow(api_key=api_key, geo=geo)
    
    try:
        results = await workflow.run_full_workflow(
            business_type=business_type,
            services=services,
            location=location,
            domain=domain,
            business_goal=business_goal
        )
        return results
    finally:
        await workflow.close()
