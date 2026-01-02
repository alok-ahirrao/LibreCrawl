"""
Grid Engine
Manages grid generation and parallel execution of grid scans.
"""
import time
import math
import random
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple

from ..models import get_db, save_serp_cache, get_cached_serp
from ..config import config
from .geo_driver import GeoCrawlerDriver
from .parsers import GoogleMapsParser


class GridEngine:
    """
    Engine for executing grid-based rank tracking scans.
    """
    
    def __init__(self, max_workers: int = 3):  # Reduced to 3 for stability
        self.max_workers = max_workers
        self.parser = GoogleMapsParser()
        print(f"[GridEngine] Initialized with {max_workers} workers")
    
    def generate_grid(
        self, 
        center_lat: float, 
        center_lng: float, 
        radius_meters: float, 
        grid_size: int = 5,
        shape: str = 'square'
    ) -> List[Tuple[int, float, float]]:
        """
        Generate a grid of coordinates around a center point.
        
        Args:
            center_lat: Center latitude
            center_lng: Center longitude
            radius_meters: Radius from center to edge in meters
            grid_size: Number of points per side (e.g., 5 = 5x5 = 25 points). 
                       For circle, this determines the density/rings.
            shape: 'square' or 'circle'
            
        Returns:
            List of (index, lat, lng) tuples
        """
        points = []
        
        # Convert meters to degrees (approximate)
        # At equator: 1 degree lat ≈ 111,320 meters
        meters_per_degree_lat = 111320
        meters_per_degree_lng = 111320 * math.cos(math.radians(center_lat))
        
        # Circular (Radial) Grid Generation
        if shape == 'circle':
            # Always add center point first
            points.append((0, round(center_lat, 6), round(center_lng, 6)))
            
            if grid_size <= 1:
                return points
                
            # Determine rings based on grid_size mapping
            # 3x3 (9pts) -> 1 ring
            # 5x5 (25pts) -> 2 rings
            # 7x7 (49pts) -> 3 rings
            num_rings = (grid_size - 1) // 2
            
            index = 1
            for r in range(1, num_rings + 1):
                # Radius for this ring
                current_radius = (r / num_rings) * radius_meters
                
                # Number of points in this ring = 8 * ring_index
                points_in_ring = 8 * r
                
                for i in range(points_in_ring):
                    # Distribute points evenly
                    angle = (2 * math.pi * i) / points_in_ring
                    
                    # Calculate offset in meters
                    # Using sin for Lat (North) and cos for Lng (East) to start from Top
                    dy = current_radius * math.sin(angle)
                    dx = current_radius * math.cos(angle)
                    
                    # Convert to degrees
                    d_lat = dy / meters_per_degree_lat
                    d_lng = dx / meters_per_degree_lng
                    
                    lat = center_lat + d_lat
                    lng = center_lng + d_lng
                    
                    points.append((index, round(lat, 6), round(lng, 6)))
                    index += 1
            
            return points

        # Square Grid Generation (Default)
        radius_degrees_lat = radius_meters / meters_per_degree_lat
        radius_degrees_lng = radius_meters / meters_per_degree_lng
        
        # Calculate step size
        if grid_size > 1:
            step_lat = (2 * radius_degrees_lat) / (grid_size - 1)
            step_lng = (2 * radius_degrees_lng) / (grid_size - 1)
        else:
            step_lat = step_lng = 0
        
        # Generate grid points
        index = 0
        for row in range(grid_size):
            for col in range(grid_size):
                lat = center_lat - radius_degrees_lat + (row * step_lat)
                lng = center_lng - radius_degrees_lng + (col * step_lng)
                points.append((index, round(lat, 6), round(lng, 6)))
                index += 1
        
        return points
    
    def _scan_single_point(
        self, 
        scan_id: int,
        point_index: int,
        keyword: str, 
        lat: float, 
        lng: float,
        target_place_id: str = None,
        target_business_name: str = None
    ) -> dict:
        """
        Scan a single grid point.
        
        Returns:
            dict with scan result
        """
        result = {
            'scan_id': scan_id,
            'point_index': point_index,
            'lat': lat,
            'lng': lng,
            'target_rank': None,
            'target_found': False,
            'top_results': [],
            'error': None
        }
        
        try:
            # Check cache first
            cached = get_cached_serp(keyword, lat, lng)
            if cached:
                result['top_results'] = cached
                result['cached'] = True
            else:
                # Perform fresh crawl
                driver = GeoCrawlerDriver(
                    headless=config.CRAWLER_HEADLESS,
                    proxy_url=config.PROXY_URL if config.PROXY_ENABLED else None
                )
                
                html = driver.scan_grid_point(keyword, lat, lng)
                
                if html:
                    parsed_results = self.parser.parse_list_results(html)
                    result['top_results'] = parsed_results[:20]  # Top 20
                    
                    # Cache the results
                    save_serp_cache(keyword, lat, lng, parsed_results, config.CACHE_TTL_SERP_RESULT)
                    result['cached'] = False
                else:
                    result['error'] = 'Failed to fetch results'
            
            # Find target business rank
            if result['top_results']:
                # First try place_id match (exact)
                # First try place_id match (exact)
                if target_place_id:
                    print(f"[GridEngine] Checking target ID: {target_place_id}")
                    for r in result['top_results']:
                        rid = r.get('place_id')
                        # Debug extracted IDs
                        # print(f"  -> Candidate ID: {rid} | Name: {r.get('name')}") 
                        if rid == target_place_id:
                            print(f"[GridEngine] MATCH FOUND by ID! {rid}")
                            result['target_rank'] = r['rank']
                            result['target_found'] = True
                            break
                
                # If not found by place_id, try name-based matching (fuzzy)
                if not result['target_found'] and target_business_name:
                    print(f"[GridEngine] Fallback to name match for: {target_business_name}")
                    target_lower = target_business_name.lower().strip()
                    for r in result['top_results']:
                        result_name = r.get('name', '').lower().strip()
                        # print(f"  -> Comparing '{target_lower}' vs '{result_name}'")
                        
                        # Check for substring match (either direction) or high similarity
                        if (target_lower in result_name or 
                            result_name in target_lower or
                            self._name_similarity(target_lower, result_name) > 0.7):
                            
                            print(f"[GridEngine] MATCH FOUND by Name! {result_name}")
                            result['target_rank'] = r['rank']
                            result['target_found'] = True
                            break
            
        except Exception as e:
            result['error'] = str(e)
        
        # Rate limiting delay
        time.sleep(random.uniform(2.0, 5.0) / config.CRAWLER_RATE_LIMIT)
        
        return result
    
    def _name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate simple similarity ratio between two business names.
        Uses a basic approach: ratio of matching words.
        """
        if not name1 or not name2:
            return 0.0
        
        # Normalize names
        words1 = set(name1.lower().split())
        words2 = set(name2.lower().split())
        
        # Remove common filler words
        filler_words = {'the', 'a', 'an', 'and', '&', 'of', 'in', 'at', 'llc', 'inc', 'ltd', 'pvt'}
        words1 = words1 - filler_words
        words2 = words2 - filler_words
        
        if not words1 or not words2:
            return 0.0
        
        # Calculate Jaccard similarity
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return intersection / union if union > 0 else 0.0
    
    def execute_scan(
        self,
        scan_id: int,
        keyword: str,
        center_lat: float,
        center_lng: float,
        radius_meters: float,
        grid_size: int,
        target_place_id: str = None,
        target_business_name: str = None,
        grid_shape: str = 'square'
    ) -> List[dict]:
        """
        Execute a full grid scan.
        
        Args:
            scan_id: Database ID of the scan record
            keyword: Search keyword
            center_lat: Center latitude
            center_lng: Center longitude
            radius_meters: Scan radius in meters
            grid_size: Grid dimension (e.g., 5 for 5x5)
            target_place_id: Optional place ID to track ranking for
            target_business_name: Optional business name to track ranking for
            
        Returns:
            List of all scan results
        """
        print(f"[GridEngine] execute_scan called - scan_id={scan_id}, keyword='{keyword}', target_business='{target_business_name}'")
        
        # Generate grid points
        points = self.generate_grid(center_lat, center_lng, radius_meters, grid_size, grid_shape)
        total_points = len(points)
        
        results = []
        completed = 0
        
        # Update scan status to running
        self._update_scan_status(scan_id, 'running', completed, total_points)
        
        try:
            # Execute scans with limited parallelism
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(
                        self._scan_single_point,
                        scan_id, idx, keyword, lat, lng, target_place_id, target_business_name
                    ): (idx, lat, lng)
                    for idx, lat, lng in points
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    
                    # Save result to database
                    self._save_point_result(result)
                    
                    # Update progress
                    completed += 1
                    self._update_scan_progress(scan_id, completed)
            
            # Mark scan as completed
            self._update_scan_status(scan_id, 'completed', completed, total_points)
            
        except Exception as e:
            print(f"Grid scan {scan_id} error: {e}")
            self._update_scan_status(scan_id, 'failed', completed, total_points)
            raise
        
        return results
    
    def _save_point_result(self, result: dict):
        """Save a single point result to database."""
        import json
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO gmb_grid_results (
                    scan_id, point_index, lat, lng, 
                    target_rank, target_found, top_results, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                result['scan_id'],
                result['point_index'],
                result['lat'],
                result['lng'],
                result['target_rank'],
                1 if result['target_found'] else 0,
                json.dumps(result['top_results']),
                result['error']
            ))
    
    def _update_scan_status(self, scan_id: int, status: str, completed: int, total: int):
        """Update scan status in database."""
        with get_db() as conn:
            cursor = conn.cursor()
            
            if status == 'completed':
                cursor.execute('''
                    UPDATE gmb_grid_scans 
                    SET status = ?, completed_points = ?, completed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status, completed, scan_id))
            else:
                cursor.execute('''
                    UPDATE gmb_grid_scans 
                    SET status = ?, completed_points = ?
                    WHERE id = ?
                ''', (status, completed, scan_id))
    
    def _update_scan_progress(self, scan_id: int, completed: int):
        """Update scan progress."""
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE gmb_grid_scans SET completed_points = ? WHERE id = ?
            ''', (completed, scan_id))
    
    def calculate_grid_stats(self, scan_id: int) -> dict:
        """
        Calculate statistics for a completed grid scan.
        
        Returns:
            dict with stats like avg_rank, rank_distribution, weak_zones
        """
        import json
        
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM gmb_grid_results WHERE scan_id = ?
            ''', (scan_id,))
            results = cursor.fetchall()
        
        if not results:
            return {}
        
        ranks = [r['target_rank'] for r in results if r['target_rank']]
        
        stats = {
            'total_points': len(results),
            'points_with_rank': len(ranks),
            'avg_rank': sum(ranks) / len(ranks) if ranks else None,
            'best_rank': min(ranks) if ranks else None,
            'worst_rank': max(ranks) if ranks else None,
            'rank_distribution': {
                'top_3': len([r for r in ranks if r <= 3]),
                'top_10': len([r for r in ranks if r <= 10]),
                'top_20': len([r for r in ranks if r <= 20]),
                'not_found': len(results) - len(ranks)
            },
            'weak_zones': [
                {'lat': r['lat'], 'lng': r['lng'], 'rank': r['target_rank']}
                for r in results
                if r['target_rank'] and r['target_rank'] > 10
            ]
        }
        
        return stats
