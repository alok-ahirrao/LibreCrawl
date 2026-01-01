"""
GMB Core Module
Google Business Profile optimization and grid tracking.
"""
from .config import config
from .models import init_gmb_tables
from .router import gmb_bp

__all__ = ['config', 'init_gmb_tables', 'gmb_bp']
__version__ = '0.2.0'
