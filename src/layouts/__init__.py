"""
Tab layouts for the dashboard.
"""

from .header import create_header
from .clv_layout import create_clv_layout
from .vip_layout import create_vip_layout
from .segmentation_layout import create_segmentation_layout
from .customer360_layout import create_customer360_layout
from .churn_layout import create_churn_layout

__all__ = [
    'create_header',
    'create_clv_layout',
    'create_vip_layout',
    'create_segmentation_layout',
    'create_customer360_layout',
    'create_churn_layout'
]

