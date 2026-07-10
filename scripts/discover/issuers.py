"""
Discovery Targets Registry
==========================

Không scrape.

Không parser.

Chỉ quản lý danh sách issuer cần discover.

Các module khác sẽ đọc registry này.
"""

from dataclasses import dataclass, field
from typing import List, Dict


# ============================================================
# DATA MODEL
# ============================================================

@dataclass(slots=True)
class DiscoveryTarget:

    ticker: str

    issuer: str

    asset: str

   
