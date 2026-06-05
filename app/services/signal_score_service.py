from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from app.models.momentum_current import MomentumCurrent
from app.models.momentum_structure_current import MomentumStructureCurrent


class SignalScoreService:
    """Centralized final score calculation for SignalMaker.

    The legacy engine can keep producing its raw score and hierarchy context.
    This service normalizes the final score after hierarchy gates, context
