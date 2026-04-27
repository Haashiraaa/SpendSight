

# src/models/aliases.py

from typing import Tuple
from haashi_pkg.data_engine import DataFrame

AnalysisLike = Tuple[DataFrame, DataFrame, float, int, float]
