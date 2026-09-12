"""Independent weekly value strategy: 5bp per side, 2% net valuation margin.

Reads existing pricing and daily inputs; never runs model or factor scripts.
"""
from __future__ import annotations
import io
import sys

if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from weekly_ensemble_strategy import main

if __name__ == "__main__":
    main(value_strategy=True)
