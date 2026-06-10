"""
B-S 6因子复合策略回测 — 入口包装。

实现见同目录 mispricing_factor_core.py（BS/ZL 共享核心，消除了两份 1200+ 行的复制粘贴）。
输出: B-S_alpha_strategy_results.csv / B-S_alpha_strategy_chart*.png / BS_factor_correlation.png 等。
"""

from mispricing_factor_core import main

if __name__ == "__main__":
    main(model="BS")
