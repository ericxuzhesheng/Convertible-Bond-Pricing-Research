"""
Z-L 6因子复合策略回测 — 入口包装。

实现见同目录 mispricing_factor_core.py（BS/ZL/LSM 共享核心，消除了多份因子回测的复制粘贴）。
输出: Z-L_alpha_strategy_results.csv / Z-L_alpha_strategy_chart*.png / ZL_factor_correlation.png 等。
"""

from mispricing_factor_core import main

if __name__ == "__main__":
    main(model="ZL")
