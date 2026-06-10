"""
token_loader.py — Tushare token 统一加载入口。

token 不允许写入任何会被 git 追踪的文件。加载优先级:
  1. 环境变量 TUSHARE_TOKEN
  2. backtest/tushare_token.txt（已加入 .gitignore，仅存于本机）
"""

import os

_TOKEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tushare_token.txt")


def load_tushare_token() -> str:
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if token:
        return token
    if os.path.exists(_TOKEN_FILE):
        with open(_TOKEN_FILE, encoding="utf-8") as f:
            token = f.read().strip()
        if token:
            return token
    raise RuntimeError(
        "未找到 Tushare token。请设置环境变量 TUSHARE_TOKEN，"
        f"或将 token 写入 {_TOKEN_FILE}（该文件不会被提交到 git）。"
    )
