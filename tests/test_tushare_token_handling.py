from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCES = [
    REPO_ROOT / "backtest" / "data_pipeline.py",
    REPO_ROOT / "backtest" / "B-S_backtest.py",
    REPO_ROOT / "backtest" / "Z-L_backtest_GPU_prod.py",
    REPO_ROOT / "long-short strategy" / "update_benchmark.py",
]


def test_tushare_clients_do_not_persist_token_to_user_home() -> None:
    for path in SOURCES:
        source = path.read_text(encoding="utf-8")
        assert "ts.set_token(" not in source, path
        assert "ts.pro_api(" in source, path
