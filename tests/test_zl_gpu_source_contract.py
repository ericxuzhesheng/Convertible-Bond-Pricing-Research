from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GPU_SOURCE = (
    REPO_ROOT / "backtest" / "Z-L_backtest_GPU_prod.py"
).read_text(encoding="utf-8")
DAILY_SOURCE = (
    REPO_ROOT / "backtest" / "daily_signal.py"
).read_text(encoding="utf-8")


def test_gpu_production_source_has_no_constant_market_data_fallbacks() -> None:
    forbidden = [
        "df_volatility = df_volatility.fillna(0.40)",
        "pd.DataFrame(0.02, index=df_price.index",
        "get_credit_spread_by_maturity",
        "pending_mask.loc[:\"2026-07-09\"]",
        "redeem_price = 106.0",
        "PUT_PRICE = 100.0",
        "PUT_BARRIER = 0.7",
    ]

    for snippet in forbidden:
        assert snippet not in GPU_SOURCE


def test_gpu_production_source_requires_real_clause_and_spread_caches() -> None:
    assert "cb_clause_terms.csv" in GPU_SOURCE
    assert "cb_credit_spread_cache.csv" in GPU_SOURCE
    assert "put_window_arr" in GPU_SOURCE
    assert "redeem_required_arr" in GPU_SOURCE
    assert "maturity_redem_arr" in GPU_SOURCE
    assert "initial_put_count_arr" in GPU_SOURCE
    assert "initial_redeem_flags_arr" in GPU_SOURCE
    assert "build_clause_history_state" in GPU_SOURCE


def test_full_rebuild_ignores_historical_model_workbook() -> None:
    assert "REBUILD_ALL = '--rebuild-all' in sys.argv" in GPU_SOURCE
    assert "if os.path.exists(SUMMARY_FILE) and not REBUILD_ALL:" in GPU_SOURCE
    assert "load_rebuildable_matrix_cache" in GPU_SOURCE
    assert "rebuild_all=REBUILD_ALL" in GPU_SOURCE


def test_full_rebuild_has_an_explicit_pricing_coverage_gate() -> None:
    assert "ZL_MIN_REBUILD_COVERAGE" in GPU_SOURCE
    assert "rebuild coverage" in GPU_SOURCE


def test_daily_signal_never_invokes_cpu_fallback() -> None:
    assert "Z-L_backtest_CPU.py" not in DAILY_SOURCE
