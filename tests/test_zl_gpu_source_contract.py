from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GPU_SOURCE = (
    REPO_ROOT / "backtest" / "Z-L_backtest_GPU_prod.py"
).read_text(encoding="utf-8")
DAILY_SOURCE = (
    REPO_ROOT / "backtest" / "daily_signal.py"
).read_text(encoding="utf-8")
LEGACY_CPU_PATH = REPO_ROOT / "backtest" / "Z-L_backtest_CPU.py"
EXPERIMENTAL_GPU_SOURCE = (
    REPO_ROOT / "backtest" / "Z-L_backtest_GPU.py"
).read_text(encoding="utf-8")
BS_SOURCE = (
    REPO_ROOT / "backtest" / "B-S_backtest.py"
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
    assert (
        "if os.path.exists(SUMMARY_FILE) and not REBUILD_ALL "
        "and can_reuse_history:"
        in GPU_SOURCE
    )
    assert "load_rebuildable_matrix_cache" in GPU_SOURCE
    assert "refresh_cache=REFRESH_INPUT_CACHE" in GPU_SOURCE


def test_full_rebuild_has_an_explicit_pricing_coverage_gate() -> None:
    assert "ZL_MIN_REBUILD_COVERAGE" in GPU_SOURCE
    assert "rebuild coverage" in GPU_SOURCE


def test_daily_signal_never_invokes_cpu_fallback() -> None:
    assert "Z-L_backtest_CPU.py" not in DAILY_SOURCE


def test_legacy_cpu_entrypoint_is_removed() -> None:
    assert not LEGACY_CPU_PATH.exists()


def test_gpu_production_source_does_not_reference_removed_cpu() -> None:
    assert "Z-L_backtest_CPU.py" not in GPU_SOURCE


def test_legacy_experimental_gpu_cannot_emit_assumption_based_prices() -> None:
    assert "LEGACY_GPU_DISABLED" in EXPERIMENTAL_GPU_SOURCE
    assert "df_volatility.fillna(0.40)" not in EXPERIMENTAL_GPU_SOURCE
    assert "pd.DataFrame(0.02" not in EXPERIMENTAL_GPU_SOURCE
    assert "get_credit_spread_by_maturity" not in EXPERIMENTAL_GPU_SOURCE


def test_bs_model_rebuild_does_not_implicitly_clear_input_cache() -> None:
    assert "REBUILD_ALL = '--rebuild-all' in sys.argv" in BS_SOURCE
    assert "REFRESH_INPUT_CACHE = '--refresh-input-cache' in sys.argv" in BS_SOURCE
    assert "load_rebuildable_matrix_cache" in BS_SOURCE
    assert "refresh_cache=REFRESH_INPUT_CACHE" in BS_SOURCE


def test_pro_bar_uses_the_authenticated_client() -> None:
    assert "api=pro" in BS_SOURCE
    assert "api=pro" in GPU_SOURCE


def test_production_models_have_weekly_mode_and_coverage_gates() -> None:
    for source in (BS_SOURCE, GPU_SOURCE):
        assert "WEEKLY_ONLY = '--weekly' in sys.argv" in source
        assert "select_completed_weekly_dates" in source
        assert "validate_pricing_coverage" in source


def test_zl_reuses_only_history_with_current_contract_manifest() -> None:
    assert "ZL_INPUT_CONTRACT_VERSION" in GPU_SOURCE
    assert "ZL_Model_Manifest.json" in GPU_SOURCE
    assert "can_reuse_history" in GPU_SOURCE
    assert "verified_dates" in GPU_SOURCE
    assert "_build_input_fingerprint" in GPU_SOURCE
    assert '"input_fingerprint"' in GPU_SOURCE
    assert '"output_sha256"' in GPU_SOURCE
    assert '"model_parameters"' in GPU_SOURCE


def test_zl_uses_official_maturity_call_price_for_terminal_redemption() -> None:
    assert "basic_row.get('maturity_call_price')" in GPU_SOURCE


def test_zl_never_reuses_stored_deviation_sheets() -> None:
    assert "df_diff_hist = pd.read_excel" not in GPU_SOURCE
    assert "df_diff_pct_hist = pd.read_excel" not in GPU_SOURCE
    assert "df_diff = df_zl_model - df_price" in GPU_SOURCE


def test_zl_does_not_require_unused_bps_or_stock_price_inputs() -> None:
    assert "BPS_arr" not in GPU_SOURCE
    assert "cb_bps_cache.csv" not in GPU_SOURCE
    assert "zl_stock_price_cache.csv" not in GPU_SOURCE


def test_bs_and_zl_share_the_verified_volatility_cache() -> None:
    assert 'VOL_CACHE_FILE = os.path.join(PIPELINE_DIR, "bs_volatility_cache.csv")' in GPU_SOURCE
    assert "zl_stock_volatility_cache.csv" not in GPU_SOURCE


def test_zl_fingerprint_schema_contains_only_effective_static_fields() -> None:
    assert "BASIC_FINGERPRINT_FIELDS" in GPU_SOURCE
    assert "CLAUSE_FINGERPRINT_FIELDS" in GPU_SOURCE
    for field in (
        "par_value",
        "value_date",
        "maturity_date",
        "maturity_call_price",
        "rate_clause",
    ):
        assert f'"{field}"' in GPU_SOURCE
    assert '"remain_size"' not in GPU_SOURCE


def test_gpu_plot_does_not_replace_missing_errors_with_zero() -> None:
    assert (
        "replace([np.inf, -np.inf], np.nan).fillna(0)"
        not in GPU_SOURCE
    )
