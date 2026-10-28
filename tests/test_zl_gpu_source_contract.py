"""Corrected entrypoints must not expose the retired pricing algorithms."""
from pathlib import Path
import sys
import pytest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"backtest"))
from pricing_backtest import options
from zl_cuda_backend import price_batch_cuda
from zl_cpu_backend import price_batch_cpu


@pytest.mark.parametrize("entry,model",[("B-S_backtest.py","BS"),("Z-L_backtest_GPU_prod.py","ZL"),("Z-L_backtest_CPU_prod.py","ZL"),("LSM_backtest.py","LSM")])
def test_entrypoint_routes_to_independent_corrected_runner(entry,model):
    source=(ROOT/"backtest"/entry).read_text(encoding="utf-8")
    assert f'main("{model}")' in source
    assert "ZL_Model_Prices.csv" not in source
    assert "maximum(" not in source


def test_corrected_cuda_is_explicit_and_retired_apis_stay_disabled():
    assert options("ZL",["--weekly","--backend","cuda"]).backend=='cuda'
    with pytest.raises(SystemExit): options("ZL",["--weekly","--backend","cuda","--sampler","mt19937"])
    for fn in [price_batch_cuda,price_batch_cpu]:
        with pytest.raises(RuntimeError,match="Retired"): fn({})


def test_experimental_entrypoint_remains_disabled():
    assert "LEGACY_GPU_DISABLED" in (ROOT/"backtest/Z-L_backtest_GPU.py").read_text(encoding="utf-8")
