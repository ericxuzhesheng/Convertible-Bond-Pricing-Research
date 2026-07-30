from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTEST_DIR = REPO_ROOT / "backtest"
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"


def test_daily_signal_automation_is_removed() -> None:
    for relative_path in (
        "backtest/daily_signal.py",
        "backtest/run_daily.bat",
        "backtest/setup_notification.py",
    ):
        assert not (REPO_ROOT / relative_path).exists()


def test_weekly_batch_does_not_publish_daily_signal() -> None:
    source = (BACKTEST_DIR / "weekly_update.bat").read_text(encoding="utf-8")

    assert "daily_signal.py" not in source
    assert "data_pipeline.py" in source
    assert "B-S_backtest.py" in source
    assert "Z-L_backtest_GPU_prod.py" in source


def test_workflows_only_target_main() -> None:
    post_zl = (WORKFLOWS_DIR / "post-zl-research.yml").read_text(
        encoding="utf-8"
    )

    assert "      - main" in post_zl
    assert "codex/full-backtest-remote-20260728" not in post_zl
    assert not (WORKFLOWS_DIR / "full-history-data-rebuild.yml").exists()


def test_public_docs_do_not_advertise_daily_signal_system() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    assert "每日信号系统" not in readme
    assert "Daily Signal System" not in readme
