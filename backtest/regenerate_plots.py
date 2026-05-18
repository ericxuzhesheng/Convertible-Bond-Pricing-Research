"""
regenerate_plots.py — 一键重生成 README 全部图表

从现有模型输出 XLSX / CSV 读取数据，不重跑模型计算。
生成 6 张 README 引用图片：
  backtest/Fig1_BS_Price_Time_Series.png
  backtest/Fig1_ZL_Price_Time_Series.png
  long-short strategy/BS_model_performance.png
  long-short strategy/ZL_model_performance.png
  mispricing factor/BS_factor_correlation.png
  mispricing factor/ZL_factor_correlation.png
"""

from __future__ import annotations  # Python 3.9 compatible union types

import sys
import io
import os
import glob
import warnings

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

# ── Windows 控制台 UTF-8 ─────────────────────────────────────────────────────
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── 路径常量 ─────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)

BS_XLSX = os.path.join(SCRIPT_DIR, "BS_Model_Summary.xlsx")
ZL_XLSX = os.path.join(SCRIPT_DIR, "ZL_Model_Summary.xlsx")
LS_DIR = os.path.join(REPO_ROOT, "long-short strategy")
MF_DIR = os.path.join(REPO_ROOT, "mispricing factor")

# XLSX sheet 名称（理论价格 / 市场价格 / 绝对偏差 / 相对偏差）
SHEET_MODEL = "理论价格"
SHEET_MARKET = "市场价格"
SHEET_RELDEV = "相对偏差"

# ── 全局绘图样式 ──────────────────────────────────────────────────────────────
for _style in ("seaborn-v0_8", "seaborn", "ggplot"):
    try:
        plt.style.use(_style)
        break
    except OSError:
        continue
plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def _load_xlsx_wide(path: str, sheet: str) -> pd.DataFrame | None:
    """读取宽格式 XLSX (行=日期, 列=债券代码)，返回 DataFrame 或 None。"""
    if not os.path.exists(path):
        print(f"  [跳过] 文件不存在: {path}")
        return None
    df = pd.read_excel(path, sheet_name=sheet, index_col=0)
    df.index = pd.to_datetime(df.index, errors="coerce")
    df = df[df.index.notnull()]
    df = df.apply(pd.to_numeric, errors="coerce")
    return df


def _align_series(*series: pd.Series) -> tuple[pd.Series, ...]:
    """将多个 Series 对齐到公共非 NaN 索引。"""
    combined = pd.concat(series, axis=1).dropna()
    return tuple(combined.iloc[:, i] for i in range(len(series)))


def _save(path: str) -> None:
    plt.savefig(path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"  saved: {path}")


# ── 图1：BS 时序对比 ──────────────────────────────────────────────────────────

def plot_bs_timeseries() -> None:
    print("[BS] 加载 BS_Model_Summary.xlsx ...")
    df_model = _load_xlsx_wide(BS_XLSX, SHEET_MODEL)
    df_market = _load_xlsx_wide(BS_XLSX, SHEET_MARKET)
    df_reldev = _load_xlsx_wide(BS_XLSX, SHEET_RELDEV)
    if df_model is None or df_market is None or df_reldev is None:
        print("  [跳过] BS 时序图缺少必要 sheet")
        return

    raw_model = df_model.mean(axis=1)
    raw_market = df_market.mean(axis=1)
    raw_err = df_reldev.mean(axis=1) * 100
    daily_model_avg, daily_market_avg, daily_err_pct = _align_series(raw_model, raw_market, raw_err)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    l1, = ax1.plot(daily_model_avg.index, daily_model_avg, "k-", label="BS模型", linewidth=1.5)
    l2, = ax1.plot(daily_market_avg.index, daily_market_avg, "k--", label="市场价格", linewidth=1.5)
    ax1.set_ylabel("转债平均价格 (元)")
    ax1.set_xlabel("年份")

    ax2 = ax1.twinx()
    ax2.fill_between(daily_err_pct.index, daily_err_pct, 0, color="gray", alpha=0.5, label="定价错误")
    ax2.set_ylabel("平均定价错误 (%)")
    ax2.set_ylim(-30, 80)

    patch = mpatches.Patch(color="gray", alpha=0.5, label="定价错误")
    ax1.legend([l1, l2, patch], ["BS模型", "市场价格", "定价错误"], loc="upper center")
    plt.title("图1 BS模型定价结果与市场价格对比")
    _save(os.path.join(SCRIPT_DIR, "Fig1_BS_Price_Time_Series.png"))


# ── 图1：ZL 时序对比 ──────────────────────────────────────────────────────────

def plot_zl_timeseries() -> None:
    print("[ZL] 加载 ZL_Model_Summary.xlsx ...")
    df_model = _load_xlsx_wide(ZL_XLSX, SHEET_MODEL)
    df_market = _load_xlsx_wide(ZL_XLSX, SHEET_MARKET)
    df_reldev = _load_xlsx_wide(ZL_XLSX, SHEET_RELDEV)
    if df_model is None or df_market is None or df_reldev is None:
        print("  [跳过] ZL 时序图缺少必要 sheet")
        return

    raw_model = df_model.mean(axis=1)
    raw_market = df_market.mean(axis=1)
    raw_err = df_reldev.mean(axis=1) * 100
    daily_model_avg, daily_market_avg, daily_err_pct = _align_series(raw_model, raw_market, raw_err)

    fig, ax1 = plt.subplots(figsize=(12, 6))
    l1, = ax1.plot(daily_model_avg.index, daily_model_avg, "k-", label="ZL模型", linewidth=1.5)
    l2, = ax1.plot(daily_market_avg.index, daily_market_avg, "k--", label="市场价格", linewidth=1.5)
    ax1.set_ylabel("转债平均价格 (元)")
    ax1.set_xlabel("年份")

    ax2 = ax1.twinx()
    ax2.fill_between(daily_err_pct.index, daily_err_pct, 0, color="gray", alpha=0.5, label="定价错误")
    ax2.set_ylabel("平均定价错误 (%)")
    ax2.set_ylim(-30, 80)

    patch = mpatches.Patch(color="gray", alpha=0.5, label="定价错误")
    ax1.legend([l1, l2, patch], ["ZL模型", "市场价格", "定价错误"], loc="upper center")
    plt.title("图1 ZL模型定价结果与市场价格对比")
    _save(os.path.join(SCRIPT_DIR, "Fig1_ZL_Price_Time_Series.png"))


# ── 策略绩效图 ────────────────────────────────────────────────────────────────

def _plot_one_strategy(csv_path: str, label_prefix: str, dev_col: str, out_path: str) -> None:
    """从 alpha_strategy_results.csv 绘制策略净值对比图。"""
    if not os.path.exists(csv_path):
        print(f"  [跳过] 找不到策略结果: {csv_path}")
        return

    df = pd.read_csv(csv_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()

    required = {"benchmark_nav", "long_nav"}
    missing = required - set(df.columns)
    if missing:
        print(f"  [跳过] CSV 缺少必要列: {missing}")
        return

    b_nav = df["benchmark_nav"]
    l_nav = df["long_nav"]
    dev_nav = df[dev_col] if dev_col in df.columns else l_nav

    b_cum = b_nav - 1
    l_cum = l_nav - 1
    dev_cum = dev_nav - 1
    excess_pct = ((1 + l_cum) / (1 + b_cum) - 1) * 100

    fig, ax = plt.subplots(figsize=(14, 8))
    ax.plot(b_cum.index, b_cum, color="#000000", linestyle=":", label="基准 (000832)", linewidth=2)
    ax.plot(l_cum.index, l_cum, color="#D32F2F", linestyle="-", label=f"{label_prefix} 多因子组合收益率", linewidth=2.5)
    ax.plot(dev_cum.index, dev_cum, color="#1976D2", linestyle="-", label=f"{label_prefix} 定价偏差因子收益率", linewidth=2)
    ax.set_xlabel("年份", fontsize=15)
    ax.set_ylabel("累计收益率", fontsize=15)
    ax.grid(True, linestyle="--", alpha=0.3)

    ax2 = ax.twinx()
    ax2.plot(excess_pct.index, excess_pct, color="#8E24AA", linestyle="-.", label=f"{label_prefix} 多因子超额收益（右轴）", linewidth=2, alpha=0.8)
    ax2.set_ylabel("累计超额 (%)", fontsize=15)

    lines_1, labels_1 = ax.get_legend_handles_labels()
    lines_2, labels_2 = ax2.get_legend_handles_labels()
    ax.legend(
        lines_1 + lines_2,
        labels_1 + labels_2,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.05),
        ncol=4,
        fontsize=12,
        frameon=True,
        facecolor="white",
        edgecolor="lightgray",
    )

    plt.title(f"{label_prefix} 模型策略效果：多因子组合净值与基准超额", fontsize=17, fontweight="bold", pad=25)
    plt.tight_layout()
    _save(out_path)


def plot_strategy_performance() -> None:
    print("[Strategy] 加载策略结果 CSV ...")
    _plot_one_strategy(
        csv_path=os.path.join(MF_DIR, "B-S_alpha_strategy_results.csv"),
        label_prefix="BS",
        dev_col="bs_deviation_nav",
        out_path=os.path.join(LS_DIR, "BS_model_performance.png"),
    )
    _plot_one_strategy(
        csv_path=os.path.join(MF_DIR, "Z-L_alpha_strategy_results.csv"),
        label_prefix="ZL",
        dev_col="zl_deviation_nav",
        out_path=os.path.join(LS_DIR, "ZL_model_performance.png"),
    )


# ── 因子相关性热力图 ───────────────────────────────────────────────────────────

def _standardize_code(code: str) -> str:
    """sh110067 → 110067.SH; sz128145 → 128145.SZ; already-normalized pass through."""
    if isinstance(code, str):
        if code.startswith("sh"):
            return code[2:] + ".SH"
        if code.startswith("sz"):
            return code[2:] + ".SZ"
        if "." in code:
            return code
    return code


def _load_factor_csvs(factor_dir: str, xlsx_reldev: pd.DataFrame, model_label: str) -> pd.DataFrame | None:
    """
    将 XLSX 相对偏差 + 目录下全部因子 CSV stack 后合并，
    返回用于相关性计算的 DataFrame。
    """
    factor_files = [
        f for f in glob.glob(os.path.join(factor_dir, "*.csv"))
        if "alpha_strategy_results" not in f
    ]
    if not factor_files:
        print(f"  [跳过] {factor_dir} 中无因子 CSV")
        return None

    stacked_factors: dict[str, pd.Series] = {}

    dev_stacked = xlsx_reldev.stack()
    dev_stacked.name = f"{model_label}_deviation"
    stacked_factors[f"{model_label}_deviation"] = dev_stacked

    for fpath in sorted(factor_files):
        try:
            df = pd.read_csv(fpath)
            if "date" not in df.columns:
                continue
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            df.columns = [_standardize_code(c) for c in df.columns]
            df = df.apply(pd.to_numeric, errors="coerce")
            stacked = df.stack()
            factor_name = os.path.splitext(os.path.basename(fpath))[0].replace("等权和", "")
            stacked_factors[factor_name] = stacked
        except Exception as e:
            print(f"  [警告] 加载因子 {fpath} 失败: {e}")

    merged = pd.DataFrame(stacked_factors).dropna()
    if merged.empty:
        print("  [跳过] 因子数据对齐后为空")
        return None
    return merged


def _plot_factor_corr(merged: pd.DataFrame, title: str, out_path: str) -> None:
    corr = merged.corr()
    plt.figure(figsize=(10, 8))
    sns.heatmap(corr, annot=True, cmap="RdBu_r", center=0, vmin=-1, vmax=1, fmt=".2f")
    plt.title(title)
    plt.tight_layout()
    _save(out_path)


def plot_factor_correlation() -> None:
    print("[Factor] 加载因子数据 ...")

    df_bs_reldev = _load_xlsx_wide(BS_XLSX, SHEET_RELDEV)
    if df_bs_reldev is not None:
        merged_bs = _load_factor_csvs(MF_DIR, df_bs_reldev, "BS")
        if merged_bs is not None:
            _plot_factor_corr(
                merged_bs,
                title="BS 因子相关性热力图",
                out_path=os.path.join(MF_DIR, "BS_factor_correlation.png"),
            )

    df_zl_reldev = _load_xlsx_wide(ZL_XLSX, SHEET_RELDEV)
    if df_zl_reldev is not None:
        merged_zl = _load_factor_csvs(MF_DIR, df_zl_reldev, "ZL")
        if merged_zl is not None:
            _plot_factor_corr(
                merged_zl,
                title="ZL 因子相关性热力图",
                out_path=os.path.join(MF_DIR, "ZL_factor_correlation.png"),
            )


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 60)
    print("regenerate_plots.py — 重生成 README 全部图表")
    print("=" * 60)

    plot_bs_timeseries()
    plot_zl_timeseries()
    plot_strategy_performance()
    plot_factor_correlation()

    print("=" * 60)
    print("完成。")


if __name__ == "__main__":
    main()
