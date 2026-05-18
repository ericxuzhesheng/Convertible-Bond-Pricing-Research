"""
daily_signal.py — 每日收盘后自动运行，输出最值得买的 5 只可转债。

流程:
  1. 增量更新数据 (data_pipeline.py)
  2. 增量运行 B-S / Z-L 模型
  3. 按过滤条件 + 综合评分选出 Top 5
  4. 推送通知（PushPlus 微信 / SMTP 邮件）

配置:
  signal_config.json 示例（PushPlus）:
      {"provider": "pushplus", "token": "YOUR_TOKEN"}
  signal_config.json 示例（SMTP 邮件）:
      {"provider": "smtp", "smtp_server": "...", "smtp_port": 587,
       "use_ssl": false, "sender_email": "...", "sender_password": "...",
       "recipient_emails": ["..."]}
  或运行 python setup_notification.py 一键配置。

过滤条件:
  - 剩余期限 > 0.5 年
  - 转股溢价率 < 30%   [(市价 - 转换价值) / 转换价值]
  - 正股总市值 > 50 亿  (即 > 500000 万元)
  - 信用评级 >= AA-    (AA-, AA, AA+, AAA)

评分逻辑:
  combined_score = (bs_underpricing + zl_underpricing) / 2
  其中 underpricing = (model_price - market_price) / market_price
  分数越高 = 模型认为该债越被低估
"""

import io
import json
import os
import smtplib
import subprocess
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

# Windows GBK console 兼容: emoji 回退为 ?
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import numpy as np
import pandas as pd

# ── 路径配置 ──────────────────────────────────────────────────
DIR = os.path.dirname(os.path.abspath(__file__))


def _load_config() -> dict:
    cfg_path = os.path.join(DIR, "signal_config.json")
    if os.path.exists(cfg_path):
        with open(cfg_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


# ── 过滤参数 ──────────────────────────────────────────────────
MIN_MATURITY   = 0.5          # 年
MAX_PREMIUM    = 0.30         # 转股溢价率上限
MIN_MV_WAN     = 500_000      # 万元 (= 50亿)
VALID_RATINGS  = {"AAA", "AA+", "AA", "AA-"}


# ── 步骤 1: 增量更新数据 ──────────────────────────────────────
def run_pipeline() -> None:
    today = datetime.today().strftime("%Y%m%d")
    print(f"[1/3] 增量更新数据到 {today} …")
    result = subprocess.run(
        [sys.executable, os.path.join(DIR, "data_pipeline.py"),
         "--start", today, "--end", today],
        cwd=DIR, capture_output=True, text=True
    )
    if result.returncode != 0:
        print("  data_pipeline 警告:", result.stderr[-500:] if result.stderr else "")
    else:
        print("  数据更新完成。")


# ── 步骤 2: 增量跑模型 ────────────────────────────────────────
def run_models() -> None:
    for name, script in [("B-S", "B-S_backtest.py"), ("Z-L", "Z-L_backtest_CPU.py")]:
        print(f"[2/3] 运行 {name} 模型 …")
        result = subprocess.run(
            [sys.executable, os.path.join(DIR, script)],
            cwd=DIR, capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  {name} 模型错误:", result.stderr[-300:] if result.stderr else "")
        else:
            print(f"  {name} 完成。")


# ── 步骤 3: 计算信号 ──────────────────────────────────────────
def load_wide(filename: str) -> pd.DataFrame:
    path = os.path.join(DIR, filename)
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df.index = pd.to_datetime(df.index, errors="coerce")
    return df[df.index.notna()].apply(pd.to_numeric, errors="coerce")


def compute_signals() -> pd.DataFrame:
    print("[3/3] 计算信号并过滤 …")

    # ── 加载数据 ──
    bs_dev  = load_wide("BS_Model_Deviation_Pct.csv")   # (model-mkt)/mkt
    zl_dev  = load_wide("ZL_Model_Deviation_Pct.csv")
    mkt     = load_wide("cb_price_cache.csv")
    cv      = load_wide("cb_convert_val_cache.csv")
    mat     = load_wide("cb_maturity_cache.csv")
    stock_mv = load_wide("cb_stock_mv_cache.csv")
    rating  = pd.read_csv(os.path.join(DIR, "cb_rating_cache.csv"),
                          index_col=0, parse_dates=True)
    rating.index = pd.to_datetime(rating.index, errors="coerce")
    basic   = pd.read_csv(os.path.join(DIR, "cb_basic_info.csv"))

    # ── 取最新有效交易日 ──
    latest = bs_dev.index[bs_dev.notna().any(axis=1)][-1]
    print(f"  最新有效日期: {latest.date()}")

    # ── 当日截面 ──
    def latest_row(df: pd.DataFrame) -> pd.Series:
        idx = df.index[df.notna().any(axis=1)]
        if len(idx) == 0:
            return pd.Series(dtype=float)
        return df.loc[idx[-1]]

    s_bs   = bs_dev.loc[latest].dropna()      # BS (model-mkt)/mkt
    s_zl   = latest_row(zl_dev)               # ZL 可能滞后一天
    s_mkt  = mkt.loc[latest]
    s_cv   = cv.loc[latest]
    s_mat  = mat.loc[latest]
    s_mv   = latest_row(stock_mv)

    # 评级: ffill 到当日
    rating_ffill = rating.reindex(
        rating.index.union([latest])
    ).sort_index().ffill()
    s_rating = rating_ffill.loc[latest] if latest in rating_ffill.index else pd.Series(dtype=str)

    # ── 构建候选 DataFrame ──
    bonds = s_mkt.dropna().index
    rows = []
    for code in bonds:
        mkt_p  = s_mkt.get(code)
        cv_p   = s_cv.get(code)
        mat_yr = s_mat.get(code)
        mv     = s_mv.get(code)
        rat    = s_rating.get(code)
        bs_d   = s_bs.get(code)
        zl_d   = s_zl.get(code)

        if any(pd.isna(x) for x in [mkt_p, cv_p, mat_yr]):
            continue

        premium = (mkt_p - cv_p) / cv_p if cv_p > 0 else np.nan

        rows.append({
            "ts_code":  code,
            "market_price": round(float(mkt_p), 2),
            "convert_val":  round(float(cv_p),  2),
            "premium":      round(float(premium), 4) if pd.notna(premium) else np.nan,
            "maturity_yr":  round(float(mat_yr),  2),
            "stock_mv_wan": float(mv) if pd.notna(mv) else np.nan,
            "rating":       str(rat) if pd.notna(rat) else "",
            "bs_dev":       float(bs_d) if pd.notna(bs_d) else np.nan,
            "zl_dev":       float(zl_d) if pd.notna(zl_d) else np.nan,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # 综合得分: 两个模型低估程度均值
    # dev > 0 表示模型价 > 市场价 = 低估
    df["bs_score"]  = df["bs_dev"].fillna(0)
    df["zl_score"]  = df["zl_dev"].fillna(0)
    df["score"]     = (df["bs_score"] + df["zl_score"]) / 2

    # ── 过滤 ──
    mask = (
        (df["maturity_yr"] > MIN_MATURITY) &
        (df["premium"].fillna(9) < MAX_PREMIUM) &
        (df["stock_mv_wan"].fillna(0) > MIN_MV_WAN) &
        (df["rating"].isin(VALID_RATINGS))
    )
    df_filtered = df[mask].copy()
    print(f"  过滤后剩余 {len(df_filtered)} 只，原始 {len(df)} 只。")

    # ── 排名 ──
    top5 = df_filtered.sort_values("score", ascending=False).head(5)

    # 拼接债券名称
    name_map = basic.set_index("ts_code")["name"].to_dict() if "name" in basic.columns else {}
    top5["name"] = top5["ts_code"].map(name_map).fillna("")

    top5.insert(0, "date", latest.strftime("%Y-%m-%d"))
    return top5.reset_index(drop=True)


# ── 步骤 4: 发 Webhook ────────────────────────────────────────
RATING_EMOJI = {"AAA": "🏆", "AA+": "⭐", "AA": "✅", "AA-": "🔹"}

def format_message(top5: pd.DataFrame, date_str: str) -> str:
    lines = [
        f"## 📊 可转债每日精选 · {date_str}",
        f"> 过滤条件: 剩余期限>{MIN_MATURITY}年 | 溢价率<{int(MAX_PREMIUM*100)}% | 市值>50亿 | 评级≥AA-",
        "",
    ]
    for i, row in top5.iterrows():
        rat_icon = RATING_EMOJI.get(row["rating"], "")
        bs_pct   = f"{row['bs_dev']*100:+.1f}%" if pd.notna(row["bs_dev"]) else "N/A"
        zl_pct   = f"{row['zl_dev']*100:+.1f}%" if pd.notna(row["zl_dev"]) else "N/A"
        prem_pct = f"{row['premium']*100:.1f}%"
        mv_yi    = f"{row['stock_mv_wan']/10000:.0f}亿" if pd.notna(row["stock_mv_wan"]) else "N/A"
        name_str = f" {row['name']}" if row.get("name") else ""
        lines += [
            f"**{i+1}. {row['ts_code']}{name_str}** {rat_icon}{row['rating']}",
            f"   市价 **{row['market_price']}** | 溢价率 {prem_pct} | 剩余 {row['maturity_yr']:.1f}年 | 市值 {mv_yi}",
            f"   B-S低估 {bs_pct} · Z-L低估 {zl_pct} · 综合评分 **{row['score']*100:+.1f}%**",
            "",
        ]
    return "\n".join(lines)


def _send_pushplus(title: str, content: str, token: str) -> bool:
    try:
        resp = requests.post(
            "https://www.pushplus.plus/send",
            json={"token": token, "title": title, "content": content, "template": "txt"},
            timeout=15,
        )
        data = resp.json()
        if data.get("code") == 200:
            print("  PushPlus 推送成功。")
            return True
        print(f"  PushPlus 推送失败: {data.get('msg')}")
        return False
    except Exception as e:
        print(f"  PushPlus 推送异常: {e}")
        return False


def _send_email(subject: str, body: str, cfg: dict) -> bool:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = ", ".join(cfg["recipient_emails"])
    msg.attach(MIMEText(body, "plain", "utf-8"))
    try:
        if cfg.get("use_ssl"):
            ctx = smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"])
        else:
            ctx = smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"])
            ctx.starttls()
        with ctx as s:
            s.login(cfg["sender_email"], cfg["sender_password"])
            s.sendmail(cfg["sender_email"], cfg["recipient_emails"], msg.as_string())
        print("  邮件推送成功。")
        return True
    except Exception as e:
        print(f"  邮件推送失败: {e}")
        return False


def send_notification(title: str, body: str, cfg: dict) -> bool:
    provider = cfg.get("provider", "")
    if provider == "pushplus":
        return _send_pushplus(title, body, cfg["token"])
    if provider == "smtp":
        return _send_email(title, body, cfg)
    print("  [WARN] signal_config.json 未配置推送，跳过。")
    return False


# ── 主入口 ────────────────────────────────────────────────────
def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="每日可转债择券信号")
    parser.add_argument("--skip-pipeline", action="store_true",
                        help="跳过数据更新（数据已是最新时使用）")
    parser.add_argument("--skip-models", action="store_true",
                        help="跳过模型运行（模型结果已是最新时使用）")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印结果，不发送 webhook")
    args = parser.parse_args()

    start = datetime.now()
    print(f"=== 每日转债信号 {start.strftime('%Y-%m-%d %H:%M')} ===\n")

    if not args.skip_pipeline:
        run_pipeline()
    if not args.skip_models:
        run_models()

    top5 = compute_signals()

    if top5.empty:
        print("[WARN] 未找到符合条件的转债，请检查数据或放宽过滤条件。")
        return

    date_str = top5["date"].iloc[0]

    # 保存 CSV
    out_csv = os.path.join(DIR, f"top5_{date_str.replace('-', '')}.csv")
    top5.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"\n已保存: {out_csv}")

    # 打印
    print("\n" + "─" * 60)
    cols = ["ts_code", "name", "rating", "market_price", "premium",
            "maturity_yr", "bs_dev", "zl_dev", "score"]
    print(top5[[c for c in cols if c in top5.columns]].to_string(index=False))
    print("─" * 60)

    # 推送通知
    message = format_message(top5, date_str)
    cfg = _load_config()
    if args.dry_run:
        print("\n── 消息预览 (dry-run) ──")
        print(message)
    elif not cfg.get("provider"):
        print("\n[WARN] signal_config.json 未配置推送，跳过。")
        print("   运行: python setup_notification.py")
        print("\n── 消息预览 ──")
        print(message)
    else:
        send_notification(f"可转债每日精选 · {date_str}", message, cfg)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n总耗时: {elapsed:.0f}s")


if __name__ == "__main__":
    main()
