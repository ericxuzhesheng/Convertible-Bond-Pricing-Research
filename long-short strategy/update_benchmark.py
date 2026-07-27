"""
更新中证转债指数（000832.CSI）基准收盘价缓存 000832_CSI_close_price.xlsx。

数据源：Tushare Pro `index_daily(ts_code='000832.CSI')`。
幂等：仅追加比文件内现有最大日期更新的交易日，保留原 Wind 导出格式
（前 5 行元数据 + 第 6 行 ['Date','close'] 表头 + 时序），使
B-S_Z-L_strategy.py 的 `skiprows=5` 读取方式继续有效。

用法：
    python "long-short strategy/update_benchmark.py" [--end YYYYMMDD]
"""
import os
import sys
import argparse
import datetime as dt

import pandas as pd
import tushare as ts

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
BACKTEST_DIR = os.path.join(REPO_ROOT, "backtest")
BENCH_FILE = os.path.join(SCRIPT_DIR, "000832_CSI_close_price.csv")
TS_CODE = "000832.CSI"


def _load_token():
    """复用 backtest/token_loader.py 的 token 解析（环境变量或本地文件）。"""
    sys.path.insert(0, BACKTEST_DIR)
    from token_loader import load_tushare_token
    return load_tushare_token()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--end", default=dt.date.today().strftime("%Y%m%d"),
                        help="抓取截止日期 YYYYMMDD，默认今天")
    args = parser.parse_args()

    if os.path.exists(BENCH_FILE):
        series = pd.read_csv(BENCH_FILE)
    else:
        series = pd.DataFrame(columns=["Date", "close"])

    series["Date"] = pd.to_datetime(series["Date"], errors="coerce")
    series = series.dropna(subset=["Date"])
    last_date = series["Date"].max() if not series.empty else pd.to_datetime("2017-01-01")
    print(f"现有基准最新日期: {last_date.date()}  (共 {len(series)} 行)")

    start = (last_date + dt.timedelta(days=1)).strftime("%Y%m%d")
    if start > args.end:
        print("已是最新，无需更新。")
        return

    ts.set_token(_load_token())
    pro = ts.pro_api()
    df = pro.index_daily(ts_code=TS_CODE, start_date=start, end_date=args.end)
    if df is None or df.empty:
        print(f"Tushare 返回空（{start}~{args.end}），无新增。")
        return

    new = df[["trade_date", "close"]].copy()
    new["Date"] = pd.to_datetime(new["trade_date"], format="%Y%m%d")
    new = new[["Date", "close"]].sort_values("Date")
    new = new[new["Date"] > last_date]
    if new.empty:
        print("无晚于现有最大日期的新交易日。")
        return
    print(f"新增 {len(new)} 个交易日: {new['Date'].min().date()} ~ {new['Date'].max().date()}"
          f"  最新 close={new['close'].iloc[-1]:.4f}")

    merged_series = pd.concat([series, new], ignore_index=True)
    merged_series = merged_series.drop_duplicates(subset="Date").sort_values("Date")

    merged_series.to_csv(BENCH_FILE, index=False)
    print(f"已写回 {BENCH_FILE}  (总 {len(merged_series)} 行时序)")


if __name__ == "__main__":
    main()
