"""Run the ensemble strategy; independent outputs preserve legacy research."""
from __future__ import annotations
import argparse
from dataclasses import asdict, replace
import json
from pathlib import Path
import sys
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
BASE = HERE.parent / "backtest"
sys.path.insert(0, str(BASE))
from strategy_data import write_csv, write_json, write_text, now_local
from market_data_contracts import observed_average_risk_free_rate
from weekly_strategy_core import Config, ValueConfig, net_valuation_upside, Inputs, build_ranking, next_session, select_targets, simulate, performance, timestamp

METHOD = "weekly-ensemble-v1"
ENGINE_REVISION = 4
VALUE_METHOD = "weekly-value-v1"
VALUE_ENGINE_REVISION = 1
LIMITATION = ("理论多空，无历史券源验证；历史强赎/ST覆盖不完整；初次抓取的财务历史不是原始发布版本。"
              "日净值为价格与已核验现金流的研究估计，尚未验证全部历史付息，不能称为完整总收益。")

def clean_json(value):
    return json.loads(pd.Series([value]).to_json(orient="values", date_format="iso"))[0]

def certify_append(path, frame):
    if not path.exists() or path.stat().st_size < 3:
        return
    try:
        old = pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return
    if old.empty:
        return
    if "date" not in frame or "date" not in old:
        raise ValueError(f"Cannot verify tracking history: {path}")
    old["date"] = pd.to_datetime(old.date)
    compare = frame.copy()
    compare["date"] = pd.to_datetime(compare.date)
    for column in set(old.columns).intersection(compare.columns):
        if column.endswith("_date"):
            old[column] = pd.to_datetime(old[column])
            compare[column] = pd.to_datetime(compare[column])
    prefix = compare.loc[compare.date <= old.date.max()].reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(old.reset_index(drop=True), prefix, check_dtype=False,
                                      check_exact=False, rtol=1e-10, atol=1e-12)
    except AssertionError:
        raise ValueError(f"Verified tracking history changed: {path}; not overwritten") from None

def window_metrics(nav, rf):
    result = []
    if nav.empty:
        return pd.DataFrame()
    for year, frame in nav.groupby(pd.DatetimeIndex(nav.date).year):
        frame = frame.copy()
        frame["nav"] = (1+frame["return"]).cumprod()
        result.append({"window": str(year), **performance(frame, rf)})
    for end in range(252, len(nav)+1, 21):
        frame = nav.iloc[end-252:end].copy()
        frame["nav"] = (1+frame["return"]).cumprod()
        result.append({"window": f"rolling_252_to_{pd.Timestamp(frame.date.iloc[-1]).date()}",
                       **performance(frame, rf)})
    return pd.DataFrame(result)

def run_one(data, config, start, end, output, mode="both", detail=True):
    output.mkdir(parents=True, exist_ok=True)
    state_path = output / "manifest.json"
    previous = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    is_value = isinstance(config, ValueConfig)
    method = VALUE_METHOD if is_value else METHOD
    revision = VALUE_ENGINE_REVISION if is_value else ENGINE_REVISION
    specification = {"method": method, "config": asdict(config), "start": str(start.date()), "mode": mode}
    if previous and previous.get("specification") != specification:
        raise ValueError(f"Output directory belongs to another configuration: {output}")
    if (previous.get("engine_revision") == revision
            and previous.get("input_cutoff") == data.metadata["cutoff"]
            and previous.get("requested_end") == str(end.date())
            and previous.get("input_vintage") == getattr(data,"input_vintage",None)):
        path = output / "metrics.csv"
        if path.exists() and path.stat().st_size > 1:
            print(f"No new daily observations: {output}", flush=True)
            try:
                return pd.read_csv(path, float_precision="round_trip")
            except pd.errors.EmptyDataError:
                # Pending-only tracking has no performance rows; CRLF is two bytes.
                return pd.DataFrame()
        if path.exists():
            return pd.DataFrame()
    dates = data.signal_dates(start, end, config.frequency)
    rankings, selections, targets = [], [], {"long_only": {}, "long_short": {}}
    entry_limits = {}
    for date in dates:
        ranking = build_ranking(data, date, config)
        execute = next_session(date, data.calendar)
        if config.risk_mode == "tracking":
            freeze = output / "signals" / f"{date.date()}.json"
            if freeze.exists():
                frozen = json.loads(freeze.read_text(encoding="utf-8"))
                ranking = pd.DataFrame(frozen["ranking"]).set_index("code")
            else:
                # A risk snapshot retrieved after execution cannot create a historical trade.
                if not ranking.event_known.any() or pd.isna(execute):
                    if date == dates[-1] and pd.notna(execute) and timestamp(now_local()) < execute + pd.Timedelta(hours=9, minutes=30):
                        raise ValueError("Latest tracking risk snapshot unavailable; no new trading targets published")
                    continue
                current = timestamp(now_local())
                if current >= execute + pd.Timedelta(hours=9, minutes=30):
                    continue
                # Genuine missing reports exclude individuals; request failure is rejected by Inputs.
                write_json(freeze, {"frozen_at": now_local(), "method": method, "config": asdict(config),
                    "ranking": json.loads(ranking.reset_index(names="code").to_json(
                        orient="records", date_format="iso"))})
        selection_options = {}
        if is_value:
            selection_options = {"transaction_bps": config.transaction_bps,
                "safety_margin": config.long_safety_margin, "balance_legs": config.balance_legs}
            ranking = ranking.copy()
            ranking["entry_value"] = ranking.average_price if config.model == "mean" else ranking[config.model]
            ranking["roundtrip_net_upside"] = net_valuation_upside(ranking.signal, config.transaction_bps)
            fee = config.transaction_bps / 10000
            ranking["long_entry_limit"] = ranking.entry_value * (1-fee) / ((1+fee)*(1+config.long_safety_margin))
            ranking["long_entry_qualified"] = ranking.eligible & np.isfinite(ranking.roundtrip_net_upside) & ranking.roundtrip_net_upside.ge(config.long_safety_margin-1e-12)
            entry_limits[date] = ranking.long_entry_limit.to_dict()
        long, long_short = select_targets(ranking, config.select_ratio, config.single_cap, **selection_options)
        if is_value:
            ranking["long_selected"] = ranking.index.isin(long)
            ranking["long_excluded_by"] = np.where(~ranking.eligible, ranking.excluded_by,
                np.where(~ranking.long_entry_qualified, "net_valuation_gap_below_safety_margin",
                    np.where(~ranking.long_selected, "outside_top_quintile_slots", "")))
        targets["long_only"][date], targets["long_short"][date] = long, long_short
        rankings.append(ranking.reset_index(names="code"))
        for portfolio, weights in (("long_only", long), ("long_short", long_short)):
            for code, weight in sorted(weights.items()):
                selections.append({"signal_date": date, "execution_date": execute, "portfolio": portfolio,
                    "code": code, "weight": weight, "signal": ranking.at[code, "signal"],
                    "mean_upside": ranking.at[code, "mean_upside"],
                    "valuation_gap_yuan": ranking.at[code,"average_price"]-ranking.at[code,"market_price"],
                    **({"roundtrip_net_upside": ranking.at[code, "roundtrip_net_upside"],
                        "long_safety_margin": config.long_safety_margin,
                        "long_entry_limit": ranking.at[code, "long_entry_limit"] if weight > 0 else np.nan} if is_value else {}),
                    "status": "pending_open" if pd.isna(execute) or execute > data.bars["close"].index.max() else "simulation_target"})
        if len(rankings) % 100 == 0:
            print(f"{config.risk_mode}/{config.model}/{config.frequency}: {len(rankings)} signals", flush=True)
    files = {}
    files["rankings.csv"] = pd.concat(rankings, ignore_index=True) if rankings else pd.DataFrame()
    files["targets.csv"] = pd.DataFrame(selections)
    modes = ["long_only", "long_short"] if mode == "both" else [mode]
    metrics, windows = [], []
    curves = {}
    bar_slice = {name: frame.loc[start:end] for name, frame in data.bars.items()}
    for portfolio in modes:
        nav, trades, holdings, issues = simulate(bar_slice, data.calendar, targets[portfolio],
            config=config, mode=portfolio, entry_limits=entry_limits if is_value else None, cashflows=data.cashflows, suspensions=getattr(data,"suspensions",None), alert_fn=lambda day, held: data.daily_alerts(day, held, config.fundamentals))
        if not nav.empty:
            first = pd.Timestamp(nav.date.iloc[0])
            benchmark = data.benchmark.reindex(pd.DatetimeIndex(nav.date))
            opening = benchmark.open.iloc[0]
            if not np.isfinite(opening) or opening <= 0 or benchmark.close.isna().any():
                raise ValueError("Benchmark open/daily close missing")
            nav["benchmark_nav"] = benchmark.close.to_numpy()/opening
            nav["excess_nav"] = nav.nav/nav.benchmark_nav
            rf = observed_average_risk_free_rate(curve=data.rf, start=first,
                end=pd.Timestamp(nav.date.iloc[-1]), tenor_years=1.)
            metric = performance(nav, rf)
            span_days = max(1, (pd.Timestamp(nav.date.iloc[-1])-first).days+1)
            metric["benchmark_annual_return"] = nav.benchmark_nav.iloc[-1]**(365.25/span_days)-1
            metric["annual_excess_return"] = metric["annual_return"]-metric["benchmark_annual_return"] if metric["annual_return"] is not None else None
            metric.update({"period_start": str(first.date()), "period_end": str(pd.Timestamp(nav.date.iloc[-1]).date()), "portfolio": portfolio, "risk_mode": config.risk_mode,
                           "model": config.model, "frequency": config.frequency,
                           "transaction_bps": config.transaction_bps, "borrow_rate": config.borrow_rate,
                           "total_return_certified": False,
                           "bank_weight_mean": 0., "max_single_weight": 0.})
            if is_value:
                metric.update({"long_safety_margin": config.long_safety_margin,
                    "average_long_exposure": nav.long_exposure.mean(),
                    "average_short_exposure": nav.short_exposure.mean(),
                    "average_net_exposure": nav.net_exposure.mean(),
                    "cumulative_transaction_cost": nav.cost.sum(),
                    "cumulative_borrow_cost": nav.borrow_cost.sum()})
            if not holdings.empty:
                banks = data.basic.bond_full_name.astype(str).str.contains("银行", regex=False)
                bank_positions = holdings.loc[holdings.code.map(banks).fillna(False)]
                metric["bank_weight_mean"] = bank_positions.groupby("date").weight.apply(
                    lambda x: x.abs().sum()).reindex(pd.DatetimeIndex(nav.date), fill_value=0).mean()
                metric["max_single_weight"] = holdings.weight.abs().max()
            metrics.append(metric)
            win = window_metrics(nav, rf)
            win["portfolio"] = portfolio
            windows.append(win)
            curves[portfolio] = nav
        for suffix, frame in (("daily_nav", nav), ("trades", trades), ("holdings", holdings), ("issues", issues)):
            files[f"{portfolio}_{suffix}.csv"] = frame
    files["metrics.csv"] = pd.DataFrame(metrics)
    files["windows.csv"] = pd.concat(windows, ignore_index=True) if windows else pd.DataFrame()
    coverage_rows = []
    for ranking in rankings:
        pre = ranking.loc[ranking.market_eligible]
        coverage_rows.append({"signal_date": ranking.signal_date.iloc[0], "market_count": len(ranking),
            "market_eligible_count": len(pre), "eligible_count": int(ranking.eligible.sum()),
            **({"long_entry_qualified_count": int(ranking.long_entry_qualified.sum()),
                "long_selected_count": int(ranking.long_selected.sum())} if is_value else {}),
            "three_model_count": int(ranking[["BS", "ZL", "LSM"]].notna().all(axis=1).sum()),
            "financial_coverage": pre.financial_known.mean() if len(pre) else None,
            "event_coverage": pre.event_known.mean() if len(pre) else None,
            "st_coverage": pre.st_known.mean() if len(pre) else None})
    files["coverage.csv"] = pd.DataFrame(coverage_rows)
    if not detail:
        files = {name: frame for name, frame in files.items() if name != "rankings.csv" and not name.endswith("_holdings.csv")}
    if config.risk_mode == "tracking":
        # Validate ALL ledger prefixes before replacing any confirmed ledger.
        for name, frame in files.items():
            if name.endswith(("_daily_nav.csv", "_trades.csv", "_holdings.csv", "_issues.csv")):
                certify_append(output/name, frame)
    curves_changed = any(not (output/name).exists() or (output/name).read_text(encoding="utf-8") != frame.to_csv(index=False, lineterminator="\n")
                         for name, frame in files.items() if name.endswith("_daily_nav.csv"))
    for name, frame in files.items():
        write_csv(output/name, frame)
    report = ["# 周度均价安全边际策略" if is_value else "# 周度三模型相对排名策略", "", f"模型：{config.model}；频率：{config.frequency}；模式：{config.risk_mode}。",
              "", "采用每日OHLCV和日持仓账本，下一交易日开盘调仓。", "", LIMITATION, ""]
    if rankings:
        latest = rankings[-1]
        eligible = latest.loc[latest.eligible].sort_values("signal", ascending=False)
        latest_date = pd.Timestamp(latest.signal_date.iloc[0])
        latest_long, latest_ls = targets["long_only"][latest_date], targets["long_short"][latest_date]
        report += [f"最新信号：{latest.signal_date.iloc[0]}；风险筛选合格券 {len(eligible)} 只；多头 {len(latest_long)} 只，理论空头 {sum(w<0 for w in latest_ls.values())} 只。",
            f"多头目标仓位 {sum(latest_long.values()):.1%}；目标现金约 {1-sum(latest_long.values()):.1%}（另扣实际成交成本）。理论多空目标净敞口 {sum(latest_ls.values()):.1%}。"]
        if is_value:
            report += [f"买入要求：扣除往返综合成本后的条件性估值余量≥{config.long_safety_margin:.1%}；开盘价再次复核。该余量不是预期周收益。",
                "", "|代码|转债|多头目标权重|扣费后估值余量|买入价格上限约|三模型均高于市价|", "|---|---|---:|---:|---:|---|"]
            for code, weight in latest_long.items():
                row = latest.set_index("code").loc[code]
                consensus = "是" if row[["BS", "ZL", "LSM"]].gt(row.market_price).all() else "否"
                report.append(f"|{code}|{row['name']}|{weight:.1%}|{row.roundtrip_net_upside:.2%}|{np.floor(row.long_entry_limit*1000)/1000:.3f}|{consensus}|")
        report += ["", "以下为风险合格池相对排名，未必满足买入门槛。", "", "|代码|转债|均价与市价差(元)|均价估值差|排序信号|", "|---|---|---:|---:|---:|"]
        for row in eligible.itertuples():
            report.append(f"|{row.code}|{row.name}|{row.average_price-row.market_price:.2f}|{row.mean_upside:.2%}|{row.signal:.2%}|")
        if selections:
            latest_targets = files["targets.csv"]
            pending = latest_targets.loc[latest_targets.status.eq("pending_open")]
            report += ["", f"待开盘执行目标：{len(pending)} 条（含两个组合），不计作已成交。"]
    else:
        report += ["尚无满足交易前快照要求的跟踪信号。"]
    if metrics:
        report += ["", "|组合|年化研究收益|夏普|最大回撤|未验证价格天数|", "|---|---:|---:|---:|---:|"]
        for m in metrics:
            sharpe = m.get("sharpe")
            report.append(f"|{m['portfolio']}|{format(m['annual_return'], '.2%') if m['annual_return'] is not None else '不可验收'}|{sharpe if sharpe is not None else 'NA'}|{format(m['max_drawdown'], '.2%') if m['max_drawdown'] is not None else '不可验收'}|{m['unverified_mark_days']}|")
    report += ["", f"成本假设：单边综合摩擦{config.transaction_bps:g}bp，借券年化{config.borrow_rate:.1%}，闲置现金收益0；只按实际成交扣费。",
               "详见 targets.csv、holdings.csv、trades.csv、daily_nav.csv、coverage.csv 和 acquisition.json。"]
    write_text(output/"weekly_report.md", "\n".join(report)+"\n")
    state = {"engine_revision":revision,"input_vintage":getattr(data,"input_vintage",None), "specification": specification, "input_cutoff": data.metadata["cutoff"], "requested_end": str(end.date()),
             "last_signal": str(rankings[-1].signal_date.iloc[0]) if rankings else None,
             "total_return_certified": False, "limitations": data.metadata["research_limitations"]}
    if (state != previous or curves_changed) and curves:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(figsize=(11, 5))
        for name, nav in curves.items():
            ax.plot(nav.date, nav.nav.where(nav.marked_prices_verified), label=name+" (explained marks)")
        nav = next(iter(curves.values()))
        ax.plot(nav.date, nav.benchmark_nav, label="000832.CSI", color="black", alpha=.6)
        ax.set(title="Daily NAV (research estimate; verified suspensions use stale marks; incomplete cashflows)", ylabel="NAV")
        ax.legend()
        ax.grid(alpha=.2)
        fig.tight_layout()
        fig.savefig(output/"daily_nav.png", dpi=150)
        plt.close(fig)
    write_json(state_path, state)
    return pd.DataFrame(metrics)

def main(argv=None, *, value_strategy=False):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=["mean", "BS", "ZL", "LSM"], default="mean")
    parser.add_argument("--frequency", choices=["weekly", "monthly"], default="weekly")
    parser.add_argument("--mode", choices=["both", "long_only", "long_short"], default="both")
    parser.add_argument("--line", choices=["both", "research", "tracking"], default="both")
    parser.add_argument("--start", default="2019-01-01")
    parser.add_argument("--end")
    parser.add_argument("--transaction-bps", type=float, default=5. if value_strategy else 10.)
    if value_strategy:
        parser.add_argument("--long-safety-margin", type=float, default=.02, help="Net valuation margin as a decimal (0.02 = 2%%)")
    parser.add_argument("--borrow-rate", type=float, default=.03)
    parser.add_argument("--without-fundamentals", action="store_true")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--input-dir", type=Path, default=BASE/"strategy_inputs")
    parser.add_argument("--output-dir", type=Path, default=HERE/("weekly_value_results" if value_strategy else "weekly_results"))
    args = parser.parse_args(argv)
    data = Inputs(BASE, args.input_dir)
    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end or data.metadata["cutoff"])
    if end > pd.Timestamp(data.metadata["cutoff"]):
        raise ValueError("Requested end exceeds verified daily data")
    config_type = ValueConfig if value_strategy else Config
    config = config_type(model=args.model, frequency=args.frequency, fundamentals=not args.without_fundamentals,
        transaction_bps=args.transaction_bps, borrow_rate=args.borrow_rate,
        **({"long_safety_margin": args.long_safety_margin} if value_strategy else {}))
    lines = ["research", "tracking"] if args.line == "both" else [args.line]
    summaries = []
    for line in lines:
        c = replace(config, risk_mode=line)
        summaries.append(run_one(data, c, start, end, args.output_dir/line, args.mode))
    if args.compare:
        monthly_dates = data.signal_dates(start, end, "monthly")
        if len(monthly_dates) == 0:
            raise ValueError("No completed month available for matched-period comparisons")
        comparison_start = monthly_dates[0]
        summaries = []
        variants = {"mean_weekly": replace(config, model="mean", frequency="weekly"),
                    "mean_monthly": replace(config, model="mean", frequency="monthly"),
                    **{f"{m}_weekly": replace(config, model=m) for m in ("BS", "ZL", "LSM")},
                    "no_fundamentals": replace(config, fundamentals=False),
                    **{f"cost_{bps}bp": replace(config, transaction_bps=bps) for bps in ((10, 20) if value_strategy else (5, 20))},
                    **{f"borrow_{rate}": replace(config, borrow_rate=rate) for rate in (0., .08)}}
        for name, c in variants.items():
            result = run_one(data, c, comparison_start, end, args.output_dir/"comparisons"/name, args.mode, detail=False).copy()
            result["variant"] = name
            summaries.append(result)
        write_csv(args.output_dir/"comparison_metrics.csv", pd.concat(summaries, ignore_index=True))
    print(f"Strategy outputs: {args.output_dir}", flush=True)

if __name__ == "__main__":
    main()

