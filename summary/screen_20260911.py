"""Reproduce the 2026-09-11 three-model, high-rating valuation screen."""
from pathlib import Path
import json
import re
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'backtest'
DATE = pd.Timestamp('2026-09-11')


def read_matrix(name):
    frame = pd.read_csv(DATA / name, index_col=0, parse_dates=True, low_memory=False)
    assert frame.index.is_unique and frame.columns.is_unique, name
    assert DATE in frame.index, f'{name}: target date missing'
    return frame.loc[:DATE]


def main():
    price = read_matrix('cb_price_cache.csv')
    result = price.loc[DATE].rename('market_price').to_frame()
    for model, market_file in [('BS', 'Market_Prices.csv'), ('ZL', 'ZL_Market_Prices.csv'), ('LSM', 'LSM_Market_Prices.csv')]:
        estimates = read_matrix(f'{model}_Model_Prices.csv')
        market = read_matrix(market_file).loc[DATE].reindex(result.index)
        valid = estimates.loc[DATE].reindex(result.index).notna()
        assert np.allclose(market[valid], result.loc[valid, 'market_price'], equal_nan=False), model
        result[model] = estimates.loc[DATE]
    for model in ['ZL', 'LSM']:
        manifest = json.loads((DATA / f'{model}_Model_Manifest.json').read_text(encoding='utf-8'))
        assert manifest['input_cutoff'] == str(DATE.date()), model
    result['average_price'] = result[['BS', 'ZL', 'LSM']].mean(axis=1, skipna=False)
    result['upside_pct'] = (result.average_price / result.market_price - 1) * 100
    result['discount_to_fair_pct'] = (1 - result.market_price / result.average_price) * 100
    for label, filename in [('rating','cb_rating_cache.csv'), ('bond_floor','cb_bond_floor_cache.csv'), ('maturity_years','cb_maturity_cache.csv'), ('balance_wan','cb_balance_cache.csv'), ('bps','cb_bps_cache.csv'), ('conversion_value','cb_convert_val_cache.csv')]:
        result[label] = read_matrix(filename).loc[DATE]
    amount = read_matrix('cb_amount_cache.csv')
    result['amount_today_wan'] = amount.loc[DATE]
    result['amount_20d_mean_wan'] = amount.tail(20).mean()
    result['amount_20d_observations'] = amount.tail(20).notna().sum()
    result['bond_premium_pct'] = (result.market_price / result.bond_floor - 1) * 100
    result['conversion_premium_pct'] = (result.market_price / result.conversion_value - 1) * 100
    result['volatility_20d_pct'] = price.tail(21).pct_change(fill_method=None).std() * np.sqrt(252) * 100
    basic = pd.read_csv(DATA / 'cb_basic_info.csv').set_index('ts_code')
    financial_raw = json.loads((DATA / 'logs/financial_risk_20260911.json').read_text(encoding='utf-8'))
    financial = pd.DataFrame(financial_raw['records']).set_index('bond_code')
    for field in ['eps', 'profit_dedt', 'roe', 'debt_to_assets', 'ocfps', 'ann_date']:
        result['H1_' + field] = financial[field]
    result['name'] = basic['bond_short_name']
    result['stock_name'] = basic['stk_short_name']
    raw = json.loads((DATA / 'logs/jsl_redeem_20260911.json').read_text(encoding='utf-8'))
    risk = pd.DataFrame([x['cell'] for x in raw['data']['rows']]).set_index('bond_id')
    ids = result.index.str.split('.').str[0]
    for out, source in [('call_status','redeem_icon'), ('call_counter','redeem_count'), ('last_trading_day','delist_dt'), ('jsl_price','price')]:
        result[out] = ids.map(risk[source])
    result['risk_record_found'] = ids.isin(risk.index)
    result['call_counter'] = result.call_counter.fillna('').map(lambda x: re.sub('<[^>]+>', '', str(x)))
    rating_events = pd.DataFrame(json.loads((DATA / 'logs/rating_events_20260911.json').read_text(encoding='utf-8'))).set_index('ts_code')
    result['rating_ann_date'] = rating_events['ann_date']
    result['rating_agency'] = rating_events['rating_com_name']
    result['rating'] = result.rating.astype('string').str.strip()
    conditions = {
        'three_models_present': result[['BS','ZL','LSM']].notna().all(axis=1),
        'rating_AAplus_or_AAA': result.rating.isin(['AA+', 'AAA']),
        'positive_average_upside': result.upside_pct.gt(0),
        'price_at_most_130': result.market_price.le(130),
        'bond_premium_at_most_25pct': result.bond_premium_pct.le(25),
        'maturity_at_least_half_year': result.maturity_years.ge(.5),
        'balance_at_least_3yi': result.balance_wan.ge(30000),
        'liquidity_mean_at_least_1000wan': result.amount_20d_mean_wan.ge(1000) & result.amount_20d_observations.ge(15) & result.amount_today_wan.gt(0),
        'positive_stock_net_assets': result.bps.gt(0),
        'H1_profit_and_recurring_profit_positive': result.H1_eps.gt(0) & result.H1_profit_dedt.gt(0),
        'nonbank_leverage_and_cashflow': result.stock_name.fillna('').str.contains('银行') | (result.H1_debt_to_assets.le(75) & result.H1_ocfps.gt(0)),
        'no_ST_stock': ~result.stock_name.fillna('').str.contains('ST|退', regex=True, case=False),
        'redeem_status_clear': result.risk_record_found & result.call_status.isin(['', 'G']),
        'no_near_delisting': pd.to_datetime(result.last_trading_day, errors='coerce').isna() | pd.to_datetime(result.last_trading_day, errors='coerce').gt(DATE + pd.Timedelta(days=180)),
    }
    passed = pd.Series(True, index=result.index)
    counts = {}
    for label, mask in conditions.items():
        passed &= mask.fillna(False)
        counts[label] = int(passed.sum())
    result['risk_eligible'] = pd.DataFrame({k: v.fillna(False) for k, v in conditions.items() if k != 'positive_average_upside'}).all(axis=1)
    result['eligible'] = passed
    result['excluded_by'] = ['; '.join(k for k,v in conditions.items() if not bool(v.fillna(False).loc[code])) for code in result.index]
    ranked = result[result.market_price.gt(0)].sort_values('upside_pct', ascending=False)
    high_rating_top = ranked[ranked.rating.isin(['AA+', 'AAA']) & ranked.upside_pct.gt(0)].head(5)
    selected = ranked[ranked.eligible].head(5)
    watchlist = ranked[ranked.risk_eligible].head(5)
    columns = ['name','rating','market_price','BS','ZL','LSM','average_price','upside_pct','bond_floor','bond_premium_pct','maturity_years','balance_wan','amount_20d_mean_wan','volatility_20d_pct','call_status','call_counter']
    print('Progressive filter counts:', json.dumps(counts))
    print('Selected:\n', selected[columns].to_string())
    print('Top five passing risk filters (negative upside is NOT undervalued):\n', watchlist[columns].to_string())
    print('AA+/AAA before risk filters:\n', ranked[ranked.rating.isin(['AA+','AAA'])].head(20)[columns+['excluded_by']].to_string())
    output = {'valuation_date':str(DATE.date()), 'formula':'((BS+ZL+LSM)/3/market_price-1)*100', 'risk_source':raw['url'], 'risk_fetched_at':raw['fetched_at'], 'filter_counts':counts, 'selected':json.loads(selected.reset_index(names='ts_code').to_json(orient='records', force_ascii=False)), 'high_rating_top_five':json.loads(high_rating_top.reset_index(names='ts_code').to_json(orient='records', force_ascii=False)), 'risk_top_five':json.loads(watchlist.reset_index(names='ts_code').to_json(orient='records', force_ascii=False)), 'all_ranked':json.loads(ranked.reset_index(names='ts_code').to_json(orient='records', force_ascii=False))}
    (ROOT / 'summary/CB_screen_20260911.json').write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding='utf-8')

    display = high_rating_top.reset_index(names='ts_code')[['ts_code','name','rating','market_price','BS','ZL','LSM','average_price','upside_pct']]
    display.columns = ['代码','转债','评级','收盘价','BS','ZL','LSM','三模型均价','均价相对市价差(%)']
    risk_display = watchlist.reset_index(names='ts_code')[['name','bond_floor','bond_premium_pct','maturity_years','balance_wan','amount_20d_mean_wan','H1_eps','H1_debt_to_assets','rating_ann_date']].copy()
    risk_display['balance_wan'] /= 10000
    risk_display.columns = ['转债','纯债价值','纯债溢价(%)','剩余年限','余额(亿元)','20日均成交额(万元)','半年报EPS','资产负债率(%)','评级公告日']
    eligible_count = int(ranked.eligible.sum())
    lines = [
        '# 2026-09-11 转债三模型平均估值与风险筛选',
        '',
        f'采用 2026-09-11 收盘数据。本次保守风险筛选后，三模型均价高于市价的转债共 **{eligible_count} 只**。下表列出 AA+／AAA 中三模型均值显示低估的前五名，供估值与风险对照；它们并不都满足低风险条件。',
        '',
        display.to_markdown(index=False, floatfmt='.3f'),
        '',
        '三模型均价 = (BS + ZL + LSM) / 3；估值差 = (三模型均价 / 市价 - 1) × 100%。三模型均要求目标日期有有效价格，且对应市价一致。该差值为模型估值差，不是预期收益率。',
        '',
        '筛选口径：AA+ 或 AAA；价格不高于 130 元；纯债溢价不超过 25%；剩余期限不少于半年；余额不少于 3 亿元；近 20 个交易日平均成交额不少于 1000 万元，至少 15 日有观测，目标日成交非零。',
        '',
        '基本面与事件：正股非 ST、净资产为正，已公告的 2026 年半年报 EPS 和扣非净利润均为正；非银行发行人资产负债率不高于 75%、经营现金流为正。银行因业务结构不同不套用工业企业的杠杆和现金流门槛。排除集思录显示强赎、即将强赎或已经满足强赎条件，以及已知半年内退市的转债。',
        '',
        '通过保守风险筛选的估值差前五名如下。除节能转债外，其余四只均价低于市价，仅作观察，不称为低估：',
        '',
        risk_display.to_markdown(index=False, floatfmt='.3f'),
        '',
        '高评级低估榜的风险差异：晶能半年报扣非亏损约 33.61 亿元；金田经营现金流为负且强赎计数 12/15；节能符合本次门槛；平煤纯债溢价约 30.36%；洪城纯债溢价约 85.77%。',
        '',
        '“风险较低”仅表示通过上述相对保守的筛选。纯债价值是估计值，不是损失下限。银行候选仍存在信贷和行业集中风险；其他发行人仍受经营及正股波动影响。',
        '',
        '模型结构：本项目 LSM 输出取 max(ZL 条款模型, LSM 自愿转股价值)，因此三模型并非三份独立证据。BS 不含完整强赎条款，均值可能受到模型分歧影响。',
        '',
        '数据来源：[Tushare 日行情](https://tushare.pro/document/2?doc_id=187)、[评级历史](https://tushare.pro/document/2?doc_id=458)、[财务指标](https://tushare.pro/document/2?doc_id=79)，以及[集思录强赎信息](https://www.jisilu.cn/data/cbnew/#redeem)。Tushare 赎回接口无权限，本次强赎状态使用集思录快照。',
        '',
        f"强赎数据抓取时间：{raw['fetched_at']}（本机北京时间）。各条财务记录均限制公告日不晚于 2026-09-11。原始风险快照保存在 backtest/logs 下对应日期 JSON 文件中。",
        '',
        '覆盖与更新限制：本次 313 只存在市场价格的可转债中，300 只具备完整三模型估值。兴业、重银、常银等银行转债由于缺少模型要求的回售参数而未进入平均排名。不能据此推断这些银行转债不低估。行情、三模型、基准与独立三模型多空策略已更新；六因子 IC 及其关联回测未完成同步，因为原始 8 月 28 日输入本身无法匹配旧 IC Manifest。未删除 Manifest 或重建 IC 历史。',
        '',
        '完整排名、被排除原因及详细指标见同目录 CB_screen_20260911.json；计算逻辑见 screen_20260911.py。',
    ]
    (ROOT / 'summary/CB_screen_20260911.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')

if __name__ == '__main__':
    main()
