# 周度四模型均价安全边际策略

模型：mean；频率：weekly；模式：tracking。

采用每日OHLCV和日持仓账本，下一交易日开盘调仓。

理论多空，无历史券源验证；历史强赎/ST覆盖不完整；初次抓取的财务历史不是原始发布版本。日净值为价格与已核验现金流的研究估计，尚未验证全部历史付息，不能称为完整总收益。

尚无满足交易前快照要求的跟踪信号。

成本假设：单边综合摩擦5bp，借券年化3.0%，闲置现金收益0；只按实际成交扣费。
详见 targets.csv、holdings.csv、trades.csv、daily_nav.csv、coverage.csv 和 acquisition.json。
