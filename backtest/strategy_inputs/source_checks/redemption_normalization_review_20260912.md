# 赎回公告分类更正（2026-09-12）

原normalize_notices将所有NOTICE_DATE_SH非空记录都写成announced_redemption。该日期字段也包含不提前赎回公告，造成节能、平煤、东南等券误排除，并影响历史日风险退出。原813条不能称为813条已决定赎回公告。

v2要求REDEEM_TYPE=2、公告日和登记日有效、执行价格为正；得到710条来源层面的赎回记录。其余103条有公告日期但未通过执行证据要求的记录保留在redemption_notice_review.csv，状态未知，不代表安全。具体付款仍须发行人公告核验。

原redemption_notices.csv不覆盖；修正版为redemption_notices_v2.csv。event_sources.json记normalization_revision=2及更正时间，原始来源获取时间保持2026-09-12T13:51:30+08:00。新策略从修正输入重新冻结；旧冻结信号不倒填。第一次新策略结果保留在weekly_value_results_event_v1_invalid，已标INVALID，禁止用作有效绩效。

原公告证据：

- [节能2026-05-20不提前赎回公告](https://static.cninfo.com.cn/finalpage/2026-05-20/1225316239.PDF)：公告选择不行使提前赎回权，不是实施赎回。
- [平煤2024-06-29不提前赎回公告](https://static.cninfo.com.cn/finalpage/2024-06-29/1220498530.PDF)：公司决议不提前赎回。
- [东南2026-04-29董事会公告](https://disc.static.szse.cn/download/disc/disk03/finalpage/2026-04-29/3d7cc5b4-54fa-442d-a885-8b00500d579e.PDF)：通过不提前赎回议案。

上述承诺都有期限，不能据旧不赎回公告推断当前永久安全。最新跟踪仍须同周期事件快照，历史仍是事件覆盖不完整的研究线。
