"""Final acceptance checks on saved outputs (does not rerun pricing)."""
import json
from pathlib import Path
import numpy as np
import pandas as pd

HERE=Path(__file__).resolve().parent
OUT=HERE/'results'


def main():
    manifest=json.loads((OUT/'manifest.json').read_text(encoding='utf-8'))
    allrows=pd.read_csv(OUT/'CCB_observations.csv')
    common=pd.read_csv(OUT/'common_sample.csv')
    metrics=pd.read_csv(OUT/'comparison_overall.csv').set_index('model')
    models=['CCB','BS','ZL','LSM']
    assert not allrows.duplicated(['date','code']).any()
    mask=np.isfinite(allrows[models+['market']]).all(axis=1)&allrows[models+['market']].gt(0).all(axis=1)
    pd.testing.assert_frame_equal(common.reset_index(drop=True),allrows.loc[mask].reset_index(drop=True))
    assert len(common)==manifest['common_sample'] and len(allrows)==manifest['prices']
    assert common.floor_source.eq('production Tushare').all()
    assert common.date.nunique()==manifest['weeks']==400
    assert common.date.max()==manifest['cutoff']
    for model in models:
        delta=common[model].to_numpy()-common.market.to_numpy()
        assert np.isclose(np.abs(delta).mean(),metrics.at[model,'MAE'],rtol=0,atol=1e-10)
        assert np.isclose(np.sqrt(np.dot(delta,delta)/len(delta)),metrics.at[model,'RMSE'],rtol=0,atol=1e-10)
        assert int(metrics.at[model,'n'])==len(common)
    coverage=pd.read_csv(OUT/'coverage.csv')
    issues=pd.read_csv(OUT/'pricing_issues.csv')
    assert coverage.priced.sum()==len(allrows)
    assert coverage.quoted.sum()==len(allrows)+len(issues)
    before=pd.read_csv(HERE/'inputs/missing_before.csv')
    after=pd.read_csv(HERE/'inputs/missing_after.csv')
    repairs=pd.read_csv(HERE/'inputs/repairs.csv')
    assert len(before)==len(after)+len(repairs)
    used=allrows.floor_source.ne('production Tushare')
    numeric=pd.read_csv(OUT/'observed_grid_convergence.csv')
    assert numeric.difference.abs().max()<manifest['observed_grid_tolerance_yuan']
    for path in ['REPORT.md','comparison_by_year.png','numerical_validation.json']:
        assert (OUT/path).is_file() and (OUT/path).stat().st_size>0
    result=dict(passed=True,common_sample=len(common),all_prices=len(allrows),
        common_weeks=common.date.nunique(),common_bonds=common.code.nunique(),
        source_supplements=len(repairs),supplements_successfully_priced=int(used.sum()),
        unresolved_field_cells=len(after),issue_observations=len(issues),
        acceptance=['exact common-sample intersection','no duplicate bond-date',
                    'all four MAE/RMSE independently recomputed','coverage and exclusion reconciliation',
                    'source supplement segregation','repair reconciliation','moderate grid tolerance'])
    (OUT/'acceptance.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result,indent=2))


if __name__=='__main__':main()
