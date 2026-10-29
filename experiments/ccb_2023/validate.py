"""Independent bridge Monte Carlo and constant-curve analytical validation."""
from dataclasses import replace
import json
import math
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from numba import njit

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[1]/'backtest'))
from pricing_engine import Contract
from ccb import price_ccb
from curve_pde import price_curve_ccb


@njit(cache=True,nogil=True)
def _bridge_mc(parity,sigma,barrier,tp,times,discount,coupon_pv,strike,paths,seed):
    np.random.seed(seed)
    means=np.empty(paths//2)
    for pair in range(paths//2):
        s=np.full(2,parity);alive=np.ones(2,dtype=np.bool_);pay=np.zeros(2)
        for k in range(1,len(times)):
            dt=times[k]-times[k-1]
            drift=math.log(discount[k-1]/discount[k])-.5*sigma*sigma*dt
            z=np.random.standard_normal();u=np.random.random()
            for side in range(2):
                if not alive[side]:continue
                old=s[side]
                s[side]*=math.exp(drift+sigma*math.sqrt(dt)*(z if side==0 else -z))
                if times[k-1]>=tp-1e-12:
                    crossing = s[side]>=barrier or old>=barrier
                    if not crossing:
                        p=math.exp(-2*math.log(old/barrier)*math.log(s[side]/barrier)/(sigma*sigma*dt))
                        crossing=(u if side==0 else 1-u)<p
                    if crossing:alive[side]=False
                # At protection end, its coupon precedes immediate conversion.
                if alive[side]:pay[side]+=coupon_pv[k]
                if times[k]>=tp-1e-12 and s[side]>=barrier:alive[side]=False
        for side in range(2):
            if alive[side]:pay[side]+=discount[-1]*max(strike-s[side],0.)
        means[pair]=parity+.5*(pay[0]+pay[1])
    return means.mean(),means.std()/math.sqrt(len(means)-1)


def bridge_mc(c,protection,barrier,paths=32768,steps_per_year=192,seed=41):
    expiry=min(c.maturity,c.conversion_end)
    alltimes=np.r_[c.coupon_times,c.maturity]
    allcash=np.r_[c.coupon_amounts,c.redemption]*np.exp(-c.spread*alltimes)
    before=alltimes<expiry-1e-12
    strike=float(np.sum(allcash[~before]*c.discount(alltimes[~before]))/c.discount(expiry))
    tp=min(protection,expiry)
    times=np.unique(np.r_[np.linspace(0,expiry,int(np.ceil(expiry*steps_per_year))+1),tp,alltimes[before]])
    discount=c.discount(times)
    coupons=np.zeros(len(times))
    for t,amount in zip(alltimes[before],allcash[before]):
        coupons[np.searchsorted(times,t)]+=amount*float(c.discount(t))
    return _bridge_mc(c.parity,c.sigma,barrier,tp,times,discount,coupons,strike,paths,seed)


def main():
    out=HERE/'results';out.mkdir(exist_ok=True)
    records=[]
    for parity in [60.,100.,140.]:
        for tp in [1/12,.5]:
            c=Contract(parity=parity,maturity=6.,redemption=108.,sigma=.5,spread=.03,
                coupon_times=np.arange(1.,6.),coupon_amounts=np.ones(5),
                curve_rates=np.full(2,math.log1p(.02)))
            analytic=price_ccb(parity=parity,sigma=.5,maturity=6,redemption=108,spread=.03,
                protection=tp,coupon_times=c.coupon_times,coupon_amounts=c.coupon_amounts,order=192)
            for curve in ['flat_report','observed_shape_test']:
                if curve!='flat_report':
                    c=replace(c,curve_tenors=np.array([.25,1.,3.,5.,10.]),curve_rates=np.log1p([.012,.018,.027,.034,.038]))
                value=price_curve_ccb(c,protection=tp,barrier=130.)
                fine=price_curve_ccb(c,protection=tp,barrier=130.,dx=.01,max_dt=1/192)
                mc,se=bridge_mc(c,tp,130.,seed=41+len(records))
                record=dict(curve=curve,parity=parity,protection=tp,pde=value,fine_pde=fine,
                    analytic=analytic if curve=='flat_report' else None,mc=mc,mc_se=se,
                    grid_difference=abs(value-fine),mc_z=abs(fine-mc)/se)
                assert abs(value-fine)<.02,record
                if curve=='flat_report':assert abs(value-analytic)<.02,record
                assert abs(fine-mc)<4.5*se+.01,record
                records.append(record)
                print(json.dumps(record),flush=True)
    pd.DataFrame(records).to_csv(out/'numerical_validation.csv',index=False)
    (out/'numerical_validation.json').write_text(json.dumps(dict(passed=True,cases=len(records),
        paths_per_case=32768,bridge_steps_per_year=192,standard_error='independent antithetic pair means',
        estimator='stopped-equity martingale plus simulated surviving coupons and terminal put',
        max_grid_difference=max(r['grid_difference'] for r in records),
        max_mc_z=max(r['mc_z'] for r in records)),indent=2),encoding='utf-8')


if __name__=='__main__':main()
