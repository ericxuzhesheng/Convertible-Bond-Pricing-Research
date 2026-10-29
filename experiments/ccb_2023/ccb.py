"""Guosheng 2023 CCB assumptions, evaluated by deterministic Gaussian quadrature.

With zero dividend yield, stopped discounted conversion value is a martingale.
Thus CCB = parity + a down-side terminal payment on surviving paths + coupons.
The terminal strike is credit-adjusted redemption (report pp. 6-8), NOT face.
After protection expires, the log-price transition density is absorbed at the
call barrier. Integrating that density over the protection-end normal law is
equivalent to the report's bivariate-normal expressions (appendices 1 and 2).
This implementation uses 1-D quadrature rather than stochastic normal CDFs.
"""
from functools import lru_cache
import math

import numpy as np
from scipy.special import ndtr, log_ndtr, roots_legendre


@lru_cache(None)
def quadrature(order):
    return roots_legendre(order)


def survival_moments(parity, sigma, rate, protection, times, ceilings, barrier=130., order=96):
    """Return P(alive, S_t < ceiling) and E[S_t 1(alive, S_t < ceiling)]."""
    times, ceilings = np.broadcast_arrays(np.asarray(times, float), np.asarray(ceilings, float))
    if np.any(times <= protection) or protection < 0 or sigma <= 0:
        raise ValueError('survival moments require t > protection >= 0 and sigma > 0')
    mu = rate - .5*sigma*sigma
    x0 = math.log(parity/barrier)
    if protection == 0:
        if x0 >= 0:
            return np.zeros_like(times), np.zeros_like(times)
        x, weights = np.array([x0]), np.array([1.])
    else:
        sd0 = sigma*math.sqrt(protection)
        zbar = -(x0+mu*protection)/sd0
        # The omitted normal tail is < 2e-33; under the share measure the
        # lower tail is smaller still. No unbounded payoff lives in this tail.
        if zbar <= -12:
            return np.zeros_like(times), np.zeros_like(times)
        nodes, w = quadrature(order)
        left, right = -12., min(12., zbar)
        z = left+(nodes+1)*(right-left)/2
        weights = w*(right-left)/2*np.exp(-z*z/2)/math.sqrt(2*math.pi)
        x = x0+mu*protection+sd0*z
    u = (times-protection)[..., None]
    v = sigma*sigma*u
    sd = np.sqrt(v)
    b = np.minimum(0., np.log(ceilings/barrier))[..., None]
    reflection = -2*mu*x/(sigma*sigma)
    a = (b-x-mu*u)/sd
    reflected = (b+x-mu*u)/sd
    prob = ndtr(a)-np.exp(reflection+log_ndtr(reflected))
    moment = barrier*(np.exp(x+rate*u+log_ndtr(a-sd))
                      - np.exp(reflection-x+rate*u+log_ndtr(reflected-sd)))
    if np.min(prob) < -1e-10 or np.min(moment) < -1e-8:
        raise ArithmeticError('negative absorbed transition probability/moment')
    return np.maximum(prob, 0.)@weights, np.maximum(moment, 0.)@weights


def price_ccb(*, parity, sigma, maturity, redemption, coupon_times=(), coupon_amounts=(),
              protection=1/12, rate=math.log1p(.02), spread=0., barrier=130., order=96):
    """Full price per bond; annual continuous rates; no puts/resets/dividends.

Coupons at protection end are paid before any immediate call, following the
report's within-protection cash-flow partition. Maturity redemption already
includes the final coupon. Zero volatility is evaluated deterministically.
"""
    ct, ca = np.asarray(coupon_times, float), np.asarray(coupon_amounts, float)
    values = [parity, sigma, maturity, redemption, protection, rate, spread, barrier]
    if (not np.isfinite(values).all() or min(parity, maturity, redemption, barrier) <= 0
            or min(sigma, spread, protection) < 0 or ct.shape != ca.shape
            or not np.isfinite(ct).all() or not np.isfinite(ca).all()
            or np.any(ct <= 0) or np.any(ct >= maturity) or np.any(ca < 0)):
        raise ValueError('invalid CCB inputs')
    tp = min(protection, maturity)
    strike = redemption*math.exp(-spread*maturity)
    coupon_pv = ca*np.exp(-(rate+spread)*ct)
    if sigma == 0:
        call = math.inf
        if tp < maturity:
            if parity*math.exp(rate*tp) >= barrier:
                call = tp
            elif rate > 0:
                call = math.log(barrier/parity)/rate
        paid = (ct <= tp) | (ct < call)
        put = max(0., strike-parity*math.exp(rate*maturity)) if call >= maturity else 0.
        return float(parity+math.exp(-rate*maturity)*put+coupon_pv[paid].sum())
    if tp == maturity:
        sd = sigma*math.sqrt(maturity)
        d1 = (math.log(parity/strike)+(rate+.5*sigma*sigma)*maturity)/sd
        put = strike*math.exp(-rate*maturity)*ndtr(-d1+sd)-parity*ndtr(-d1)
        return float(parity+put+coupon_pv.sum())
    after = ct > tp
    times = np.r_[ct[after], maturity]
    ceilings = np.r_[np.full(after.sum(), barrier), strike]
    probability, moment = survival_moments(parity, sigma, rate, tp, times, ceilings, barrier, order)
    put = math.exp(-rate*maturity)*(strike*probability[-1]-moment[-1])
    if put < -1e-7:
        raise ArithmeticError('negative surviving terminal put')
    return float(parity+max(0., put)+coupon_pv[~after].sum()+coupon_pv[after]@probability[:-1])
