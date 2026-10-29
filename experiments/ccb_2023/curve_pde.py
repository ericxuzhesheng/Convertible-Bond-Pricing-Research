"""CCB with observed zero curve and contractual barrier: log-space PDE.

Use W=V-S. Terminal W is a put with credit-adjusted cash value; W=0 on
the absorbing call barrier after protection. Discount factors and exact dated
cash flows are shared input conventions, not another model's theoretical value.
Crank-Nicolson with two implicit half steps after payoff/jump events.
"""
import math
import numpy as np
from numba import njit
from scipy.special import ndtr


@njit(cache=True, nogil=True)
def _discount(t, tenors, rates):
    return math.exp(-np.interp(t, tenors, rates)*t)


@njit(cache=True, nogil=True)
def _solve(parity, sigma, expiry, strike, ct, adjusted_coupons, protection,
           barrier, tenors, rates, dx, max_dt):
    tp = min(expiry, protection)
    x0 = math.log(parity/barrier)
    lower = min(x0, math.log(strike/barrier), 0.)-max(3., 8*sigma*math.sqrt(expiry))
    upper = max(x0, 0.)+max(1., 8*sigma*math.sqrt(tp))
    nleft = int(math.ceil(-lower/dx))
    nright = int(math.ceil(upper/dx))
    n = nleft+nright+1
    stock = barrier*np.exp((np.arange(n)-nleft)*dx)
    w = np.maximum(strike-stock, 0.)
    if tp < expiry:
        w[nleft:] = 0.
    nodes = np.linspace(0., expiry, max(2, int(math.ceil(expiry/max_dt))+1))
    extras = np.concatenate((ct, np.array([tp]), tenors[(tenors>0)&(tenors<expiry)]))
    times = np.unique(np.concatenate((nodes, extras)))
    # Exact calendar fractions can differ from linspace by one floating-point
    # ulp. Merge those coincident events before taking implicit half steps.
    keep = np.ones(len(times),dtype=np.bool_)
    for j in range(1,len(times)):
        if times[j]-times[j-1]<1e-12:
            keep[j]=False
    times=times[keep]
    work = np.empty(n); cprime = np.empty(n)
    lowercoef = np.empty(n); diag = np.empty(n); uppercoef = np.empty(n)
    terminal_pv = strike*_discount(expiry, tenors, rates)
    cpv = np.empty(len(ct))
    for j in range(len(ct)):
        cpv[j] = adjusted_coupons[j]*_discount(ct[j], tenors, rates)
    for k in range(len(times)-2, -1, -1):
        high, low = times[k+1], times[k]
        if high <= low:
            continue
        post = high > tp+1e-12
        last = nleft if post else n-1
        event = k == len(times)-2 or abs(high-tp)<1e-12
        for j in range(len(ct)):
            if abs(high-ct[j])<1e-12:
                event = True
        halves = 2 if event else 1
        theta = 1. if event else .5
        for half in range(halves):
            t1 = high-(high-low)*half/halves
            t0 = high-(high-low)*(half+1)/halves
            dt = t1-t0
            d0, d1 = _discount(t0,tenors,rates), _discount(t1,tenors,rates)
            rate = math.log(d0/d1)/dt
            mu = rate-.5*sigma*sigma
            a = .5*sigma*sigma/(dx*dx)-mu/(2*dx)
            c = .5*sigma*sigma/(dx*dx)+mu/(2*dx)
            # Degenerate low-volatility limit: monotone one-sided drift.
            if min(a,c)<0:
                a = .5*sigma*sigma/(dx*dx)+max(-mu,0.)/dx
                c = .5*sigma*sigma/(dx*dx)+max(mu,0.)/dx
            b = -a-c-rate
            cash0, cash1, top0, top1 = terminal_pv, terminal_pv, 0., 0.
            for j in range(len(ct)):
                # w at high already includes its coupon; at low the coupon
                # jump is added after propagation, so do not count it twice.
                if ct[j] > t0+1e-12:
                    cash0 += cpv[j]
                    if ct[j] <= tp+1e-12:
                        top0 += cpv[j]
                if ct[j] >= t1-1e-12:
                    cash1 += cpv[j]
                    if ct[j] <= tp+1e-12:
                        top1 += cpv[j]
            oldleft = cash1/d1-stock[0]
            newleft = cash0/d0-stock[0]
            oldright = 0. if post else top1/d1
            newright = 0. if post else top0/d0
            w[0], w[last] = oldleft, oldright
            for j in range(1,last):
                lowercoef[j] = -theta*dt*a
                diag[j] = 1-theta*dt*b
                uppercoef[j] = -theta*dt*c
                work[j] = (1+(1-theta)*dt*b)*w[j]+(1-theta)*dt*(a*w[j-1]+c*w[j+1])
            work[1] -= lowercoef[1]*newleft
            work[last-1] -= uppercoef[last-1]*newright
            cprime[1] = uppercoef[1]/diag[1]
            work[1] /= diag[1]
            for j in range(2,last):
                denominator = diag[j]-lowercoef[j]*cprime[j-1]
                cprime[j] = uppercoef[j]/denominator
                work[j] = (work[j]-lowercoef[j]*work[j-1])/denominator
            w[last-1] = work[last-1]
            for j in range(last-2,0,-1):
                w[j] = work[j]-cprime[j]*w[j+1]
            w[0], w[last] = newleft, newright
        if abs(low-tp)<1e-12 and tp < expiry:
            w[nleft:] = 0.
        for j in range(len(ct)):
            if abs(low-ct[j])<1e-12:
                limit = nleft if low > tp+1e-12 else n
                w[:limit] += adjusted_coupons[j]
    position = x0/dx+nleft
    left = int(math.floor(position))
    # Local cubic interpolation reduces location bias without moving barrier.
    if 1 <= left < n-2:
        z = position-left
        value = (-z*(z-1)*(z-2)/6*w[left-1]
                 +(z+1)*(z-1)*(z-2)/2*w[left]
                 -(z+1)*z*(z-2)/2*w[left+1]
                 +(z+1)*z*(z-1)/6*w[left+2])
    else:
        value = np.interp(position, np.arange(n), w)
    return parity+max(0.,value)


def price_curve_ccb(contract, *, protection, barrier, dx=.02, max_dt=1/96):
    c = contract
    c.validate()
    if np.isnan(barrier) or barrier <= 0 or protection < c.conversion_start-1e-12:
        raise ValueError('invalid barrier or protection before conversion starts')
    expiry = min(c.maturity, c.conversion_end)
    all_times = np.r_[c.coupon_times, c.maturity]
    all_cash = np.r_[c.coupon_amounts, c.redemption]
    adjusted = all_cash*np.exp(-c.spread*all_times)
    if expiry <= 0 or c.conversion_start > expiry:
        return float(np.sum(adjusted*c.discount(all_times)))
    before = all_times < expiry-1e-12
    # Honour a final conversion day before redemption payment. Beyond-expiry
    # contractual payments are represented by their value on that choice date.
    strike = float(np.sum(adjusted[~before]*c.discount(all_times[~before]))/c.discount(expiry))
    if np.isinf(barrier) or protection >= expiry:
        sd = c.sigma*math.sqrt(expiry)
        discount = float(c.discount(expiry))
        if sd == 0:
            terminal = max(c.parity, strike*discount)
        else:
            d1 = (math.log(c.parity/(strike*discount))+.5*sd*sd)/sd
            terminal = c.parity*ndtr(d1)+strike*discount*ndtr(-d1+sd)
        return float(terminal+np.sum(adjusted[before]*c.discount(all_times[before])))
    if protection == 0 and c.parity >= barrier:
        return c.parity
    return float(_solve(c.parity,c.sigma,expiry,strike,all_times[before],adjusted[before],
                        protection,barrier,c.curve_tenors,c.curve_rates,dx,max_dt))
