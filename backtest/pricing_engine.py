"""Cash-flow-consistent European, clause-MC and Longstaff-Schwartz prices.

Values are full prices per bond. Cash uses D(t)*exp(-spread*t); shares use
D(t). This is a TF-style credit approximation, not an explicit default model.
Issuer calls terminate paths. No price from another model enters valuation.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
import numpy as np
from scipy.special import ndtr
from numba import njit


@dataclass
class Contract:
    parity: float
    maturity: float
    redemption: float
    sigma: float
    spread: float
    par: float = 100.0
    conversion_start: float = 0.0
    conversion_end: float = math.inf
    call_start: float = 0.0
    call_ratio: float = math.inf
    call_window: int = 30
    call_required: int = 15
    call_notice_days: int = 30
    forced_call_time: float = math.inf
    put_start: float = math.inf
    put_ratio: float = 0.0
    put_days: int = 30
    initial_put_count: int = 0
    put_redemption: float | None = None
    put_includes_accrued: bool = False
    call_history: np.ndarray = field(default_factory=lambda: np.zeros(30, dtype=np.int8))
    coupon_times: np.ndarray = field(default_factory=lambda: np.empty(0))
    coupon_amounts: np.ndarray = field(default_factory=lambda: np.empty(0))
    # Accrual intervals relative to valuation: [start, end), annual cash rate.
    accrual_starts: np.ndarray = field(default_factory=lambda: np.empty(0))
    accrual_ends: np.ndarray = field(default_factory=lambda: np.empty(0))
    accrual_rates: np.ndarray = field(default_factory=lambda: np.empty(0))
    curve_tenors: np.ndarray = field(default_factory=lambda: np.array([1., 10.]))
    curve_rates: np.ndarray = field(default_factory=lambda: np.array([0.02, 0.02]))
    dividend_yield: float = 0.0

    def validate(self):
        required = [self.parity, self.maturity, self.redemption, self.sigma,
                    self.spread, self.par, self.dividend_yield, self.conversion_start]
        if not np.isfinite(required).all() or min(self.parity, self.maturity, self.redemption, self.par) <= 0:
            raise ValueError("invalid contract prices, maturity or conversion start")
        if self.sigma < 0 or self.spread < 0 or self.dividend_yield < 0:
            raise ValueError("negative volatility, spread or dividend yield")
        if self.put_redemption is not None and (not np.isfinite(self.put_redemption) or self.put_redemption <= 0):
            raise ValueError("invalid put redemption")
        if not 1 <= self.call_required <= self.call_window or self.put_days < 1 or self.call_notice_days < 0:
            raise ValueError("invalid clause observation windows")
        if len(self.call_history) != self.call_window or not np.isin(self.call_history, [0, 1]).all():
            raise ValueError("invalid observed call history")
        if len(self.curve_tenors) != len(self.curve_rates) or not len(self.curve_rates):
            raise ValueError("invalid yield curve")
        if not np.isfinite(self.curve_rates).all() or not np.isfinite(self.curve_tenors).all() or np.any(np.diff(self.curve_tenors) <= 0):
            raise ValueError("yield curve must have finite, increasing tenors")
        if len(self.coupon_times) != len(self.coupon_amounts) or not np.isfinite(self.coupon_amounts).all():
            raise ValueError("invalid contractual coupons")
        if np.any(self.coupon_times <= 0) or np.any(self.coupon_times >= self.maturity) or np.any(self.coupon_amounts < 0):
            raise ValueError("intermediate coupons must precede maturity; final coupon is in redemption")
        if not (len(self.accrual_starts) == len(self.accrual_ends) == len(self.accrual_rates)):
            raise ValueError("invalid accrual schedule")

    def discount(self, times):
        t = np.asarray(times, dtype=float)
        return np.exp(-np.interp(t, self.curve_tenors, self.curve_rates) * t)


def price_bs(c: Contract) -> float:
    """European conversion baseline with contractual redemption and coupons."""
    c.validate()
    full_cash = float(np.sum(c.coupon_amounts*c.discount(c.coupon_times)*np.exp(-c.spread*c.coupon_times))
                      + c.redemption*float(c.discount(c.maturity))*math.exp(-c.spread*c.maturity))
    expiry = min(c.maturity, c.conversion_end)
    if expiry < 0 or c.conversion_start > expiry:
        return full_cash
    if expiry == 0:
        return max(c.parity, full_cash)
    discount = float(c.discount(expiry))
    before = c.coupon_times <= expiry
    coupons = float(np.sum(c.coupon_amounts[before]*c.discount(c.coupon_times[before])*np.exp(-c.spread*c.coupon_times[before])))
    strike = (full_cash-coupons)/(discount*math.exp(-c.spread*expiry))
    forward = c.parity * math.exp(-c.dividend_yield*expiry) / discount
    deviation = c.sigma * math.sqrt(expiry)
    if deviation == 0:
        equity = c.parity*math.exp(-c.dividend_yield*expiry) if forward >= strike else 0.
        cash = strike*discount*math.exp(-c.spread*expiry) if forward < strike else 0.
    else:
        d1 = (math.log(forward/strike) + .5*deviation**2)/deviation
        d2 = d1-deviation
        equity = c.parity*math.exp(-c.dividend_yield*expiry)*ndtr(d1)
        cash = strike*discount*math.exp(-c.spread*expiry)*ndtr(-d2)
    return float(equity+cash+coupons)


@njit(cache=True, nogil=True)
def _simulate(parity, sigma, par, redemption, q, times, de, dc, coupon_pv,
              exercise_nodes, conversion_start, conversion_end, call_start,
              call_ratio, window, required, notice, forced_call,
              history, put_start, put_ratio, put_days, initial_put,
              exercise_cash, put_cash, paths, seed, force_put, last_conversion,
              supplied_normals=None):
    n = len(times)-1
    nodes = len(exercise_nodes)
    stocks = np.zeros((paths, nodes))
    states = np.zeros((paths, nodes, 3))
    alive = np.zeros((paths, nodes), dtype=np.bool_)
    put_allowed = np.zeros((paths, nodes), dtype=np.bool_)
    cash = np.zeros(paths)
    equity = np.zeros(paths)
    drift = np.zeros(n+1)
    diffusion = np.zeros(n+1)
    for k in range(1,n+1):
        dt = times[k]-times[k-1]
        drift[k] = math.log(de[k-1]/de[k])-q*dt-.5*sigma*sigma*dt
        diffusion[k] = sigma*math.sqrt(dt)
    # Numba's thread-local MT19937 stream is seeded per contract. Store only
    # one antithetic pair's draws; generate beyond the first stopping time only
    # if its partner survives longer. No full path-by-time random array is needed.
    np.random.seed(seed)
    normals = np.empty(n)
    generated = 0
    for p in range(paths):
        sign = 1. if p%2 == 0 else -1.
        if p%2 == 0:
            generated = 0
        flags = history.copy()
        hits = int(np.sum(flags))
        put_count = initial_put
        call_end = n+1
        if times[0] >= call_start and hits >= required:
            call_end = notice
        if np.isfinite(forced_call):
            call_end = min(call_end, int(np.searchsorted(times, forced_call)))
        s = parity
        node = 0
        for k in range(n+1):
            t = times[k]
            if k:
                if supplied_normals is not None:
                    z = supplied_normals[p//2,k-1]
                else:
                    if k > generated:
                        normals[k-1] = np.random.standard_normal()
                        generated = k
                    z = normals[k-1]
                s *= math.exp(drift[k]+diffusion[k]*z*sign)
                if call_start <= t <= conversion_end and call_end > n:
                    slot = (k-1)%window
                    hits -= flags[slot]
                    flags[slot] = 1 if s >= call_ratio*par else 0
                    hits += flags[slot]
                    if hits >= required:
                        call_end = k+notice
                if t >= put_start and s <= put_ratio*par:
                    put_count += 1
                else:
                    put_count = 0
            can_convert = conversion_start <= t <= conversion_end
            can_put = t >= put_start and put_count >= put_days
            stopped = k == n or k >= call_end or (force_put and can_put)
            if stopped:
                amount = redemption if k == n else (exercise_cash[k] if k >= call_end else put_cash[k])
                if can_convert and s >= amount:
                    equity[p] = s*de[k]
                else:
                    cash[p] += amount*dc[k]
                # Coupons at the stopping observation have already been paid;
                # the maturity coupon is deliberately excluded from coupon_pv.
                cash[p] += coupon_pv[k]
                break
            if force_put and k == last_conversion and can_convert:
                # The clause-only policy still has to decide before conversion
                # rights expire, even if maturity cash is paid on a later day.
                end = call_end if call_end <= n else n
                amount = exercise_cash[end] if end < n else redemption
                remaining_cash = (amount*dc[end]+coupon_pv[end]-coupon_pv[k])/dc[k]
                if s >= remaining_cash:
                    equity[p] = s*de[k]
                    cash[p] += coupon_pv[k]
                    break
            if node < nodes and k == exercise_nodes[node]:
                stocks[p, node] = s
                alive[p, node] = True
                put_allowed[p, node] = can_put
                states[p, node, 0] = hits/window
                states[p, node, 1] = min(put_count/put_days, 1.)
                states[p, node, 2] = max(0., call_end-k)/max(1, notice) if call_end <= n else 0.
                node += 1
    return stocks, states, alive, put_allowed, cash, equity


def _basis(stock, state, initial):
    x = stock/initial
    return np.column_stack((np.ones(len(x)), x, x*x, state))


def price_mc(c: Contract, *, method="LSM", paths=512, exercise_steps=48,
             seed=42, trading_days=252, backend="cpu"):
    """Return (price, antithetic standard error).

    LSM learns its stopping rule on one path set and values that rule on an
    independent set. ZL follows the declared clause-trigger stopping policy.
    Both enforce calls and account for coupons in the same path simulation.
    """
    c.validate()
    if method not in ("LSM", "ZL") or paths < 8 or paths%2 or exercise_steps < 2 or trading_days < 1:
        raise ValueError("invalid pricing method, even path count or time grid")
    n = max(1, int(math.ceil(c.maturity*trading_days)))
    times = np.linspace(0., c.maturity, n+1)
    de = c.discount(times)
    dc = de*np.exp(-c.spread*times)
    coupon_pv = np.zeros(n+1)
    for t, amount in zip(c.coupon_times, c.coupon_amounts):
        k = min(n, int(np.searchsorted(times, t)))
        coupon_pv[k:] += amount*float(c.discount(t))*math.exp(-c.spread*t)
    exercise_cash = np.full(n+1, c.par)
    for start, end, annual_cash in zip(c.accrual_starts, c.accrual_ends, c.accrual_rates):
        mask = (times >= start) & (times < end)
        exercise_cash[mask] += annual_cash*(times[mask]-start)
    put_amount = c.par if c.put_redemption is None else c.put_redemption
    put_cash = np.full(n+1,put_amount) if c.put_includes_accrued else exercise_cash-c.par+put_amount
    boundaries = [0, n]
    for boundary, rounding in [(c.conversion_start, math.ceil), (c.conversion_end, math.floor)]:
        if np.isfinite(boundary) and 0 <= boundary <= c.maturity:
            boundaries.append(min(n, max(0, int(rounding(boundary*n/c.maturity)))))
    for t in c.coupon_times:
        k = min(n, int(np.searchsorted(times, t)))
        boundaries.extend([max(0, k-1), k])
    nodes = np.unique(np.r_[np.linspace(0, n, min(n, exercise_steps)+1).round().astype(np.int64), boundaries])
    # Use no exercise-state storage for the clause-only policy.
    if method == "ZL":
        nodes = np.array([0], dtype=np.int64)
    total_paths = paths*2 if method == "LSM" else paths
    if backend == "cpu":
        simulate = _simulate
    elif backend in ("cuda", "cuda-reference"):
        from pricing_cuda import simulate_cuda, simulate_reference
        simulate = simulate_cuda if backend == "cuda" else simulate_reference
    else:
        raise ValueError("invalid simulation backend")
    stock, states, alive, put, cash, equity = simulate(
        c.parity, c.sigma, c.par, c.redemption, c.dividend_yield, times, de, dc,
        coupon_pv, nodes, c.conversion_start, c.conversion_end, c.call_start,
        c.call_ratio, c.call_window, c.call_required, c.call_notice_days,
        c.forced_call_time, np.asarray(c.call_history, dtype=np.int8),
        c.put_start, c.put_ratio, c.put_days, c.initial_put_count,
        exercise_cash, put_cash, total_paths, np.uint64(seed), method == "ZL",
        min(n,int(np.searchsorted(times,c.conversion_end,side='right'))-1))
    if method == "LSM":
        # Ignoring nonnegative coupons, waiting for forced settlement guarantees
        # at least the minimum cash payment at the worst future cash discount.
        # Exercising below this contractual bound is strictly dominated.
        has_call = np.isfinite(c.call_ratio) or np.isfinite(c.forced_call_time) or np.sum(c.call_history)>=c.call_required
        minimum_cash = min(c.redemption,float(exercise_cash.min())) if has_call else c.redemption
        lower_bound = minimum_cash*np.minimum.accumulate(dc[::-1])[::-1]/dc
        for j in range(len(nodes)-1, -1, -1):
            k = nodes[j]
            conversion = c.conversion_start <= times[k] <= c.conversion_end
            immediate = np.maximum(stock[:, j] if conversion else 0.,
                                   np.where(put[:, j], put_cash[k], 0.))
            eligible = alive[:, j] & (immediate > 0) & (immediate > lower_bound[k])
            train = np.flatnonzero(eligible[:paths])
            if not len(train):
                continue
            future = (cash[train]-coupon_pv[k])/dc[k] + equity[train]/de[k]
            x = _basis(stock[train, j], states[train, j], c.parity)
            if len(train) >= x.shape[1]+2:
                beta = np.linalg.lstsq(x, future, rcond=1e-10)[0]
                continuation = x@beta
            else:
                beta = None
                continuation = future.mean()
            def apply_policy(indices,prediction):
                exercise=indices[immediate[indices]>prediction]
                convert=conversion & (stock[exercise,j]>=np.where(put[exercise,j],put_cash[k],0.))
                shares=exercise[convert];take_cash=exercise[~convert]
                cash[shares]=coupon_pv[k];equity[shares]=stock[shares,j]*de[k]
                cash[take_cash]=coupon_pv[k]+put_cash[k]*dc[k];equity[take_cash]=0.
            apply_policy(train,continuation)
            # Evaluation states never decide whether training is performed and
            # evaluation future payoffs never enter the fitted continuation.
            evaluate=np.flatnonzero(eligible[paths:])+paths
            if len(evaluate):
                prediction=_basis(stock[evaluate,j],states[evaluate,j],c.parity)@beta if beta is not None else continuation
                apply_policy(evaluate,prediction)
        samples = (cash+equity)[paths:]
    else:
        samples = cash+equity
    pairs = samples.reshape(-1, 2).mean(axis=1)
    value = float(pairs.mean())
    error = float(pairs.std(ddof=1)/math.sqrt(len(pairs)))
    # An admissible time-zero conversion is a known contractual alternative.
    if c.conversion_start <= 0 <= c.conversion_end and value < c.parity:
        value, error = c.parity, 0.
    return value, error
