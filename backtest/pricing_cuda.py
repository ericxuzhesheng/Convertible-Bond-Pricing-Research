"""Float64 CUDA path simulation; valuation/regression stays in pricing_engine.

Numba's xoroshiro128+ streams are separated by 2**64 steps per pair.
The same supplied draws can be replayed by the CPU reference for pathwise QA.
No CUDA dependency is imported by a normal CPU pricing run.
"""
import math
import numpy as np
from numba import cuda, njit
from numba.cuda.random import (xoroshiro128p_dtype, init_xoroshiro128p_states_cpu,
                               xoroshiro128p_uniform_float64)


@cuda.jit(fastmath=False)
def _paths(parity, par, redemption, times, de, dc, coupon_pv, nodes,
           conversion_start, conversion_end, call_start, call_ratio,
           window, required, notice, forced_end, history, put_start,
           put_ratio, put_days, initial_put, exercise_cash, put_cash,
           force_put, last_conversion, drift, diffusion, rng,
           flags, stocks, states, alive, put_allowed, cash, equity):
    p = cuda.grid(1)
    if p >= len(cash):
        return
    n = len(times)-1
    hits = 0
    for i in range(window):
        flags[p,i] = history[i]
        hits += history[i]
    call_end = n+1
    if times[0] >= call_start and hits >= required:
        call_end = notice
    call_end = min(call_end, forced_end)
    put_count = initial_put
    s = parity
    node = 0
    cash[p] = 0.
    equity[p] = 0.
    for j in range(len(nodes)):
        stocks[p,j] = 0.
        alive[p,j] = False
        put_allowed[p,j] = False
        for i in range(3):
            states[p,j,i] = 0.
    sign = 1. if p%2 == 0 else -1.
    for k in range(n+1):
        t = times[k]
        if k:
            u1 = 1.-xoroshiro128p_uniform_float64(rng,p)
            u2 = xoroshiro128p_uniform_float64(rng,p)
            z = math.sqrt(-2.*math.log(u1))*math.cos(2.*math.pi*u2)
            s *= math.exp(drift[k]+diffusion[k]*z*sign)
            if call_start <= t <= conversion_end and call_end > n:
                slot = (k-1)%window
                hits -= flags[p,slot]
                flags[p,slot] = 1 if s >= call_ratio*par else 0
                hits += flags[p,slot]
                if hits >= required:
                    call_end = k+notice
            if t >= put_start and s <= put_ratio*par:
                put_count += 1
            else:
                put_count = 0
        can_convert = conversion_start <= t <= conversion_end
        can_put = t >= put_start and put_count >= put_days
        if k == n or k >= call_end or (force_put and can_put):
            amount = redemption if k == n else (exercise_cash[k] if k >= call_end else put_cash[k])
            if can_convert and s >= amount:
                equity[p] = s*de[k]
            else:
                cash[p] = amount*dc[k]
            cash[p] += coupon_pv[k]
            break
        if force_put and k == last_conversion and can_convert:
            end = call_end if call_end <= n else n
            amount = exercise_cash[end] if end < n else redemption
            remaining = (amount*dc[end]+coupon_pv[end]-coupon_pv[k])/dc[k]
            if s >= remaining:
                equity[p] = s*de[k]
                cash[p] = coupon_pv[k]
                break
        if node < len(nodes) and k == nodes[node]:
            stocks[p,node] = s
            alive[p,node] = True
            put_allowed[p,node] = can_put
            states[p,node,0] = hits/window
            states[p,node,1] = min(put_count/put_days,1.)
            states[p,node,2] = max(0.,call_end-k)/max(1,notice) if call_end <= n else 0.
            node += 1


def _pair_states(paths,seed):
    states=np.empty(paths//2,dtype=xoroshiro128p_dtype)
    init_xoroshiro128p_states_cpu(states,seed,0)
    return states


@njit(cache=True,nogil=True)
def _reference_draws(states,steps):
    draws=np.empty((len(states),steps))
    for p in range(len(states)):
        for k in range(steps):
            u1=1.-xoroshiro128p_uniform_float64(states,p)
            u2=xoroshiro128p_uniform_float64(states,p)
            draws[p,k]=math.sqrt(-2.*math.log(u1))*math.cos(2.*math.pi*u2)
    return draws


def _draws(paths,steps,seed):
    return _reference_draws(_pair_states(paths,seed),steps)


def simulate_reference(*args):
    """CPU replay with exactly the CUDA backend's normal draws."""
    from pricing_engine import _simulate
    return _simulate(*args, _draws(args[25],len(args[5])-1,args[26]))


def simulate_cuda(parity, sigma, par, redemption, q, times, de, dc, coupon_pv,
                  nodes, conversion_start, conversion_end, call_start,
                  call_ratio, window, required, notice, forced_call,
                  history, put_start, put_ratio, put_days, initial_put,
                  exercise_cash, put_cash, paths, seed, force_put, last_conversion):
    if not cuda.is_available():
        raise RuntimeError("CUDA requested but unavailable; install a compatible CUDA toolkit or choose --backend cpu")
    n = len(times)-1
    dt = np.diff(times)
    drift = np.r_[0.,np.log(de[:-1]/de[1:])-q*dt-.5*sigma*sigma*dt]
    diffusion = np.r_[0.,sigma*np.sqrt(dt)]
    # Partners receive identical initial states in separate mutable slots:
    # their signs differ, and neither thread can race the other's RNG state.
    rng = np.repeat(_pair_states(paths,seed),2)
    # Paths by observation arrays also match the CPU continuation-value code.
    shapes = [(paths,len(nodes)),(paths,len(nodes),3),
              (paths,len(nodes)),(paths,len(nodes)),(paths,),(paths,)]
    device_outputs = [cuda.device_array(shape,dtype=np.bool_ if i in (2,3) else np.float64,order='F')
                      for i,shape in enumerate(shapes)]
    flags = cuda.device_array((paths,window),dtype=np.int8,order='F')
    arrays = [times,de,dc,coupon_pv,nodes,history,exercise_cash,put_cash,drift,diffusion,rng]
    t,d_e,d_c,cp,nd,hi,ec,pc,dr,df,rs = [cuda.to_device(a) for a in arrays]
    forced_end = int(np.searchsorted(times,forced_call)) if np.isfinite(forced_call) else n+1
    _paths[(paths+127)//128,128](parity,par,redemption,t,d_e,d_c,cp,nd,
        conversion_start,conversion_end,call_start,call_ratio,window,required,notice,
        forced_end,hi,put_start,put_ratio,put_days,initial_put,ec,pc,force_put,
        last_conversion,dr,df,rs,flags,*device_outputs)
    return tuple(a.copy_to_host() for a in device_outputs)
