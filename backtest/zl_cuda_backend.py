"""CUDA execution backend for the production ZL Monte Carlo model."""

from __future__ import annotations

import math

import numpy as np
from numba import cuda, int8
from numba.cuda.random import (
    create_xoroshiro128p_states,
    xoroshiro128p_normal_float64,
)


def cuda_is_available() -> bool:
    return bool(cuda.is_available())


def cuda_device_name() -> str:
    name = cuda.get_current_device().name
    return name.decode() if isinstance(name, bytes) else str(name)


@cuda.jit
def _zl_mc_kernel_batch(
    s0_values,
    x0_values,
    rate_values,
    spread_values,
    sigma_values,
    maturity_values,
    redemption_values,
    call_values,
    put_values,
    put_barriers,
    put_windows,
    put_years_values,
    redeem_ratios,
    redeem_windows,
    redeem_required_values,
    initial_put_counts,
    initial_redeem_counts,
    initial_redeem_flags,
    paths,
    rng_states,
    output,
):
    path_index = cuda.grid(1)
    bond_index = path_index // paths
    if bond_index >= s0_values.shape[0]:
        return

    s0 = s0_values[bond_index]
    x0 = x0_values[bond_index]
    rate = rate_values[bond_index]
    spread = spread_values[bond_index]
    sigma = sigma_values[bond_index]
    maturity = maturity_values[bond_index]
    redemption = redemption_values[bond_index]
    call_price = call_values[bond_index]
    put_price = put_values[bond_index]
    put_barrier = put_barriers[bond_index]
    put_window = int(put_windows[bond_index])
    put_years = put_years_values[bond_index]
    redeem_ratio = redeem_ratios[bond_index]
    redeem_window = int(redeem_windows[bond_index])
    redeem_required = int(redeem_required_values[bond_index])

    steps = max(50, int(maturity * 240.0))
    dt = maturity / steps
    put_start_time = maturity - put_years
    if put_start_time < 0.0:
        put_start_time = 0.0
    put_start_index = int(put_start_time / dt)
    drift = (rate - 0.5 * sigma * sigma) * dt
    volatility_step = sigma * math.sqrt(dt)

    stock_price = s0
    conversion_price = x0
    put_count = int(initial_put_counts[bond_index])
    redeem_count = int(initial_redeem_counts[bond_index])
    redeem_flags = cuda.local.array(64, dtype=int8)
    for flag_index in range(64):
        redeem_flags[flag_index] = initial_redeem_flags[
            bond_index, flag_index
        ]

    end_time = maturity
    end_value = 0.0
    active = True
    for step in range(1, steps + 1):
        normal = xoroshiro128p_normal_float64(rng_states, path_index)
        stock_price *= math.exp(
            drift + volatility_step * normal
        )

        if step <= put_start_index:
            put_count = 0
        elif stock_price < put_barrier * conversion_price:
            put_count += 1
        else:
            put_count = 0

        if put_count >= put_window:
            end_time = step * dt
            end_value = put_price
            active = False
            break

        slot = (step - 1) % redeem_window
        redeem_count -= redeem_flags[slot]
        flag = (
            1
            if stock_price >= redeem_ratio * conversion_price
            else 0
        )
        redeem_flags[slot] = flag
        redeem_count += flag
        if redeem_count >= redeem_required:
            end_time = step * dt
            conversion_value = stock_price * (
                x0 / conversion_price
            )
            end_value = max(conversion_value, call_price)
            active = False
            break

    if active:
        conversion_value = x0 / conversion_price * stock_price
        end_value = max(conversion_value, redemption)

    output[path_index] = end_value * math.exp(
        -(rate + spread) * end_time
    )


def price_batch_cuda(
    params,
    *,
    paths: int = 10_000,
    seed: int = 42,
    threads_per_block: int = 256,
) -> np.ndarray:
    """Price one valuation-date batch on CUDA."""
    bond_count = len(params["S0"])
    total_threads = bond_count * paths
    output_device = cuda.device_array(total_threads, dtype=np.float64)
    rng_states = create_xoroshiro128p_states(
        total_threads,
        seed=seed,
    )
    blocks = (
        total_threads + threads_per_block - 1
    ) // threads_per_block
    _zl_mc_kernel_batch[blocks, threads_per_block](
        cuda.to_device(params["S0"]),
        cuda.to_device(params["X0"]),
        cuda.to_device(params["r"]),
        cuda.to_device(params["cs"]),
        cuda.to_device(params["sigma"]),
        cuda.to_device(params["T"]),
        cuda.to_device(params["maturity_redem"]),
        cuda.to_device(params["call_price"]),
        cuda.to_device(params["put_price"]),
        cuda.to_device(params["put_barrier"]),
        cuda.to_device(params["put_window"]),
        cuda.to_device(params["put_years"]),
        cuda.to_device(params["redeem_ratio"]),
        cuda.to_device(params["redeem_window"]),
        cuda.to_device(params["redeem_required"]),
        cuda.to_device(params["initial_put_count"]),
        cuda.to_device(params["initial_redeem_count"]),
        cuda.to_device(params["initial_redeem_flags"]),
        paths,
        rng_states,
        output_device,
    )
    output = output_device.copy_to_host()
    return output.reshape(bond_count, paths).mean(axis=1)
