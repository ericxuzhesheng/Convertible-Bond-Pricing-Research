"""Retired v1 backend. The corrected contract lives in pricing_engine.py."""

def price_batch_cuda(*args, **kwargs):
    raise RuntimeError("Retired v1 pricing API; use pricing_engine.price_mc with an observed Contract")
