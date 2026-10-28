"""Independent LSM API; no ZL prices are accepted."""
from pricing_engine import Contract, price_mc
import numpy as np


def price_lsm_batch(contracts, *, paths=8192, exercise_steps=192, seed=42):
    return np.array([price_mc(c, method="LSM", paths=paths,
        exercise_steps=exercise_steps, seed=seed+i)[0] for i,c in enumerate(contracts)])
