"""Independent corrected ZL pricing entrypoint.

Reads observed caches only. Initialise a new version with reprice_history.py;
routine --weekly runs verify and append to the model's own manifest.
The corrected contract is currently validated on CPU.
"""
from pricing_backtest import main

if __name__ == "__main__":
    main("ZL")
