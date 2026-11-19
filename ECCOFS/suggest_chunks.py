"""Suggest ~10 MB chunk shapes for ECCOFS `temp` using dynamic_chunks.iterative_ratio_increase_algo.

Reproduces the LAYOUTS in rechunk_vars.py. u and v have nearly the same size, so the same shapes are used.
Needs dynamic_chunks on the path: PYTHONPATH=/shared/code-sprint-2026/dynamic_chunks python suggest_chunks.py

even_divisor_algo is not useful here: xi_rho=1667 is prime, so it always returns a thin strip (2, 50, 13, 1667).
"""
from dynamic_chunks.algorithms import iterative_ratio_increase_algo

import dask.array as da
import xarray as xr

from rechunk_vars import LAYOUTS

# aspect ratio = desired relative number of chunks along each dim; -1 = do not chunk that dim
RATIOS = {
    "balanced": dict(time=1, s_rho=1, eta_rho=1, xi_rho=1),
    "map": dict(time=4, s_rho=-1, eta_rho=1, xi_rho=1),
}

if __name__ == "__main__":
    # only shape and dtype matter, so no need to open the real store
    shape = dict(time=166, s_rho=50, eta_rho=1443, xi_rho=1667)
    ds = xr.Dataset({"temp": (tuple(shape), da.zeros(tuple(shape.values()), dtype="float32"))})
    for name, ratio in RATIOS.items():
        chunks = iterative_ratio_increase_algo(ds, "10MB", ratio, size_tolerance=0.2)
        mb = 4 * chunks["time"] * chunks["s_rho"] * chunks["eta_rho"] * chunks["xi_rho"] / 1e6
        print(f"{name}: {chunks}  ({mb:.1f} MB)  matches LAYOUTS: {chunks == LAYOUTS[name]}")
