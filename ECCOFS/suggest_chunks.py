"""Suggest ~10 MB chunk shapes for ECCOFS variables using dynamic_chunks.iterative_ratio_increase_algo.

Reproduces the LAYOUTS in rechunk_vars.py. Needs dynamic_chunks on the path:
    PYTHONPATH=/shared/code-sprint-2026/dynamic_chunks python suggest_chunks.py

even_divisor_algo is not useful here: xi_rho=1667 is prime, so it always returns a thin strip (2, 50, 13, 1667).
"""
import math

import pandas as pd
from dynamic_chunks.algorithms import iterative_ratio_increase_algo

# Aspect ratio = desired relative number of chunks along (time, level, y, x); -1 = do not chunk that dim.
RATIOS = {
    "balanced": (1, 1, 1, 1),
    "map": (4, -1, 1, 1),
}


def suggest(da, ratio, target="10MB", tolerance=0.2):
    """Chunk shape (dict dim->size) for DataArray `da`; `ratio` is ordered like da.dims."""
    ds = da.to_dataset(name="v").drop_vars([c for c in da.coords], errors="ignore")
    return iterative_ratio_increase_algo(ds, target, dict(zip(da.dims, ratio)), size_tolerance=tolerance)


def summarize(da, chunks):
    """One row of facts about a chunk shape."""
    shape = [da.sizes[d] for d in da.dims]
    c = [chunks[d] for d in da.dims]
    nchunks = math.prod(math.ceil(n / k) for n, k in zip(shape, c))
    return {
        "chunks": tuple(c),
        "chunk MB": math.prod(c) * da.dtype.itemsize / 1e6,
        "chunks per dim": tuple(math.ceil(n / k) for n, k in zip(shape, c)),
        "total chunks": nchunks,
    }


def compare(da, ratios=RATIOS, **kw):
    """Table of the current (encoded) chunking plus one suggestion per named ratio."""
    rows = {}
    current = da.encoding.get("preferred_chunks") or dict(zip(da.dims, da.encoding.get("chunks", ())))
    if current:
        rows["current"] = summarize(da, current)
    for name, ratio in ratios.items():
        rows[name] = summarize(da, suggest(da, ratio, **kw))
    return pd.DataFrame(rows).T


if __name__ == "__main__":
    from rechunk_vars import open_source

    ds = open_source()
    for v in ("temp", "u", "v"):
        print(v)
        print(compare(ds[v]).to_string(), "\n")
