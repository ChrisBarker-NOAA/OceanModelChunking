# Handoff: ECCOFS temp + u,v rechunking experiment (2026-11-18, updated 2026-11-19)

## Goal
The "best series surface" notebook showed maps and time series read quickly with the existing chunking. We wanted to see whether rechunking changes read performance. We wrote native Icechunk stores of `temp` only to the OSN `esip` bucket, using `dynamic_chunks` to choose ~10 MB chunk shapes.

## What exists

### Data (OSN, `https://usgs.osn.mghpcc.org`, bucket `esip`, anonymous read)
All three hold `temp` with shape (time=166, s_rho=50, eta_rho=1443, xi_rho=1667), float32.

| Store prefix | Chunks (time, s_rho, eta, xi) | Notes |
|---|---|---|
| `rsignell/eccofs-avg-best` | 1, 1, 145, 167 | Original. Virtual Icechunk over NOAA NetCDF (`s3://noaa-nos-eccofs-pds/`). |
| `rsignell/eccofs-avg-temp-balanced-10mb` | 17, 5, 144, 167 | New, native. ~8 MB chunks. 11 commits. |
| `rsignell/eccofs-avg-temp-map-10mb` | 3, 50, 120, 139 | New, native. ~10 MB chunks. 57 commits. |

Both new stores were verified by reading back anonymously. Time axis and `s_rho` match the source, and sampled values at three time/level points are identical to it.

### Code (pushed to `origin/main` as `2ebfb13`)
- `ECCOFS/rechunk_temp.py` — `python rechunk_temp.py {balanced,map}`. Reads the virtual store, rechunks with dask (32 threads), and writes native Icechunk in time batches, one commit each. It resumes if interrupted. Write credentials come from the `osn-esip` profile in `~/.aws/credentials`. `--local DIR`, `--ntime`, `--nlev` are for small tests.
- `ECCOFS/eccofs_temp_chunking_comparison.ipynb` — times five read patterns on all three stores (median of 3 repeats at different times/places) and plots the result. Committed with outputs cleared.

Environment: `/home/conda/code-sprint-2026/envs/code-sprint-2026-rechunking` has icechunk, zarr, xarray and dask. `dynamic_chunks` is not installed there; use `PYTHONPATH=/shared/code-sprint-2026/dynamic_chunks`.

## How the chunk shapes were chosen
`even_divisor_algo` was unusable here. xi_rho=1667 is prime, so it always returned (2, 50, 13, 1667), a thin strip, whatever the aspect ratio. `iterative_ratio_increase_algo` doesn't need divisors:
- `balanced`: aspect ratio all dims 1, tolerance 0.2 → (17, 5, 144, 167), 8.2 MB.
- `map`: aspect ratio time 4, s_rho unchunked (-1), eta 1, xi 1, tolerance 0.2 → (3, 50, 120, 139), 10.0 MB.
- A time-long ratio (eta 4, xi 4) found no match within tolerance.

There is no "padding" parameter in the `dynamic_chunks` checkout (`main` or `refactor-algos`).

## Results (seconds, one run from this machine in us-east-2, median of 3)
| Read | original | balanced | map |
|---|---|---|---|
| surface map, 1 time | **3.3** | 6.7 | 9.0 |
| full-depth map, 1 time | 72.2 | 57.7 | **8.9** |
| surface point series, all times | 5.1 | **0.77** | 4.7 |
| point profile, 1 time | 1.1 | 0.78 | **0.24** |
| point, all levels, 20 times | 15.6 | 1.4 | **0.74** |

- Only a surface-map read wins with the original (tiny, one-level) chunks.
- `map` is best for anything involving full depth. `balanced` is best for surface time series.
- Caveats: single site, 3 repeats, no distributed run, and no in-region (us-east-1) run.

## Things to know
- **Throughput:** the rechunk ran at ~35 MB/s with 32 threads (8 MB/s with 8 threads in the first test). Both layouts took about 40 minutes at the same time. This pod is in AWS us-east-2 (EFS host `…efs.us-east-2.amazonaws.com`, Nebari on EKS). NOAA's bucket is in us-east-1 and OSN is at MGHPCC. Running closer to the data would likely be faster.
- **Credentials:** `~/.aws/credentials` was set to `700`/`600`. The profile is `osn-esip`. `GITHUB_TOKEN` is in `~/.bash_profile`. The push used a temporary credential helper only; the token is not stored in `.git/config`.
- **Local git state is out of sync:** local `main` is "ahead 1, behind 3". The local commit `9d27e2a` is the same change as the pushed `2ebfb13` under a different hash. Two notebooks have uncommitted edits that are not mine: `ECCOFS/eccofs_avg_best_series_surface_temp.ipynb` and `SSCOFS/SSCOFS Inspection.ipynb`. The incoming upstream commits touch the SSCOFS one. Commit or stash both, then run `git pull --rebase`; git should drop the duplicate commit.
- **Nothing is cleaned up in OSN:** no test data was written there, since the small test was local only. The two new prefixes above are the only additions.

## Update 2026-11-19: u,v rechunked into shared Icechunk stores

Same two layouts as temp, but `u` and `v` live together in one repo per layout (data in OSN `esip`, anonymous read):

| Store prefix | Chunks (time, s_rho, eta, xi) | Notes |
|---|---|---|
| `rsignell/eccofs-avg-uv-balanced-10mb` | 17, 5, 144, 167 | u (1443x1666) and v (1442x1667), 166 times x 50 levels |
| `rsignell/eccofs-avg-uv-map-10mb` | 3, 50, 120, 139 | same shapes |

- Each store has `u`, `v` plus `lat/lon_u` and `lat/lon_v`. u and v are on different grids, so dims are `eta_u/xi_u` and `eta_v/xi_v`.
- Verified by anonymous read-back: sizes and chunks are right, and a sampled point matches between the two layouts. **Not** compared against the source virtual store (temp was).
- `ECCOFS/rechunk_temp.py` now takes `--vars` (comma list from temp,u,v, stored together; prefix is `eccofs-avg-<vars joined>-<layout>-10mb`). `python rechunk_temp.py map --vars u,v`. Same chunk shapes, with horizontal dim names mapped to the grid. **This change is not committed.**
- Each full run took ~1 h when the two layouts ran in parallel (map ~73 s per 3-time batch, balanced ~430 s per 17-time batch).
- Partial per-variable stores from a first attempt (`eccofs-avg-u-balanced-10mb`, `eccofs-avg-v-balanced-10mb`) were deleted from OSN. Other `eccofs-avg*` prefixes (`-smoke*`, `-nosub-smoke`, `eccofs-avg`) are not ours from this session and were left alone.
- Gotcha: don't use `pkill -f rechunk_temp.py` from the Bash tool, since it matches and kills the tool's own shell. Launch long runs with `setsid nohup ... &`.

### Next
- Timing notebook for u,v (like `eccofs_temp_chunking_comparison.ipynb`) comparing the original virtual store with the two new ones.
- Commit the `--vars` change to `rechunk_temp.py`.

## Possible next steps
- Repeat the comparison from an in-region machine, or with a dask cluster, since the timings above are network-bound.
- Try other chunk shapes, for example an intermediate layout with a small time span and a few levels, or the time-long ratio with a larger tolerance.
- Test with compression/sharding settings, and with larger tests (more points, more times) for stable benchmarks.
- Add the stores to the shared README with the results table.
