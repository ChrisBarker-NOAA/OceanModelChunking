"""Add the ROMS grid + vertical-coordinate variables to an existing native ECCOFS Icechunk store on OSN.

Usage: add_grid.py {balanced,map,original} --vars {uv,temp} [--local DIR] [--nlev N]

Adds, in a single commit (existing data chunks are untouched):
  * lon_/lat_ for rho, u, v and psi
  * h, mask_rho/u/v/psi, angle, pm, pn, f
  * s_w, Cs_r, Cs_w           (s_rho is already present)
  * zeta (time-dependent; needed for depths), rechunked to match the layout, time axis aligned to the store
  * scalars Vtransform, Vstretching, theta_s, theta_b, Tcline, hc and the SGRID `grid` variable.
    These are not in the virtual store, so they are read from the header of one NOAA NetCDF4 file.
"""
import argparse
import configparser
import os
import sys

import dask
import icechunk
import numpy as np
import s3fs
import h5py
import xarray as xr
from icechunk.xarray import to_icechunk

from rechunk_vars import BUCKET, ENDPOINT, open_source

NOAA_FILE = "noaa-nos-eccofs-pds/avg/eccofs_avg_20260608_0001.nc"
SCALARS = ["Vtransform", "Vstretching", "theta_s", "theta_b", "Tcline", "hc", "grid"]
GRID_VARS = [f"{k}_{g}" for k in ("lon", "lat") for g in ("rho", "u", "v", "psi")] + [
    "h", "mask_rho", "mask_u", "mask_v", "mask_psi", "angle", "pm", "pn", "f", "s_w", "Cs_r", "Cs_w"]
# zeta chunks (time, eta_rho, xi_rho); balanced/map from dynamic_chunks ratios (1,1,1) tol 0.3 and a map-style tile
ZETA_CHUNKS = {"balanced": (28, 240, 278), "map": (9, 481, 556), "original": (1, 145, 167)}


def noaa_scalars():
    fs = s3fs.S3FileSystem(anon=True)
    fo = fs.open(NOAA_FILE, "rb", block_size=2**20, cache_type="blockcache", cache_options={"maxblocks": 256})
    out = {}
    with h5py.File(fo, "r") as f:
        for n in SCALARS:
            d = f[n]
            attrs = {k: (v.decode() if isinstance(v, bytes) else v) for k, v in d.attrs.items()
                     if k not in ("DIMENSION_LIST", "_FillValue", "_Netcdf4Dimid", "_Netcdf4Coordinates")}
            attrs = {k: (v.tolist() if isinstance(v, np.ndarray) else v) for k, v in attrs.items()}
            out[n] = xr.Variable((), d[()], attrs)
    return out


def storage(name, layout, local):
    if local:
        return icechunk.local_filesystem_storage(os.path.join(local, f"{name}-{layout}"))
    cp = configparser.ConfigParser()
    cp.read(os.path.expanduser("~/.aws/credentials"))
    p = cp["osn-esip"]
    return icechunk.s3_storage(
        bucket=BUCKET, prefix=f"rsignell/eccofs-avg-{name}-{layout}" + ("" if layout == "original" else "-10mb"),
        region="us-east-1", endpoint_url=ENDPOINT, force_path_style=True,
        access_key_id=p["aws_access_key_id"], secret_access_key=p["aws_secret_access_key"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("layout", choices=ZETA_CHUNKS)
    ap.add_argument("--vars", required=True, choices=["uv", "temp"])
    ap.add_argument("--local")
    ap.add_argument("--nlev", type=int, help="testing: the store only has this many top levels")
    a = ap.parse_args()

    repo = icechunk.Repository.open(storage(a.vars, a.layout, a.local))
    dest = xr.open_zarr(repo.readonly_session("main").store, consolidated=False)
    todo = [v for v in GRID_VARS + ["zeta"] + SCALARS if v not in dest.variables]
    print("already present:", [v for v in GRID_VARS + ["zeta"] + SCALARS if v in dest.variables])
    if not todo:
        return print("nothing to do")

    src = open_source()
    g = src[[v for v in GRID_VARS if v in todo]].reset_coords()
    if a.nlev:   # testing only: the small test store has just the top nlev levels
        g = g.isel(s_w=slice(-(a.nlev + 1), None), s_rho=slice(-a.nlev, None))
    if "zeta" in todo:
        z = src[["zeta"]].sel(time=dest.time)   # source has more times than the store; align by value
        z = z.chunk(dict(zip(("time", "eta_rho", "xi_rho"), ZETA_CHUNKS[a.layout])))
        g = g.merge(z.reset_coords(drop=True), compat="override")
    sc = noaa_scalars()
    for n in SCALARS:
        if n in todo:
            g[n] = sc[n]
    for v in g.variables:
        g[v].encoding.clear()
    g = g.drop_vars([v for v in g.variables if v in dest.variables])   # never rewrite what is already there
    print("adding:", {v: (g[v].dims, g[v].shape) for v in g.data_vars})

    with dask.config.set(scheduler="threads", num_workers=32):
        session = repo.writable_session("main")
        to_icechunk(g, session, mode="a")
        print("commit", session.commit(f"add ROMS grid and vertical coordinate variables ({a.layout})"))


if __name__ == "__main__":
    sys.exit(main())
