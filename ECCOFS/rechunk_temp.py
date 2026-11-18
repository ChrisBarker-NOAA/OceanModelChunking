"""Rechunk ECCOFS `avg` temp from the virtual best-estimate store into a native Icechunk store on OSN.

Usage: rechunk_temp.py {balanced,map} [--local DIR] [--ntime N] [--nlev N]

Credentials come from the `osn-esip` profile in ~/.aws/credentials.
Writes in time batches (one commit each) so an interrupted run resumes where it left off.
"""
import argparse
import configparser
import os
import sys
import time

import dask
import icechunk
import xarray as xr
import zarr
from icechunk.xarray import to_icechunk

zarr.config.set({"async.concurrency": 64})

ENDPOINT = "https://usgs.osn.mghpcc.org"
BUCKET = "esip"
SRC_PREFIX = "rsignell/eccofs-avg-best"
SOURCE = "s3://noaa-nos-eccofs-pds/"

# chosen with dynamic_chunks.algorithms.iterative_ratio_increase_algo, '10MB', tolerance 0.2
LAYOUTS = {
    # aspect ratio: all dims 1
    "balanced": dict(time=17, s_rho=5, eta_rho=144, xi_rho=167),
    # aspect ratio: time 4, s_rho unchunked, eta_rho 1, xi_rho 1
    "map": dict(time=3, s_rho=50, eta_rho=120, xi_rho=139),
}


def open_source():
    storage = icechunk.s3_storage(
        bucket=BUCKET, prefix=SRC_PREFIX, region="us-east-1", endpoint_url=ENDPOINT,
        anonymous=True, force_path_style=True,
    )
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(SOURCE, icechunk.s3_store(region="us-east-1", anonymous=True))
    )
    repo = icechunk.Repository.open(
        storage, config,
        authorize_virtual_chunk_access=icechunk.containers_credentials({SOURCE: icechunk.s3_anonymous_credentials()}),
    )
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False, chunks={})


def dest_storage(layout, local):
    if local:
        return icechunk.local_filesystem_storage(os.path.join(local, layout))
    cp = configparser.ConfigParser()
    cp.read(os.path.expanduser("~/.aws/credentials"))
    p = cp["osn-esip"]
    return icechunk.s3_storage(
        bucket=BUCKET, prefix=f"rsignell/eccofs-avg-temp-{layout}-10mb", region="us-east-1",
        endpoint_url=ENDPOINT, force_path_style=True,
        access_key_id=p["aws_access_key_id"], secret_access_key=p["aws_secret_access_key"],
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("layout", choices=LAYOUTS)
    ap.add_argument("--local")
    ap.add_argument("--ntime", type=int)
    ap.add_argument("--nlev", type=int)
    a = ap.parse_args()
    chunks = LAYOUTS[a.layout]

    src = open_source()[["temp"]]
    if a.ntime:
        src = src.isel(time=slice(0, a.ntime))
    if a.nlev:
        src = src.isel(s_rho=slice(-a.nlev, None))
    src = src.drop_vars([v for v in src.coords if v not in src.dims and v not in ("lat_rho", "lon_rho")])
    for v in src.data_vars:
        src[v].encoding.clear()
    for v in src.variables:
        src[v].encoding.pop("chunks", None)
    ds = src.chunk(chunks)
    print(a.layout, chunks, "->", dict(ds.temp.sizes))

    repo = icechunk.Repository.open_or_create(dest_storage(a.layout, a.local))
    nt, bt = ds.sizes["time"], chunks["time"]
    done = 0
    try:
        done = repo.readonly_session("main").store and xr.open_zarr(
            repo.readonly_session("main").store, consolidated=False).sizes["time"]
    except Exception:
        pass
    if done:
        print(f"resuming: {done}/{nt} times already written")

    # batches of 1 chunk-row along time; write explicitly aligned so no chunk is written twice
    with dask.config.set(scheduler="threads", num_workers=32):
        for t0 in range(done, nt, bt):
            batch = ds.isel(time=slice(t0, t0 + bt))
            session = repo.writable_session("main")
            t = time.time()
            if t0 == 0:
                to_icechunk(batch, session, mode="w")
            else:
                to_icechunk(batch.drop_vars(["lat_rho", "lon_rho", "s_rho"]), session, append_dim="time")
            session.commit(f"temp {a.layout}: times {t0}-{min(t0 + bt, nt)}")
            print(f"times {t0}-{min(t0 + bt, nt)} of {nt}: {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
