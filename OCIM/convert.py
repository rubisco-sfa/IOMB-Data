"""."""
import os

import cftime
import numpy as np
import pandas as pd
import requests
import xarray as xr
from tqdm import tqdm


def download_file(remote_source, local_source="OCIM_Cant_update.zip"):
    """."""
    if not os.path.isfile(local_source):
        resp = requests.get(remote_source, stream=True)
        total_size = int(resp.headers.get("content-length"))
        with open(local_source, "wb") as fdl:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                desc=local_source,
                ascii=True,
            ) as pbar:
                for chunk in resp.iter_content(chunk_size=1024):
                    if chunk:
                        fdl.write(chunk)
                        pbar.update(len(chunk))


MODEL_VERSION = 0

# download, unzip, and gunzip the sources
if not os.path.isfile("OCIM_Cant_update.zip"):
    REMOTE_SOURCE = (
        "https://www.dropbox.com/sh/cpehutavhpvz8bt/AAC93cgO6Up2Jnshg6gkfR7Va?dl=1"
    )
    download_file(REMOTE_SOURCE)
if not (
    os.path.isfile("OCIM_anthco2_update2017.nc.gz")
    or os.path.isfile("OCIM_anthco2_update2017.nc")
):
    os.system("unzip OCIM_Cant_update.zip")
if not os.path.isfile("OCIM_anthco2_update2017.nc"):
    os.system("gunzip OCIM_anthco2_update2017.nc.gz")
os.system("rm -f *.mat *.m")
ds = xr.open_dataset("OCIM_anthco2_update2017.nc")

# create the pre-industrial gridded product
df = {}
for tar, src in zip(["lat", "lon", "depth"], ["LAT", "LON", "DEPTH"]):
    var = xr.where(ds["mask"], ds[src], np.nan).values.flatten()
    df[tar] = var[~np.isnan(var)]
df["dic0"] = ds["dicpre"].isel({"model version": MODEL_VERSION}).values.flatten()
df = pd.DataFrame(df)
dic0 = df.set_index(["depth", "lat", "lon"]).to_xarray()["dic0"]

# create the industrial gridded product
df = {}
for tar, src in zip(["lat", "lon", "depth"], ["LAT", "LON", "DEPTH"]):
    var = xr.where(ds["mask"], ds[src], np.nan).values.flatten()
    df[tar] = np.tile(var[~np.isnan(var)], ds["year"].size)
df["time"] = np.repeat(ds["year"], ds["ocean grid point"].size)
df["dic"] = ds["dicind"].isel({"model version": MODEL_VERSION}).values.flatten()
df = pd.DataFrame(df)
dic = df.set_index(["time", "depth", "lat", "lon"]).to_xarray()["dic"]

# merge into combined product
dset = xr.merge([dic0, dic])
dset["dic0"].attrs = {
    "long_name": "preindustrial dissolved inorganic carbon concentration",
    "units": "umol kg-1",
}
dset["dic"].attrs = {
    "long_name": "industrial dissolved inorganic carbon concentration",
    "units": "umol kg-1",
}
dset.to_netcdf("Cant.nc")
