"""Conversion script to create a OCIM product for use in IOMB.

Compute the column inventory change of Cant from 1994 to 2007:

Cant = int( dic - dic0, depth = [0,3000] )
dCant = Cant(2007) - Cant(1994)

"""
import os
import time

import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import xarray as xr
from tqdm import tqdm


def download_file(remote_source, local_source="OCIM_Cant_update.zip"):
    """Download the remote file and show a progress bar."""
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


# just select a model version, the internal variability across the model
# versions is small as compared to other variabilities. [Weiwei]
MODEL_VERSION = 0
REMOTE_SOURCE = (
    "https://www.dropbox.com/sh/cpehutavhpvz8bt/AAC93cgO6Up2Jnshg6gkfR7Va?dl=1"
)
LOCAL_SOURCE = "OCIM_anthco2_update2017.nc"

# download, unzip, and gunzip the sources
if not os.path.isfile("OCIM_Cant_update.zip"):
    download_file(REMOTE_SOURCE)
if not (
    os.path.isfile("OCIM_anthco2_update2017.nc.gz") or os.path.isfile(LOCAL_SOURCE)
):
    os.system("unzip OCIM_Cant_update.zip")
if not os.path.isfile(LOCAL_SOURCE):
    os.system("gunzip OCIM_anthco2_update2017.nc.gz")
os.system("rm -f *.mat *.m")
ds = xr.open_dataset(LOCAL_SOURCE)

# time stamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(LOCAL_SOURCE))
)
generate_stamp = time.strftime("%Y-%m-%d")

# create the pre-industrial gridded product [umol kg-1]
df = {}
for tar, src in zip(
    ["lat", "lon", "depth", "area", "volume"], ["LAT", "LON", "DEPTH", "Area", "VOL"]
):
    var = xr.where(ds["mask"], ds[src], np.nan).values.flatten()
    df[tar] = var[~np.isnan(var)]
df["dic0"] = ds["dicpre"].isel({"model version": MODEL_VERSION}).values.flatten()
df = pd.DataFrame(df)
dic0 = df.set_index(["depth", "lat", "lon"]).to_xarray()

# the depths coordinate is given at the cell center, but the processing scripts
# which come with the data compute the depth as a ration of volume to area.
area = dic0["area"].isel(depth=0)
volume = dic0["volume"]
thick = (volume / area).max(dim=["lat", "lon"])
dic0 = dic0["dic0"]

# create the industrial gridded product [umol kg-1]
df = {}
for tar, src in zip(["lat", "lon", "depth"], ["LAT", "LON", "DEPTH"]):
    var = xr.where(ds["mask"], ds[src], np.nan).values.flatten()
    df[tar] = np.tile(var[~np.isnan(var)], ds["year"].size)
df["time"] = np.repeat(ds["year"], ds["ocean grid point"].size)
df["dic"] = ds["dicind"].isel({"model version": MODEL_VERSION}).values.flatten()
df = pd.DataFrame(df)
dic = df.set_index(["time", "depth", "lat", "lon"]).to_xarray()["dic"]


def replicate_fig3b(dic, dic0):
    """Reproduce figure 3b from DeVries2014:

    https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2013GB004739

    """
    cant = dic - dic0
    cant = cant.sel(depth=slice(0, 3000))
    cant *= 1025.0  # [umol kg-1] --> [umol m-3]
    cant *= 1e-6  # [umol m-3] --> [mol m-3]
    cant *= 12.011  # [mol m-3] --> [g m-3]
    cant *= 1e-15  # [g m-3] --> [Pg m-3]
    cant = (cant * thick).sum(dim="depth")  # Pg m-2
    cant = (cant * area).sum(dim=["lat", "lon"])  # Pg
    fig, ax = plt.subplots(tight_layout=True)
    cant.plot(ax=ax)
    ax.set_ylabel("Cumulative Cant Storage [PgC]")
    ax.set_title("")
    fig.savefig("DeVries3b.png")
    plt.close()


def replicate_fig4(dic, dic0):
    """Reproduce the bottom-left subplots of figure 4 from DeVries2014:

    https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2013GB004739
    """
    cant = (dic - dic0).sel(time=1994)
    cant = cant.sel(depth=slice(0, 3000))
    cant *= 1025.0  # [umol kg-1] --> [umol m-3]
    cant *= 1e-6  # [umol m-3] --> [mol m-3]
    cant = (cant * thick).sum(dim="depth")  # [mol m-2]
    cant = xr.where(cant < 1e-14, np.nan, cant)
    proj = ccrs.PlateCarree(central_longitude=300)
    fig, ax = plt.subplots(subplot_kw={"projection": proj}, tight_layout=True)
    cm = plt.get_cmap("turbo", 12)
    cant.plot(ax=ax, vmin=0, vmax=120, cmap=cm, transform=ccrs.PlateCarree())
    ax.set_title("Cant storage 1994 [mol m-2]")
    fig.savefig("DeVries4.png")
    plt.close()


def produce_column_inventory_change_1994_2007(dic):
    """Produce a commensurate plot to figure 3A in Gruber2019:

    https://www.science.org/doi/10.1126/science.aau5153

    The plot represents column inventory change from 1994 to 2007. Because the
    reference time is 1994, there is no need to subtract the pre-industrial
    values.
    """
    cant = dic
    cant = cant.sel(time=2007) - cant.sel(time=1994)
    cant = cant.sel(depth=slice(0, 3000))
    cant *= 1025.0  # [umol kg-1] --> [umol m-3]
    cant *= 1e-6  # [umol m-3] --> [mol m-3]
    cant = (cant * thick).sum(dim="depth")  # [mol m-2]
    cant = xr.where(cant < 1e-14, np.nan, cant)
    proj = ccrs.PlateCarree(central_longitude=300)
    fig, ax = plt.subplots(subplot_kw={"projection": proj}, tight_layout=True)
    cm = plt.get_cmap("plasma", 16)
    cant.plot(ax=ax, vmin=0, vmax=16, cmap=cm, transform=ccrs.PlateCarree())
    ax.set_title("Cant storage 1994 [mol m-2]")
    fig.savefig("Gruber3a.png")
    plt.close()
    return cant


# tests to ensure we are processing data correctly
replicate_fig3b(dic, dic0)
replicate_fig4(dic, dic0)

# create the ILAMB-ready dataset
delta_cant = produce_column_inventory_change_1994_2007(dic)
delta_cant.attrs = {
    "long_name": "column inventory change in Cant from 1994 to 2007",
    "unit": "mol m-2",
}
dset = delta_cant.to_dataset(name="dcant")
dset.attrs = {
    "title": "Column inventory change in Cant from 1994 to 2007 using the ocean circulation inverse model (OCIM)",
    "version": "1",
    "institutions": "Department of Atmospheric and Oceanic Sciences, University of California",
    "source": REMOTE_SOURCE,
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/IOMB-Data/blob/main/OCIM/convert.py",
    "references": """
    @ARTICLE{DeVries,
    author = {Tim DeVries},
    title= {The oceanic anthropogenic CO2 sink: Storage, air-sea fluxes, and transports over the industrial era},
    journal = {Global Biogeochemical Cycles},
    year = {2014},
    doi = {10.1002/2013GB004739}
    }
    """,
}
dset.to_netcdf("OCIM.nc")
