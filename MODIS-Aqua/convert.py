"""This script is used to download the raw datafiles for the monthly climatology
of MODIS-Aqua chlorophyll and convert them into a format which is compliant with
the ILAMB system.

The listing of files can be obtained by running:

wget -q --post-data="results_as_file=1&sensor_id=7&dtid=1043&sdate=2002-07-04 00:00:00&edate=2023-06-02 17:31:54&subType=1&addurl=1&prod_id=chlor_a&resolution_id=9km&period=MC" -O - https://oceandata.sci.gsfc.nasa.gov/api/file_search

We chose the monthly file using the most data, and insert nan's for July where
there is no data.
"""
import os
import time

import cftime
import numpy as np
import requests
import xarray as xr
from ilamb3 import dataset as il
from tqdm import tqdm


def download_file(remote_source):
    """."""
    local_source = os.path.basename(remote_source)
    local_source = local_source.split("?")[0]
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


PREFIX = "https://oceandata.sci.gsfc.nasa.gov/cgi/getfile/"
EARTHDATA_APPKEY = "4040924662f94afc761280525e22c728d1b71e6c"
remote_sources = [
    "AQUA_MODIS.20030101_20230131.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20030201_20230228.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20030301_20230331.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20030401_20220430.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20030501_20220531.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20030601_20210630.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20020801_20220831.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20020901_20220930.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20021001_20221031.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20021101_20221130.L3m.MC.CHL.chlor_a.9km.nc",
    "AQUA_MODIS.20021201_20221231.L3m.MC.CHL.chlor_a.9km.nc",
]
for source in remote_sources:
    download_file(f"{PREFIX}{source}?appkey={EARTHDATA_APPKEY}")

for source in remote_sources:
    download_file(f"{PREFIX}{source}?appkey={EARTHDATA_APPKEY}")
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(remote_sources[0]))
)
generate_stamp = time.strftime("%Y-%m-%d")

# concatendate along a new 'time' axis, dealing with lack of Sept data
das = [xr.open_dataset(f)["chlor_a"] for f in remote_sources]
time = xr.DataArray(
    data=[cftime.DatetimeNoLeap(2000, m, 1) for m in range(1, 13)], dims="time"
)
das.insert(6, xr.ones_like(das[0]) * -9999)
da = xr.concat(das, dim=time)
da = xr.where(da != -9999, da, np.nan, keep_attrs=True)
da = da.expand_dims(dim={"depth": [0.0]}, axis=1)
ds = da.to_dataset()
ds = ds.pint.quantify()

# now coarsen to half degree resolution
ds = il.coarsen_dataset(ds)
ds = ds.drop("cell_measures")
ds = ds.pint.dequantify(format="cf")
ds = ds.rename({"chlor_a": "chl"})
ds = ds.transpose("time", "depth", "lat", "lon")

# write out attributes
ds["time_bnds"] = (
    ("time", "nb"),
    np.asarray(
        [
            [cftime.DatetimeNoLeap(2000, t, 1) for t in range(1, 13)],
            [
                cftime.DatetimeNoLeap(2000 + (t == 12), 1 if t == 12 else t + 1, 1)
                for t in range(1, 13)
            ],
        ]
    ).T,
)
ds["depth_bnds"] = (("depth", "nb"), np.asarray([[0.0, 10.0]]))
ds["depth"].attrs = dict(bounds="depth_bnds", units="m")
ds.attrs = {
    "title": "MODISA Level-3 Standard Mapped Image",
    "version": "R2022.0",
    "institutions": "NASA Goddard Space Flight Center, Ocean Ecology Laboratory, Ocean Biology Processing Group",
    "source": ", ".join(remote_sources),
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/IOMB-Data/blob/main/SeaWIFS/convert.py",
    "references": """
@ARTICLE{Hu2012,
    author = {Hu, C., Lee Z., and Franz, B.A.},
    title= {Chlorophyll-a algorithms for oligotrophic oceans: A novel approach based on three-band reflectance difference},
    journal = {J. Geophys. Res.},
    year = {2012},
    doi = {10.1029/2011JC007395}
}""",
}

ds.to_netcdf(
    "MODISAqua.nc",
    encoding={
        "chl": {"zlib": True},
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
