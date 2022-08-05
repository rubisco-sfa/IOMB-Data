"""Download the WOA data and bring up to CF-standards for use in ILAMB.
"""
import os
import time
from typing import Tuple, Union

import cftime as cf
import requests
import xarray as xr


def download_files(
    remote_sources: Union[str, list[str]], output_path: str = "_raw"
) -> Tuple[Union[str, list], str, str]:
    """Given a string of list of strings representing remote files, download the
    files if they are not already present."""
    if isinstance(remote_sources, str):
        remote_sources = [remote_sources]
    local_sources = []
    for remote_source in remote_sources:
        local_source = os.path.join(output_path, os.path.basename(remote_source))
        local_sources.append(local_source)
        if not os.path.isfile(local_source):
            print(f"Downloading {remote_source}...")
            with requests.get(remote_source) as req:
                with open(local_source, "wb") as out:
                    out.write(req.content)
        download_stamp = time.strftime(
            "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
        )
    generate_stamp = time.strftime("%Y-%m-%d")
    if len(local_sources) == 1:
        local_sources = local_sources[0]
    return local_sources, download_stamp, generate_stamp


# pylint: disable=line-too-long
remote = [
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t01_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t02_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t03_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t04_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t05_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t06_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t07_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t08_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t09_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t10_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t11_01.nc",
    "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/temperature/95A4/1.00/woa18_95A4_t12_01.nc",
]
local, download_time, generate_time = download_files(remote)

# WOA files have encoded 'time' in a way that is not compatible with xarray's read
dset = [xr.open_dataset(ds, decode_times=False) for ds in local]
dset = sorted(dset, key=lambda ds: ds["time"])
dset = xr.concat(dset, dim="time")
dset = dset.assign_coords(
    {"time": [cf.DatetimeNoLeap(2000, m, 15) for m in range(1, 13)]}
)

# Encode correct climatology bounds. It would be great to have ILAMB understand
# how to parse these.
dset["climatology_bounds"] = xr.DataArray(
    [
        [cf.DatetimeNoLeap(1995, 1, 1), cf.DatetimeNoLeap(2005, 2, 1)],
        [cf.DatetimeNoLeap(1995, 2, 1), cf.DatetimeNoLeap(2005, 3, 1)],
        [cf.DatetimeNoLeap(1995, 3, 1), cf.DatetimeNoLeap(2005, 4, 1)],
        [cf.DatetimeNoLeap(1995, 4, 1), cf.DatetimeNoLeap(2005, 5, 1)],
        [cf.DatetimeNoLeap(1995, 5, 1), cf.DatetimeNoLeap(2005, 6, 1)],
        [cf.DatetimeNoLeap(1995, 6, 1), cf.DatetimeNoLeap(2005, 7, 1)],
        [cf.DatetimeNoLeap(1995, 7, 1), cf.DatetimeNoLeap(2005, 8, 1)],
        [cf.DatetimeNoLeap(1995, 8, 1), cf.DatetimeNoLeap(2005, 9, 1)],
        [cf.DatetimeNoLeap(1995, 9, 1), cf.DatetimeNoLeap(2005, 10, 1)],
        [cf.DatetimeNoLeap(1995, 10, 1), cf.DatetimeNoLeap(2005, 11, 1)],
        [cf.DatetimeNoLeap(1995, 11, 1), cf.DatetimeNoLeap(2005, 12, 1)],
        [cf.DatetimeNoLeap(1995, 12, 1), cf.DatetimeNoLeap(2005, 12, 31)],
    ],
    dims=["time", "nbounds"],
)

# Drop some of these variables and rename for output
dset = dset.drop(["t_dd", "t_mn", "t_sd", "t_se", "t_oa", "t_ma", "t_gp", "crs"])
dset = dset.rename({"t_an": "thetao"})
dset.attrs = {
    "title": "World Ocean Atlas",
    "version": "2018",
    "institutions": "NOAA National Centers for Environmental Information",
    "source": "[" + ", ".join(remote) + "]",
    "history": "merged into a single file, time and climatology bounds rewritten",
    "references": """
@ARTICLE{Boyer,
  author = {Boyer, Tim P.; Garcia, Hernan E.; Locarnini, Ricardo A.; Zweng, Melissa M.; Mishonov, Alexey V.; Reagan, James R.; Weathers, Katharine A.; Baranova, Olga K.; Seidov, Dan; Smolyar, Igor V.},
  title= {World Ocean Atlas 2018},
  journal = {NOAA National Centers for Environmental Information},
  year = {2018},
  doi = {https://www.ncei.noaa.gov/archive/accession/NCEI-WOA18}
}
""",
}
dset.to_netcdf("thetao.nc")
