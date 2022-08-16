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
    """Given a string or list of strings representing remote files, download the
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


# From examining patterns in the links, we can build the lists of files we need programatically
PREFIX = "https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa"
noaa_name = {
    "o": "oxygen",
    "p": "phosphate",
    "i": "silicate",
    "t": "temperature",
    "s": "salinity",
    "n": "nitrate",
}
variable_name = {
    "o": "o2",
    "p": "po4",
    "i": "sio3",
    "t": "thetao",
    "s": "so",
    "n": "no3",
}

# Loop through each WOA variable, check for the presence of the data, and
# re-encode bring up to CF standards
if not os.path.isdir("_raw"):
    os.makedirs("_raw")
for var, name in noaa_name.items():
    remote = []
    TAG = "all" if var in ["n", "i", "o", "p"] else "95A4"
    for month in range(1, 13):
        # pylint: disable=expression-not-assigned
        remote.append(
            f"{PREFIX}/{name}/{TAG}/1.00/woa18_{TAG}_{var}%02d_01.nc" % (month)
        )
    local, download_time, generate_time = download_files(remote)

    # WOA files have encoded 'time' in a way that is not compatible with
    # xarray's read
    dset = [xr.open_dataset(ds, decode_times=False) for ds in local]
    dset = sorted(dset, key=lambda ds: ds["time"])
    dset = xr.concat(dset, dim="time")
    dset = dset.assign_coords(
        {"time": [cf.DatetimeNoLeap(2000, m, 15) for m in range(1, 13)]}
    )

    # inadvertently added a time dimension to bounds
    dset["lat_bnds"] = dset["lat_bnds"].isel({"time": 0})
    dset["lon_bnds"] = dset["lon_bnds"].isel({"time": 0})
    dset["depth_bnds"] = dset["depth_bnds"].isel({"time": 0})

    # add time bounds
    dset["time_bnds"] = xr.DataArray(
        [
            [
                cf.DatetimeNoLeap(2000, m + 1, 1),
                cf.DatetimeNoLeap(2000 + (m == 11), (m + 1) % 12 + 1, 1),
            ]
            for m in range(12)
        ],
        dims=["time", "nbounds"],
    )

    # Drop some of these variables and rename for output
    dset = dset.drop(
        ["crs", "climatology_bounds"]
        + [
            f"{var}_{postfix}"
            for postfix in [
                "dd",
                "mn",
                "sd",
                "se",
                "oa",
                "ma",
                "gp",
            ]
        ]
    )
    dset = dset.rename({f"{var}_an": variable_name[var]})

    # correct the units
    units = dset[variable_name[var]].attrs["units"]
    if units == "micromoles_per_kilogram":
        dset[variable_name[var]].attrs["units"] = "1e-6 kg-1"
    elif units == "degrees_celsius":
        dset[variable_name[var]].attrs["units"] = "degC"

    dset.attrs = {
        "title": "World Ocean Atlas",
        "version": "2018",
        "institutions": "NOAA National Centers for Environmental Information",
        "source": "[" + ", ".join(remote) + "]",
        "history": "merged into a single file, time and bounds rewritten, units made CF-compliant",
        "references": """
    @ARTICLE{Boyer,
    author = {Boyer, Tim P., Garcia, Hernan E., Locarnini, Ricardo A., Zweng, Melissa M., Mishonov, Alexey V., Reagan, James R., Weathers, Katharine A., Baranova, Olga K., Seidov, Dan, Smolyar, Igor V.},
    title= {World Ocean Atlas 2018},
    journal = {NOAA National Centers for Environmental Information},
    year = {2018},
    url = {https://www.ncei.noaa.gov/archive/accession/NCEI-WOA18}
    }
    """,
    }
    dset.to_netcdf(
        f"{variable_name[var]}.nc",
        encoding={
            "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
            "time_bnds": {"units": "days since 1850-01-01"},
        },
    )
