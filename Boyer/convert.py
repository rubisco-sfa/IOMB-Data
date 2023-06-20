import os
import time

import cftime
import numpy as np
import requests
import xarray as xr
from tqdm import tqdm


def download_file(remote_source, local_source=None):
    """Download the remote file and show a progress bar."""
    if local_source is None:
        local_source = os.path.basename(remote_source)
    if os.path.isfile(local_source):
        return
    resp = requests.get(remote_source, stream=True, timeout=60)
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


# download raw data
remote_source = "https://cerweb.ifremer.fr/deboyer/data/mld_DT02_c1m_reg2.0.nc"
local_source = os.path.basename(remote_source)
download_file(remote_source)

# time stamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# read and apply masking
ds = xr.open_dataset(local_source)
ds = xr.where(np.isclose(ds["mask"], 1), ds, np.nan)
ds = ds["mld"].to_dataset(name="mlotstmax")

# fix time representation
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
ds["time"] = ds["time_bnds"].mean(dim="nb")

# fix attributes
ds["mlotstmax"].attrs = {"long_name": "mixed layer depth", "units": "m"}
ds.attrs = {
    "title": "Mixed layer depth with a fixed threshold criterion (0.2°C)",
    "version": "1",
    "institutions": "Ifremer, Univ. of Brest-UBO, Cnrs, Ird",
    "source": remote_source,
    "history": f"Downloaded on {download_stamp}, applied mask and fixed time/attributes on {generate_stamp} with https://github.com/rubisco-sfa/IOMB-Data/blob/main/Boyer/convert.py",
    "references": """
    @ARTICLE{Boyer,
        author = {de Boyer Montégut, C., G. Madec, A. S. Fischer, A. Lazar, and D. Iudicone},
        title= {Mixed layer depth over the global ocean: an examination of profile data and a profile-based climatology},
        journal = {J. Geophys. Res.},
        year = {2004},
        doi = {10.1029/2004JC002378}
    }""",
}
ds.to_netcdf(
    "mlotstmax.nc",
    encoding={
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
