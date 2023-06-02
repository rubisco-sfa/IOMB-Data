"""The GLODAP product is a collection of discrete ship pulls. To create a
merged dataset that can be more easily compared to model, we create a grid
consisting of a single representative year, uniform depths down to a maximum,
and a regularly spaced grid. For each month, depth, lat, and lon, we group all
data and then return a mean for that space/time cell. This results in a sparsely
populated grid that can be directly compared to models.
"""
import os
import time

import cftime
import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

# Setup target grid
GRID_RES = 2.0
DEPTH_MAX = 1000.0
lev = np.linspace(0, DEPTH_MAX, 21)
lat = np.linspace(-90, 90, int(round(180.0 / GRID_RES)) + 1)
lon = np.linspace(-180, 180, int(round(360.0 / GRID_RES)) + 1)

# Download reference data if not present
REMOTE_SOURCE = "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0257247/GLODAPv2.2022_Merged_Master_File.csv"
local_source = os.path.basename(REMOTE_SOURCE)
if not os.path.isfile(local_source):
    resp = requests.get(REMOTE_SOURCE, stream=True)
    total_size = int(resp.headers.get("content-length"))
    with open(local_source, "wb") as fdl:
        with tqdm(
            total=total_size, unit="B", unit_scale=True, desc=local_source, ascii=True
        ) as pbar:
            for chunk in resp.iter_content(chunk_size=1024):
                if chunk:
                    fdl.write(chunk)
                    pbar.update(len(chunk))

download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# Read in the dataset and remove rows/columns we aren't using to make the size
# more manageable.
df = pd.read_csv(
    os.path.basename(local_source), dtype={"G2expocode": str}, na_values=-9999
)
df = df.rename(columns={col: col.replace("G2", "") for col in df.columns})
df = df[df["depth"] < DEPTH_MAX]
df = df[
    [
        "month",
        "depth",
        "latitude",
        "longitude",
        "temperature",
        "salinity",
        "nitrate",
        "chla",
        "oxygen",
        "phosphate",
        "silicate",
        "talk",
        "tco2",
    ]
]

# Pandas magic to find means in the space/time cells. We could also compute a
# standard deviation to create uncertainty.
df = (
    df.groupby(
        [
            df["month"],
            pd.cut(df["depth"], lev),
            pd.cut(df["latitude"], lat),
            pd.cut(df["longitude"], lon),
        ]
    )
    .mean(numeric_only=True)
    .drop(columns=["depth", "latitude", "longitude"])
    .reset_index()
)

# The pandas cuts leaves these dimensions as intervals, we want the midpoint.
for col in ["depth", "latitude", "longitude"]:
    df[col] = df[col].apply(lambda x: x.mid)

# Convert to xarray and add bounds/metadata
df = df.set_index(["month", "depth", "latitude", "longitude"])
ds = df.to_xarray()
ds = ds.rename(
    {
        "month": "time",
        "latitude": "lat",
        "longitude": "lon",
        "temperature": "thetao",
        "salinity": "so",
        "nitrate": "no3",
        "oxygen": "o2",
        "phosphate": "po4",
        "silicate": "sio3",
        "tco2": "dissic",
    }
)
ds["thetao"].attrs["units"] = "degC"
ds["so"].attrs["units"] = "0.001"
ds["no3"].attrs["units"] = "umol kg-1"
ds["o2"].attrs["units"] = "umol kg-1"
ds["po4"].attrs["units"] = "umol kg-1"
ds["sio3"].attrs["units"] = "umol kg-1"
ds["chla"].attrs["units"] = "ug kg-1"
ds["talk"].attrs["units"] = "umol kg-1"
ds["dissic"].attrs["units"] = "umol kg-1"
ds["time_bnds"] = (
    ("time", "nb"),
    np.asarray(
        [
            [cftime.DatetimeNoLeap(2000, t, 1) for t in ds["time"]],
            [
                cftime.DatetimeNoLeap(2000 + (t == 12), 1 if t == 12 else t + 1, 1)
                for t in ds["time"]
            ],
        ]
    ).T,
)
ds["time"] = [cftime.DatetimeNoLeap(2000, t, 15) for t in ds["time"]]
ds["depth_bnds"] = (("depth", "nb"), np.asarray([lev[:-1], lev[1:]]).T)
ds["depth"].attrs["bounds"] = "depth_bnds"
ds["lat_bnds"] = (("lat", "nb"), np.asarray([lat[:-1], lat[1:]]).T)
ds["lat"].attrs["bounds"] = "lat_bnds"
ds["lon_bnds"] = (("lon", "nb"), np.asarray([lon[:-1], lon[1:]]).T)
ds["lon"].attrs["bounds"] = "lon_bnds"
ds.attrs = {
    "title": "Global Ocean Data Analysis Project: A uniformly calibrated open ocean data product on inorganic carbon and carbon-relevant variables",
    "version": "v2.2022",
    "institutions": "National Centers for Environmental Information",
    "source": REMOTE_SOURCE,
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/IOMB-Data/blob/main/GLODAP2022/convert.py",
    "references": """
@ARTICLE{,
    author = {Lauvset, S. K., Lange, N., Tanhua, T., Bittig, H. C., Olsen, A., Kozyr, A., Alin, S. R., Álvarez, M., Azetsu-Scott, K., Barbero, L., Becker, S., Brown, P.J., Carter, B. R., da Cunha, L. C., Feely, R. A., Hoppema, M., Humphreys, M. P., Ishii, M., Jeansson, E., Jiang, L. Q., Jones, S. D., Lo Monaco, C., Murata, A., Müller, J. D., Pérez, F. F., Pfeil, B., Schirnick, C., Steinfeldt, R., Suzuki, T., Tilbrook, B., Ulfsbo, A., Velo, A., Woosley, R. J., and Key, R. M.},
    title= {GLODAPv2.2022: the latest version of the global interior ocean biogeochemical data product},
    journal = {Earth Syst. Sci. Data Discuss.},
    year = {2022},
    doi = {10.5194/essd-2022-293}
}""",
}
ds.to_netcdf(
    "GLODAP2.2022.nc",
    encoding={
        "thetao": {"zlib": True},
        "so": {"zlib": True},
        "no3": {"zlib": True},
        "o2": {"zlib": True},
        "po4": {"zlib": True},
        "sio3": {"zlib": True},
        "chla": {"zlib": True},
        "talk": {"zlib": True},
        "dissic": {"zlib": True},
        "time": {"units": "days since 1850-01-01", "bounds": "time_bnds"},
        "time_bnds": {"units": "days since 1850-01-01"},
    },
)
