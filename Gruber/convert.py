"""."""
import os
import time

import requests
import xarray as xr
from tqdm import tqdm


def download_file(remote_source, local_source):
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


MODEL_VERSION = "DCANT_INV01"
REMOTE_SOURCE = "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0186034/inv_dcant_emlr_cstar_gruber_94-07_vs1.nc"
local_source = os.path.basename(REMOTE_SOURCE)

# download, unzip, and gunzip the sources
if not os.path.isfile(local_source):
    download_file(REMOTE_SOURCE, local_source)

# time stamps
download_stamp = time.strftime(
    "%Y-%m-%d", time.localtime(os.path.getmtime(local_source))
)
generate_stamp = time.strftime("%Y-%m-%d")

# encode for IOMB
dset = xr.open_dataset(local_source)
dset = dset.rename({"LATITUDE": "lat", "LONGITUDE": "lon"})
dset = dset[MODEL_VERSION].to_dataset(name="dcant")
dset["dcant"].attrs = {
    "long_name": "column inventory change in Cant from 1994 to 2007",
    "units": "mol m-2",
}
dset.attrs = {
    "title": "Vertically integrated changes in Cant between 1994 and 2007",
    "version": "1",
    "institutions": "Environmental Physics, Institute of Biogeochemistry and Pollutant Dynamics, ETH Zurich",
    "source": REMOTE_SOURCE,
    "history": f"Downloaded on {download_stamp} and generated netCDF file on {generate_stamp} with https://github.com/rubisco-sfa/IOMB-Data/blob/main/Gruber/convert.py",
    "references": """
    @ARTICLE{Gruber,
    author = {Nicolas Gruber, Dominic Clement, Brendan R. Carter, Richard A. Feely, Steven van Heuven, Mario Hoppema, Masao Ishii, Robert M. Key, Alex Kozyr, Siv K. Lauvset, Claire Lo Monaco, Jeremy T. Mathis, Akihiko Murata, Are Olsen, Fiz F. Perez, Christopher L. Sabine, Toste Tanhua, Rik Wanninkhof},
    title= {The oceanic sink for anthropogenic CO2 from 1994 to 2007},
    journal = {Science},
    year = {2019},
    doi = {10.1126/science.aau5153}
    }
    """,
}
dset.to_netcdf("Gruber.nc")
