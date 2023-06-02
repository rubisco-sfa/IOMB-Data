"""
wget -q --post-data="results_as_file=1&sensor_id=7&dtid=1043&sdate=2002-07-04 00:00:00&edate=2023-06-02 17:31:54&subType=1&addurl=1&prod_id=chlor_a&resolution_id=9km&period=MC" -O - https://oceandata.sci.gsfc.nasa.gov/api/file_search
"""
import os

import requests
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
