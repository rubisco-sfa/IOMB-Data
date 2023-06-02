"""
wget -q --post-data="results_as_file=1&sensor_id=6&dtid=1123&sdate=1997-09-04 00:00:00&edate=2010-12-11 00:00:00&subType=1&prod_id=chlor_a&resolution_id=9km&period=MC" -O - https://oceandata.sci.gsfc.nasa.gov/api/file_search
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
    "SEASTAR_SEAWIFS_GAC.19980101_20100131.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980201_20100228.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980301_20100331.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980401_20100430.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980501_20100531.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980601_20100630.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980701_20100731.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19980801_20100831.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19971001_20101031.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19971101_20101130.L3m.MC.CHL.chlor_a.9km.nc",
    "SEASTAR_SEAWIFS_GAC.19971201_20101231.L3m.MC.CHL.chlor_a.9km.nc",
]
for source in remote_sources:
    download_file(f"{PREFIX}{source}?appkey={EARTHDATA_APPKEY}")
