import urllib.request
# This script downloads monthly gridded salinity data from the IAP Oceanography website.

# Prefix URL for the salinity data files
prefix_url = "http://www.ocean.iap.ac.cn/ftp/cheng/CZ16_v0_IAP_Salinity_0p5_gridded_1month_netcdf/IAP_05_2000m_salinity_"
# Filename prefix for the downloaded files
prefix_filename = "salinity_data/"

for year in range(1960, 2024):
    for month in range(1, 13):
        month_str = f"{month:02d}"
        url = f"{prefix_url}year_{year}_month_{month_str}.nc"
        filename = f"{prefix_filename}IAP_Salinity_{year}_{month_str}.nc"
        urllib.request.urlretrieve(url, filename)