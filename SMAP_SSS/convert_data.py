#!/usr/bin/env python3
"""
Fetch BOTH SMAP RSS L3 SSS V6 datasets (8-day & monthly) by querying CMR,
download each granule via HTTPS with an Earthdata *token*, extract the value at
(lat=20.997, lon=37.252) to CSV on the fly, then delete each .nc to conserve disk.

Usage:
  export EDL_TOKEN="YOUR_LONG_EARTHDATA_TOKEN"
  python convert_data.py

Optional env:
  SMAP_LAT / SMAP_LON (floats)             default 20.997 / 37.252
  START_DATE / END_DATE (YYYY-MM-DD)        optional date filter
"""

import os, sys, time, json, re
from datetime import datetime
from urllib.parse import urlparse
import requests

# ---------------- CONFIG ----------------
EDL_TOKEN = os.environ.get("EDL_TOKEN", "").strip()
LAT = float(os.environ.get("SMAP_LAT", 20.875))
LON = float(os.environ.get("SMAP_LON", 37.625))
START_DATE = os.environ.get("START_DATE")  # e.g. "2015-05-25"
END_DATE   = os.environ.get("END_DATE")    # e.g. "2025-10-02"

# CMR collection IDs (stable) for RSS V6:
COLLS = {
    # 8-day running mean V6
    "8day": {
        "concept_id": "C2832227567-POCLOUD",
        "out_dir": "./data_8day",
        "csv": "SMAP_RSS_L3_SSS_8DAY_V6_37.625E_20.875N.csv",
    },
    # monthly V6
    "monthly": {
        "concept_id": "C2832226365-POCLOUD",
        "out_dir": "./data_monthly",
        "csv": "SMAP_RSS_L3_SSS_MONTHLY_V6_37.625E_20.875N.csv",
    },
}
CMR = "https://cmr.earthdata.nasa.gov/search"
# ---------------------------------------

def require_token():
    if not EDL_TOKEN:
        sys.stderr.write("ERROR: EDL_TOKEN not set. Run: export EDL_TOKEN=\"<token>\"\n")
        sys.exit(2)

def build_session():
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {EDL_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "smap-cmr-fetch/1.0",
    })
    s.max_redirects = 5
    return s

def parse_dt8(url: str):
    """Extract YYYYMMDD from filename if present."""
    m = re.search(r"(\d{8})", url)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d")
    except Exception:
        return None

def in_range(url: str, start_iso: str, end_iso: str):
    if not (start_iso or end_iso):
        return True
    dt = parse_dt8(url)
    if dt is None:
        return True
    if start_iso and dt < datetime.strptime(start_iso, "%Y-%m-%d"):
        return False
    if end_iso and dt > datetime.strptime(end_iso, "%Y-%m-%d"):
        return False
    return True

def cmr_granule_urls(sess: requests.Session, collection_concept_id: str):
    """Yield HTTPS .nc URLs for all granules in a collection (CMR JSON + pagination)."""
    page_size = 2000
    page_num = 1
    while True:
        params = {
            "collection_concept_id": collection_concept_id,
            "page_size": page_size,
            "page_num": page_num,
        }
        r = sess.get(f"{CMR}/granules.json", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        items = data.get("feed", {}).get("entry", [])
        if not items:
            break
        for e in items:
            # links array includes multiple rels; we need the archive HTTPS .nc
            for link in e.get("links", []):
                href = link.get("href", "")
                if href.startswith("https://") and href.endswith(".nc"):
                    yield href
        page_num += 1

def download(sess, url, dest_dir):
    os.makedirs(dest_dir, exist_ok=True)
    name = os.path.basename(urlparse(url).path)
    out = os.path.join(dest_dir, name)
    if os.path.exists(out) and os.path.getsize(out) > 0:
        return out, "skip"
    with sess.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=1<<20):
                if chunk:
                    f.write(chunk)
    return out, "ok"

def find_sss_var(ds):
    for v in ds.data_vars:
        if "sss" in v.lower():
            return v
    raise KeyError(f"No SSS-like variable in dataset. vars={list(ds.data_vars)}")

def find_coord_names(ds):
    lat_name = next((c for c in ds.coords if c.lower() in ("lat","latitude")), None)
    lon_name = next((c for c in ds.coords if c.lower() in ("lon","longitude")), None)
    # sometimes lat/lon are variables not coords
    if lat_name is None:
        lat_name = next((v for v in ds.variables if v.lower() in ("lat","latitude")), None)
    if lon_name is None:
        lon_name = next((v for v in ds.variables if v.lower() in ("lon","longitude")), None)
    if lat_name is None or lon_name is None:
        raise KeyError(f"Couldn't find lat/lon names in coords/vars: {list(ds.coords)} / {list(ds.variables)}")
    return lat_name, lon_name

def extract_time_from_ds_or_name(ds, nc_path):
    import pandas as pd, re
    # 1) explicit time coord present
    if "time" in ds.coords:
        t = pd.to_datetime(ds["time"].values)
        # return scalar Timestamp or array
        if getattr(t, "shape", ()) == () or getattr(t, "size", 1) == 1:
            return pd.to_datetime(t).item()
        return pd.to_datetime(t)
    # 2) time variable present
    if "time" in ds.variables:
        t = pd.to_datetime(ds["time"].values)
        if getattr(t, "shape", ()) == () or getattr(t, "size", 1) == 1:
            return pd.to_datetime(t).item()
        return pd.to_datetime(t)
    # 3) fallback: parse YYYYMMDD from filename
    m = re.search(r"(\d{8})", os.path.basename(nc_path))
    if m:
        return pd.to_datetime(m.group(1), format="%Y%m%d")
    # 4) last resort: now()
    return pd.Timestamp.utcnow()

def append_point_and_delete(nc_path, lat, lon, csv_path):
    try:
        import xarray as xr, pandas as pd
    except Exception:
        raise ImportError("Install deps: conda install -c conda-forge xarray netCDF4 pandas")
    try:
        ds = xr.open_dataset(nc_path, engine="netcdf4")
        var = find_sss_var(ds)
        lat_name, lon_name = find_coord_names(ds)

        # select nearest grid point
        da = ds[var].sel({lat_name: float(lat), lon_name: float(lon)}, method="nearest")

        # robust time
        t = extract_time_from_ds_or_name(ds, nc_path)

        # If selection is a scalar (no dims), build a one-row DataFrame
        if da.ndim == 0 or da.size == 1:
            # try to fetch the actual grid coords used after "nearest"
            lat_used = float(da.coords.get(lat_name, lat))
            lon_used = float(da.coords.get(lon_name, lon))
            row = {
                "time": t if not isinstance(t, (list, pd.Series, pd.Index)) else t[0],
                "lat": lat_used,
                "lon": lon_used,
                var: float(da.values),
            }
            df = pd.DataFrame([row])
        else:
            # has a time (or other) dimension → safe to to_dataframe()
            df = da.to_dataframe().reset_index()
            # normalize column names
            keep = [c for c in ("time", lat_name, lon_name, var) if c in df.columns]
            df = df[keep].rename(columns={lat_name: "lat", lon_name: "lon"})
            # if time missing as coord, add extracted
            if "time" not in df.columns:
                df["time"] = t

        header = not os.path.exists(csv_path)
        df.to_csv(csv_path, mode="a", header=header, index=False)

    finally:
        try: ds.close()
        except Exception: pass
        try: os.remove(nc_path)
        except Exception: pass

def quick_csv_has_date(csv_path, dt: datetime):
    if not os.path.exists(csv_path) or dt is None:
        return False
    s = dt.strftime("%Y-%m-%d")
    try:
        with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
            return s in f.read()
    except Exception:
        return False

def process_collection(name: str, cfg: dict, sess: requests.Session):
    print(f"\n=== {name.upper()} ===")
    urls = list(cmr_granule_urls(sess, cfg["concept_id"]))
    # Filter HTTPS .nc to date range if requested
    urls = [u for u in urls if in_range(u, START_DATE, END_DATE)]
    print(f"Found {len(urls)} candidate granules.")

    dl_ok = dl_skip = appended = 0
    for i, u in enumerate(sorted(urls), 1):
        dt = parse_dt8(u)
        if dt and quick_csv_has_date(cfg["csv"], dt):
            dl_skip += 1
            if i % 50 == 0:
                print(f"  … {i}/{len(urls)} (skipping duplicates)")
            continue
        try:
            nc_path, status = download(sess, u, cfg["out_dir"])
            if status == "ok": dl_ok += 1
            else: dl_skip += 1
            append_point_and_delete(nc_path, LAT, LON, cfg["csv"])
            appended += 1
            if i % 25 == 0:
                print(f"  … {i}/{len(urls)} processed")
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else "?"
            sys.stderr.write(f"[HTTP {code}] {u}\n")
        except ImportError as e:
            sys.stderr.write(f"[CSV ERROR] {e}\n")
            sys.stderr.write("Install deps then re-run; downloads so far are kept.\n")
            sys.exit(3)
        except Exception as e:
            sys.stderr.write(f"[ERR] {u} :: {e}\n")

    print(f"{name}: {dl_ok} downloaded, {dl_skip} skipped; appended {appended} rows → {cfg['csv']}")

def main():
    require_token()
    sess = build_session()

    print(f"Point: lat={LAT}, lon={LON}")
    if START_DATE or END_DATE:
        print(f"Date filter: {START_DATE or 'min'} → {END_DATE or 'max'}")

    for name, cfg in COLLS.items():
        process_collection(name, cfg, sess)

    # finish timestamp
    import datetime as _dt
    print("Finished at", _dt.datetime.now().isoformat())

if __name__ == "__main__":
    main()
