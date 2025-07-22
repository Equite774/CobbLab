import pandas as pd
import xarray as xr

def load_dataset_sst(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["Time"], errors="coerce").dt.to_period("M")
    df.rename(columns={"sst": "sst"}, inplace=True)
    return df[["month", "sst"]].groupby("month").mean().reset_index()

def load_dataset_sss(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["time"], errors="coerce").dt.to_period("M")
    df.rename(columns={"sss": "sss"}, inplace=True)
    return df[["month", "sss"]].groupby("month").mean().reset_index()

def load_dataset_sos(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["time"], errors="coerce").dt.to_period("M")
    df.rename(columns={"sos": "sos"}, inplace=True)
    return df[["month", "sos"]].groupby("month").mean().reset_index()

def load_dataset_d18O(path):
    df = pd.read_csv(path)
    df["Date_MSUD"] = pd.to_datetime(df["Date_MSUD"], errors="coerce", dayfirst=True)
    msud = df[["Date_MSUD", "d18O_MSUD"]].rename(columns={"Date_MSUD": "date", "d18O_MSUD": "d18O"})
    msud["month"] = msud["date"].dt.to_period("M")
    msud = msud[["month", "d18O"]].groupby("month").mean().reset_index()
    return msud

def unify_all(p1, p2, p3, p4, output_path):
    sst = load_dataset_sst(p1)
    sss = load_dataset_sss(p2)
    sos = load_dataset_sos(p3)
    d18O = load_dataset_d18O(p4)
    merged = sst.merge(sss, on="month", how="outer")
    merged = merged.merge(sos, on="month", how="outer")
    merged = merged.merge(d18O, on="month", how="outer")

    merged.sort_values("month", inplace=True)
    merged.to_csv(output_path, index=False)

if __name__ == "__main__":
    output_path = 'd18O_correlation/unified_datasets.csv'
    sst_path = 'd18O_correlation/sst_final.csv'
    sss_path = 'd18O_correlation/sss_final.csv'
    sos_path = 'd18O_correlation/sos_final.csv'
    d18O_path = 'd18O_correlation/coral_d18O_data.csv'
    unify_all(sst_path, sss_path, sos_path, d18O_path, output_path)
