import pandas as pd
import xarray as xr

def load_dataset_1(path):
    df = pd.read_csv(path, header=None, names=["sst", "date", "col3", "col4"])
    df = df[["date", "sst"]]
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    return df[["month", "sst"]]

def load_dataset_2(path):
    df = pd.read_csv(path, usecols=[0, 1], names=["date", "d18O"], skiprows=1)
    df["date"] = pd.to_datetime(df["date"], format="%d-%b-%y", errors="coerce")
    df["d18O"] = pd.to_numeric(df["d18O"], errors="coerce")
    df.dropna(subset=["date", "d18O"], inplace=True)
    df["month"] = df["date"].dt.to_period("M")
    return df[["month", "d18O"]].groupby("month").mean().reset_index()

def load_dataset_3(path):
    df = pd.read_csv(path)
    df["Date_MSUD"] = pd.to_datetime(df["Date_MSUD"], errors="coerce", dayfirst=True)
    df["Date_SMII"] = pd.to_datetime(df["Date_SMII"], errors="coerce", dayfirst=True)
    msud = df[["Date_MSUD", "d18O_MSUD"]].rename(columns={"Date_MSUD": "date", "d18O_MSUD": "d18O_MSUD"})
    smii = df[["Date_SMII", "d18O_SMII"]].rename(columns={"Date_SMII": "date", "d18O_SMII": "d18O_SMII"})
    msud["month"] = msud["date"].dt.to_period("M")
    smii["month"] = smii["date"].dt.to_period("M")
    msud = msud[["month", "d18O_MSUD"]].groupby("month").mean().reset_index()
    smii = smii[["month", "d18O_SMII"]].groupby("month").mean().reset_index()
    return msud.merge(smii, on="month", how="outer")

def load_dataset_4(path):
    df = pd.read_csv(path, header=None, names=["time", "lat", "lon", "depth", "salinity"])
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d", errors="coerce")
    df["month"] = df["time"].dt.to_period("M")
    df = df[["month", "salinity"]].groupby("month").mean().reset_index()
    return df

def load_dataset_5(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["Time"], format="%Y-%m", errors="coerce").dt.to_period("M")
    df.rename(columns={"Salinity": "aux_salinity"}, inplace=True)
    return df[["month", "aux_salinity"]].groupby("month").mean().reset_index()

def load_dataset_6(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["time"], format="%m/%d/%Y", errors="coerce").dt.to_period("M")
    df.rename(columns={"supplementary_salinity": "sup_salinity"}, inplace=True)
    return df[["month", "sup_salinity"]].groupby("month").mean().reset_index()

def load_dataset_7(path):
    df = pd.read_csv(path)
    df["month"] = pd.to_datetime(df["time"], errors="coerce").dt.to_period("M")
    df.rename(columns={"comp_sss": "comp_salinity"}, inplace=True)
    return df[["month", "comp_salinity"]].groupby("month").mean().reset_index()

def unify_all(p1, p2, p3, p4, p5, p6, p7, output_path):
    df1 = load_dataset_1(p1)
    df2 = load_dataset_2(p2)
    df3 = load_dataset_3(p3)
    df4 = load_dataset_4(p4)
    df5 = load_dataset_5(p5)
    df6 = load_dataset_6(p6)
    df7 = load_dataset_7(p7)

    merged = df1.merge(df2, on="month", how="outer")
    merged = merged.merge(df3, on="month", how="outer")
    merged = merged.merge(df4, on="month", how="outer")
    merged = merged.merge(df5, on="month", how="outer")
    merged = merged.merge(df6, on="month", how="outer")
    merged = merged.merge(df7, on="month", how="outer")

    merged.sort_values("month", inplace=True)
    merged.to_csv(output_path, index=False)

if __name__ == "__main__":
    output_path = 'd18O_correlation/unified_datasets.csv'
    sst_path = 'd18O_correlation/sst_data.year.csv'
    coral_data_path = 'd18O_correlation/Coral_data.csv'
    coral_d18O_path = 'd18O_correlation/Coral_d18O_data.csv'
    sal_path = 'csv/IAP_Salinity_Dataset_37.5_21.0.csv'
    aux_sal_path = 'd18O_correlation/AuxiliarySalinityData.csv'
    sup_sal_path = 'd18O_correlation/SupplementarySalinityData.csv'
    comp_sal_path = 'd18O_correlation/ComplementarySalinityData.csv'
    unify_all(sst_path, coral_data_path, coral_d18O_path, sal_path, aux_sal_path, sup_sal_path, comp_sal_path, output_path)
