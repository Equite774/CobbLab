import pandas as pd


df = pd.read_csv("d18O_correlation/2014_2024_coral_d18O_anomalies.csv")
df["Date_MSUD"] = pd.to_datetime(df["Date_MSUD"], errors="coerce", dayfirst=True)
msud = df[["Date_MSUD", "d18O_MSUD_anomaly"]].rename(columns={"Date_MSUD": "date", "d18O_MSUD_anomaly": "d18O_MSUD_anomaly"})
msud["month"] = msud["date"].dt.to_period("M")
msud = msud[["month", "d18O_MSUD_anomaly"]].groupby("month").mean().reset_index()

df["Date_SMII"] = pd.to_datetime(df["Date_SMII"], errors="coerce", dayfirst=True)
smii = df[["Date_SMII", "d18O_SMII_anomaly"]].rename(columns={"Date_SMII": "date", "d18O_SMII_anomaly": "d18O_SMII_anomaly"})
smii["month"] = smii["date"].dt.to_period("M")
smii = smii[["month", "d18O_SMII_anomaly"]].groupby("month").mean().reset_index()

df = pd.read_csv("d18O_correlation/2014-2024_sss_anomalies.csv")
df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
df.rename(columns={"SSS_anomaly": "sss_anomaly"}, inplace=True)
df["month"] = df["datetime"].dt.to_period("M")
sss = df[["month", "sss_anomaly"]].groupby("month").mean().reset_index()

merged = msud.merge(smii, on="month", how="outer")
merged = merged.merge(sss, on="month", how="outer")
merged.sort_values("month", inplace=True)
merged.to_csv("d18O_correlation/unified_anomaly_datasets.csv", index=False)