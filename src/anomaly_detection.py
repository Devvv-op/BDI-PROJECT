import pandas as pd

import numpy as np


#loading data
def load_data(path):
    return pd.read_csv(path)


#z score anomaly detection
def zscore_anomaly(df, column):
    mean = df[column].mean()
    std = df[column].std()

    df[f"{column}_zscore"] = (df[column] - mean) / std
    df[f"{column}_anomaly_zscore"] = df[f"{column}_zscore"].abs() > 2

    return df


#IQR Anomaly detection
def iqr_anomaly(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    df[f"{column}_anomaly_iqr"] = (df[column] < lower) | (df[column] > upper)

    return df


# risk score calc
def calculate_risk_score(df):
    df["risk_score_raw"] = (
        0.35 * df["excess_distribution"] +
        0.25 * df["utilization_ratio"] +
        0.20 * df["variance"] +
        0.20 * df["per_capita"]
    )

    # Normalize to 0–100
    df["risk_score"] = 100 * (
        (df["risk_score_raw"] - df["risk_score_raw"].min()) /
        (df["risk_score_raw"].max() - df["risk_score_raw"].min())
    )

    # Risk category
    df["risk_level"] = pd.cut(
        df["risk_score"],
        bins=[0, 40, 70, 100],
        labels=["Low", "Medium", "High"]
    )

    return df


#main pipeline
def run_anomaly_pipeline(input_path, output_path):
    df = load_data(input_path)

    print("Columns in dataset:", df.columns)

    # Apply anomaly detection on key columns
    df = zscore_anomaly(df, "distribution")
    df = iqr_anomaly(df, "distribution")

    df = zscore_anomaly(df, "excess_distribution")
    df = iqr_anomaly(df, "excess_distribution")

    # Risk scoring
    df = calculate_risk_score(df)

    # Save final output
    df.to_csv(output_path, index=False)

    print("Anomaly detection & risk scoring completed!")
    print("Output saved at:", output_path)


# -------------------------------
# RUN FILE
# -------------------------------
if __name__ == "__main__":
    run_anomaly_pipeline(
        "data/processed/pds_data.csv",
        "data/processed/final_output.csv"
    )