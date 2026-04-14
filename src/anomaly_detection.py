import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# ---------------- LOAD ----------------
def load_data(path):
    return pd.read_csv(path)


# ---------------- Z-SCORE ----------------
def zscore_anomaly(df, column):
    mean = df[column].mean()
    std = df[column].std()

    df[f"{column}_z"] = (df[column] - mean) / std
    df[f"{column}_anomaly_z"] = df[f"{column}_z"].abs() > 2
    return df


# ---------------- IQR ----------------
def iqr_anomaly(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR

    df[f"{column}_anomaly_iqr"] = (df[column] < lower) | (df[column] > upper)
    return df


# ---------------- COMBINE LABEL ----------------
def combine_anomalies(df, column):
    def label(row):
        z = row[f"{column}_anomaly_z"]
        iqr = row[f"{column}_anomaly_iqr"]

        if z and iqr:
            return "Both"
        elif z:
            return "Z-score"
        elif iqr:
            return "IQR"
        else:
            return "Normal"

    df[f"{column}_type"] = df.apply(label, axis=1)
    return df


# ---------------- RISK SCORE ----------------
def calculate_risk_score(df):
    df["risk_score_raw"] = (
        0.35 * df["excess_distribution"] +
        0.25 * df["utilization_ratio"] +
        0.20 * df["variance"] +
        0.20 * df["per_capita"]
    )

    df["risk_score"] = 100 * (
        (df["risk_score_raw"] - df["risk_score_raw"].min()) /
        (df["risk_score_raw"].max() - df["risk_score_raw"].min())
    )

    df["risk_level"] = pd.cut(
        df["risk_score"],
        bins=[0, 40, 70, 100],
        labels=["Low", "Medium", "High"]
    )

    return df


# ---------------- PLOTS ----------------
def plot_all(df, column):

    # 1. Z-score histogram
    plt.figure(figsize=(10,4))
    sns.histplot(df[column], kde=True)
    anomalies = df[df[f"{column}_anomaly_z"]]
    plt.scatter(anomalies[column], [0]*len(anomalies), color='red')
    plt.title("Z-score Anomalies")
    plt.show()


    # 2. IQR boxplot
    plt.figure(figsize=(8,4))
    sns.boxplot(x=df[column])
    plt.title("IQR Boxplot")
    plt.show()


    # 3. Combined anomaly scatter
    plt.figure(figsize=(10,4))
    sns.scatterplot(x=df.index, y=df[column], hue=df[f"{column}_type"])
    plt.title("Combined Anomaly Detection")
    plt.show()


    # 4. Risk score distribution
    plt.figure(figsize=(10,4))
    sns.histplot(df, x="risk_score", hue="risk_level", multiple="stack")
    plt.title("Risk Score Distribution")
    plt.show()


# ---------------- PIPELINE ----------------
def run_pipeline(input_path):
    df = load_data(input_path)

    print("Columns:", df.columns)

    # anomaly detection
    df = zscore_anomaly(df, "distribution")
    df = iqr_anomaly(df, "distribution")
    df = combine_anomalies(df, "distribution")

    df = zscore_anomaly(df, "excess_distribution")
    df = iqr_anomaly(df, "excess_distribution")
    df = combine_anomalies(df, "excess_distribution")

    # risk scoring
    df = calculate_risk_score(df)

    # visuals
    plot_all(df, "distribution")

    return df


# ---------------- RUN ----------------
if __name__ == "__main__":
    df = run_pipeline("data/processed/pds_data.csv")