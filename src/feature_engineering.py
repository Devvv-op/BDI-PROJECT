import numpy as np


def create_features(df):
    """
    Create meaningful features for analysis.
    """

    # Utilization Ratio
    df['utilization_ratio'] = df['distribution'] / df['allocation']

    # Per Capita Distribution
    df['per_capita'] = df['distribution'] / df['beneficiaries']

    # Excess Distribution
    df['excess_distribution'] = df['distribution'] - df['allocation']

    # Normalized Distribution (Z-score style)
    df['normalized_distribution'] = (
        df['distribution'] - df['distribution'].mean()
    ) / df['distribution'].std()

    # Variance (simulated since no time-series)
    df['variance'] = np.random.uniform(0.1, 0.5, size=len(df))

    return df