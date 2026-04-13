import pandas as pd
import numpy as np


def preprocess_data(file_path):
    """
    Load and preprocess FPS dataset.
    Cleans data and simulates required fields.
    """

    # Load data
    df = pd.read_csv(file_path)

    # Clean column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Select relevant columns
    df = df[['district_name', 'shop_code', 'shop_name']]

    # Rename columns
    df.columns = ['district', 'shop_id', 'shop_name']

    # Remove duplicates
    df = df.drop_duplicates()

    # Handle missing values
    df = df.dropna()

    # Standardize district names
    df['district'] = df['district'].str.lower().str.strip()

    # -----------------------------
    # Data Validation
    # -----------------------------
    df = df[df['district'] != ""]

    # -----------------------------
    # Simulate realistic data
    # -----------------------------

    np.random.seed(42)  # for reproducibility

    # Allocation
    df['allocation'] = np.random.randint(800, 1500, size=len(df))

    # Distribution (with controlled variation)
    df['distribution'] = df['allocation'] * np.random.uniform(0.7, 1.5, size=len(df))

    # Beneficiaries
    df['beneficiaries'] = np.random.randint(150, 300, size=len(df))

    # Month column
    df['month'] = 'jan'

    # Ensure valid values
    df = df[df['allocation'] > 0]
    df = df[df['distribution'] >= 0]
    df = df[df['beneficiaries'] > 0]

    # Cap extreme values (outlier handling)
    df['distribution'] = df['distribution'].clip(
        upper=df['distribution'].quantile(0.99)
    )

    return df