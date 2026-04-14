import pandas as pd
from pymongo import MongoClient
import os

# ─────────────────────────────────────────────
# MongoDB Storage
# ─────────────────────────────────────────────

def get_mongo_client():
    client = MongoClient("mongodb://localhost:27017/")
    return client

def store_to_mongodb(df):
    """
    Store processed PDS data into MongoDB collections.
    Two collections:
      - fps_shops      : shop + district info
      - transactions   : distribution/allocation per shop per month
    """
    client = get_mongo_client()
    db = client["pds_database"]

    # --- Collection 1: FPS Shops ---
    shops_col = db["fps_shops"]
    shops_col.drop()  # fresh insert each run

    shops_data = df[['shop_id', 'shop_name', 'district']].drop_duplicates()
    shops_records = shops_data.to_dict(orient='records')
    shops_col.insert_many(shops_records)
    print(f"✅ Inserted {len(shops_records)} shops into MongoDB [fps_shops]")

    # --- Collection 2: Transactions ---
    txn_col = db["transactions"]
    txn_col.drop()

    txn_data = df[['shop_id', 'month', 'allocation', 'distribution',
                   'beneficiaries', 'utilization_ratio', 'per_capita',
                   'excess_distribution', 'normalized_distribution', 'variance']]
    txn_records = txn_data.to_dict(orient='records')
    txn_col.insert_many(txn_records)
    print(f"✅ Inserted {len(txn_records)} records into MongoDB [transactions]")

    # --- Indexes for fast querying ---
    shops_col.create_index("shop_id")
    txn_col.create_index("shop_id")
    txn_col.create_index("month")
    print("✅ Indexes created on shop_id and month")

    client.close()

# ─────────────────────────────────────────────
# HBase Simulation (CSV-based row-key store)
# ─────────────────────────────────────────────

def store_to_hbase_simulation(df):
    """
    Simulates HBase storage using CSV with a composite row key.
    Row key format: shop_id#month  (mimics HBase row key design)
    Saved to: data/processed/hbase_simulation.csv
    """
    hbase_df = df.copy()

    # Composite row key (HBase style)
    hbase_df['row_key'] = hbase_df['shop_id'].astype(str) + "#" + hbase_df['month'].astype(str)

    # Column families (HBase organizes data this way)
    # CF1: shop_info | CF2: supply_data | CF3: analytics
    hbase_df = hbase_df.rename(columns={
        'district'                : 'shop_info:district',
        'shop_name'               : 'shop_info:shop_name',
        'allocation'              : 'supply_data:allocation',
        'distribution'            : 'supply_data:distribution',
        'beneficiaries'           : 'supply_data:beneficiaries',
        'month'                   : 'supply_data:month',
        'utilization_ratio'       : 'analytics:utilization_ratio',
        'per_capita'              : 'analytics:per_capita',
        'excess_distribution'     : 'analytics:excess_distribution',
        'normalized_distribution' : 'analytics:normalized_distribution',
        'variance'                : 'analytics:variance',
    })

    # Set row_key as index (like HBase)
    hbase_df = hbase_df.set_index('row_key')

    output_path = "data/processed/hbase_simulation.csv"
    hbase_df.to_csv(output_path)
    print(f"✅ HBase simulation saved to: {output_path}")
    print(f"   Row key format: shop_id#month | {len(hbase_df)} rows")