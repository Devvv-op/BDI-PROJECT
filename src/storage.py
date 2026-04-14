"""
storage.py — PDS Leakage Detection System
==========================================
Handles structured storage across three backends:
  1. MongoDB       — document store for flexible analytics queries
  2. SQLite        — relational store for structured/tabular queries
  3. HBase (CSV)   — wide-column simulation with composite row keys

Schema Design
─────────────
MongoDB Collections:
  • fps_shops       → shop metadata (shop_id, name, district)
  • transactions    → monthly supply records per shop
  • anomalies       → flagged records with method + risk level
  • district_summary→ aggregated district-level stats

SQLite Tables:
  • shops           → master shop registry
  • monthly_supply  → allocation/distribution per shop per month
  • anomaly_flags   → joined anomaly records with method labels
  • risk_scores     → computed risk metrics per shop

HBase (CSV simulation):
  • Row key         → shop_id#month
  • Column families → shop_info | supply_data | analytics | anomaly
"""

import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime

# ─── Optional MongoDB import ────────────────────────────────────────────────
try:
    from pymongo import MongoClient, ASCENDING
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False
    print("⚠️  pymongo not installed — MongoDB storage skipped.")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 1 — MONGODB STORAGE
# ════════════════════════════════════════════════════════════════════════════

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = "pds_leakage_db"


def get_mongo_client():
    """Return a connected MongoClient, or None if unavailable."""
    if not MONGO_AVAILABLE:
        return None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")   # fail fast if not running
        return client
    except Exception as e:
        print(f"⚠️  MongoDB unreachable: {e}")
        return None


# ── Schema helpers ────────────────────────────────────────────────────────

def _build_shop_doc(row) -> dict:
    """
    Schema: fps_shops
    ─────────────────
    {
      shop_id     : str   (unique, indexed)
      shop_name   : str
      district    : str   (indexed)
      inserted_at : ISO datetime
    }
    """
    return {
        "shop_id"     : str(row["shop_id"]),
        "shop_name"   : str(row["shop_name"]),
        "district"    : str(row["district"]),
        "inserted_at" : datetime.utcnow().isoformat(),
    }


def _build_transaction_doc(row) -> dict:
    """
    Schema: transactions
    ─────────────────────
    {
      shop_id               : str   (indexed)
      month                 : str   (indexed)
      allocation            : float
      distribution          : float
      beneficiaries         : int
      utilization_ratio     : float
      per_capita            : float
      excess_distribution   : float
      normalized_distribution: float
      variance              : float
      inserted_at           : ISO datetime
    }
    """
    return {
        "shop_id"                : str(row["shop_id"]),
        "month"                  : str(row["month"]),
        "allocation"             : float(row["allocation"]),
        "distribution"           : float(row["distribution"]),
        "beneficiaries"          : int(row["beneficiaries"]),
        "utilization_ratio"      : float(row.get("utilization_ratio", 0)),
        "per_capita"             : float(row.get("per_capita", 0)),
        "excess_distribution"    : float(row.get("excess_distribution", 0)),
        "normalized_distribution": float(row.get("normalized_distribution", 0)),
        "variance"               : float(row.get("variance", 0)),
        "inserted_at"            : datetime.utcnow().isoformat(),
    }


def _build_anomaly_doc(row) -> dict:
    """
    Schema: anomalies
    ──────────────────
    {
      shop_id         : str   (indexed)
      month           : str
      column          : str   — which column was flagged
      anomaly_type    : str   — "Z-score" | "IQR" | "Both" | "Normal"
      zscore_flag     : bool
      iqr_flag        : bool
      risk_score      : float
      risk_level      : str   — "Low" | "Medium" | "High"
      inserted_at     : ISO datetime
    }
    """
    return {
        "shop_id"      : str(row["shop_id"]),
        "month"        : str(row["month"]),
        "column"       : "distribution",
        "anomaly_type" : str(row.get("distribution_type", "Normal")),
        "zscore_flag"  : bool(row.get("distribution_anomaly_z", False)),
        "iqr_flag"     : bool(row.get("distribution_anomaly_iqr", False)),
        "risk_score"   : float(row.get("risk_score", 0)),
        "risk_level"   : str(row.get("risk_level", "Low")),
        "inserted_at"  : datetime.utcnow().isoformat(),
    }


def _build_district_summary(df) -> list[dict]:
    """
    Schema: district_summary
    ─────────────────────────
    Aggregated per district per month.
    {
      district           : str   (indexed)
      month              : str
      total_allocation   : float
      total_distribution : float
      total_beneficiaries: int
      avg_utilization    : float
      anomaly_count      : int
      high_risk_shops    : int
      computed_at        : ISO datetime
    }
    """
    grp = df.groupby(["district", "month"])
    docs = []
    for (district, month), g in grp:
        docs.append({
            "district"           : district,
            "month"              : month,
            "total_allocation"   : float(g["allocation"].sum()),
            "total_distribution" : float(g["distribution"].sum()),
            "total_beneficiaries": int(g["beneficiaries"].sum()),
            "avg_utilization"    : float(g.get("utilization_ratio", pd.Series([0])).mean()),
            "anomaly_count"      : int(g.get("distribution_anomaly_z", pd.Series([False])).sum()),
            "high_risk_shops"    : int((g.get("risk_level", pd.Series([])) == "High").sum())
                                    if "risk_level" in g.columns else 0,
            "computed_at"        : datetime.utcnow().isoformat(),
        })
    return docs


def store_to_mongodb(df: pd.DataFrame) -> None:
    """
    Persist all four collections to MongoDB.
    Drops and re-inserts on each run (full refresh strategy).
    """
    client = get_mongo_client()
    if client is None:
        print("⏭️  Skipping MongoDB — client not available.")
        return

    db = client[MONGO_DB]

    # ── fps_shops ────────────────────────────────────────────────────────
    shops_col = db["fps_shops"]
    shops_col.drop()
    shops_data = df[["shop_id", "shop_name", "district"]].drop_duplicates()
    shops_col.insert_many([_build_shop_doc(r) for _, r in shops_data.iterrows()])
    shops_col.create_index([("shop_id", ASCENDING)], unique=True)
    shops_col.create_index([("district", ASCENDING)])
    print(f"✅ [MongoDB] fps_shops       → {len(shops_data)} documents")

    # ── transactions ─────────────────────────────────────────────────────
    txn_col = db["transactions"]
    txn_col.drop()
    txn_col.insert_many([_build_transaction_doc(r) for _, r in df.iterrows()])
    txn_col.create_index([("shop_id", ASCENDING)])
    txn_col.create_index([("month",   ASCENDING)])
    txn_col.create_index([("shop_id", ASCENDING), ("month", ASCENDING)], unique=True)
    print(f"✅ [MongoDB] transactions    → {len(df)} documents")

    # ── anomalies ────────────────────────────────────────────────────────
    anomaly_cols_needed = {"distribution_type", "distribution_anomaly_z",
                           "distribution_anomaly_iqr", "risk_score", "risk_level"}
    if anomaly_cols_needed.issubset(df.columns):
        anom_col = db["anomalies"]
        anom_col.drop()
        flagged = df[df["distribution_type"] != "Normal"]
        anom_col.insert_many([_build_anomaly_doc(r) for _, r in flagged.iterrows()])
        anom_col.create_index([("shop_id",    ASCENDING)])
        anom_col.create_index([("risk_level", ASCENDING)])
        print(f"✅ [MongoDB] anomalies       → {len(flagged)} flagged records")
    else:
        print("⚠️  [MongoDB] anomalies skipped — run anomaly_detection first.")

    # ── district_summary ─────────────────────────────────────────────────
    dist_col = db["district_summary"]
    dist_col.drop()
    dist_docs = _build_district_summary(df)
    dist_col.insert_many(dist_docs)
    dist_col.create_index([("district", ASCENDING)])
    print(f"✅ [MongoDB] district_summary → {len(dist_docs)} documents")

    client.close()


# ════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SQLITE RELATIONAL STORAGE
# ════════════════════════════════════════════════════════════════════════════

SQLITE_PATH = "data/processed/pds_leakage.db"


def _get_sqlite_conn(db_path: str = SQLITE_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def create_sqlite_schema(conn: sqlite3.Connection) -> None:
    """
    DDL for all four SQLite tables.

    ┌─────────────────┐     ┌────────────────────────┐
    │    shops        │────<│    monthly_supply       │
    │─────────────────│     │────────────────────────│
    │ shop_id  PK     │     │ id          PK          │
    │ shop_name       │     │ shop_id     FK → shops  │
    │ district        │     │ month                   │
    └─────────────────┘     │ allocation              │
                            │ distribution            │
                            │ beneficiaries           │
                            │ utilization_ratio       │
                            │ per_capita              │
                            │ excess_distribution     │
                            │ normalized_distribution │
                            │ variance                │
                            └────────────────────────┘
                                       │
              ┌────────────────────────┼──────────────────────┐
              ▼                        ▼                      ▼
    ┌──────────────────┐    ┌──────────────────────┐  ┌──────────────────┐
    │  anomaly_flags   │    │    risk_scores       │  │ district_summary │
    │──────────────────│    │──────────────────────│  │──────────────────│
    │ id          PK   │    │ id           PK      │  │ id          PK   │
    │ shop_id     FK   │    │ shop_id      FK      │  │ district         │
    │ month            │    │ month                │  │ month            │
    │ column_name      │    │ risk_score_raw        │  │ total_allocation │
    │ anomaly_type     │    │ risk_score           │  │ total_distribution│
    │ zscore_flag      │    │ risk_level           │  │ avg_utilization  │
    │ iqr_flag         │    └──────────────────────┘  │ anomaly_count    │
    └──────────────────┘                              └──────────────────┘
    """
    cur = conn.cursor()

    cur.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;

        -- ── shops ────────────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS shops (
            shop_id     TEXT    PRIMARY KEY,
            shop_name   TEXT    NOT NULL,
            district    TEXT    NOT NULL,
            inserted_at TEXT    DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shops_district ON shops(district);

        -- ── monthly_supply ───────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS monthly_supply (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id                 TEXT    NOT NULL REFERENCES shops(shop_id),
            month                   TEXT    NOT NULL,
            allocation              REAL    NOT NULL CHECK (allocation > 0),
            distribution            REAL    NOT NULL CHECK (distribution >= 0),
            beneficiaries           INTEGER NOT NULL CHECK (beneficiaries > 0),
            utilization_ratio       REAL,
            per_capita              REAL,
            excess_distribution     REAL,
            normalized_distribution REAL,
            variance                REAL,
            inserted_at             TEXT    DEFAULT (datetime('now')),
            UNIQUE (shop_id, month)
        );
        CREATE INDEX IF NOT EXISTS idx_supply_shop  ON monthly_supply(shop_id);
        CREATE INDEX IF NOT EXISTS idx_supply_month ON monthly_supply(month);

        -- ── anomaly_flags ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS anomaly_flags (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id      TEXT    NOT NULL REFERENCES shops(shop_id),
            month        TEXT    NOT NULL,
            column_name  TEXT    NOT NULL,
            anomaly_type TEXT    NOT NULL
                         CHECK  (anomaly_type IN ('Normal','Z-score','IQR','Both')),
            zscore_flag  INTEGER NOT NULL DEFAULT 0,
            iqr_flag     INTEGER NOT NULL DEFAULT 0,
            inserted_at  TEXT    DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_anom_shop  ON anomaly_flags(shop_id);
        CREATE INDEX IF NOT EXISTS idx_anom_type  ON anomaly_flags(anomaly_type);

        -- ── risk_scores ──────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS risk_scores (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            shop_id        TEXT    NOT NULL REFERENCES shops(shop_id),
            month          TEXT    NOT NULL,
            risk_score_raw REAL,
            risk_score     REAL    CHECK (risk_score BETWEEN 0 AND 100),
            risk_level     TEXT    CHECK (risk_level IN ('Low','Medium','High')),
            inserted_at    TEXT    DEFAULT (datetime('now')),
            UNIQUE (shop_id, month)
        );
        CREATE INDEX IF NOT EXISTS idx_risk_level ON risk_scores(risk_level);

        -- ── district_summary ─────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS district_summary (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            district            TEXT    NOT NULL,
            month               TEXT    NOT NULL,
            total_allocation    REAL,
            total_distribution  REAL,
            total_beneficiaries INTEGER,
            avg_utilization     REAL,
            anomaly_count       INTEGER DEFAULT 0,
            high_risk_shops     INTEGER DEFAULT 0,
            computed_at         TEXT    DEFAULT (datetime('now')),
            UNIQUE (district, month)
        );
        CREATE INDEX IF NOT EXISTS idx_dist_district ON district_summary(district);
    """)
    conn.commit()
    print("✅ [SQLite] Schema created / verified.")


def store_to_sqlite(df: pd.DataFrame, db_path: str = SQLITE_PATH) -> None:
    """
    Insert all processed PDS data into the SQLite relational store.
    Uses INSERT OR REPLACE to support re-runs without duplicates.
    """
    conn = _get_sqlite_conn(db_path)
    create_sqlite_schema(conn)
    cur  = conn.cursor()

    # ── shops ─────────────────────────────────────────────────────────────
    shops = df[["shop_id", "shop_name", "district"]].drop_duplicates()
    cur.executemany(
        "INSERT OR REPLACE INTO shops (shop_id, shop_name, district) VALUES (?,?,?)",
        shops[["shop_id", "shop_name", "district"]].values.tolist()
    )
    print(f"✅ [SQLite] shops            → {len(shops)} rows")

    # ── monthly_supply ────────────────────────────────────────────────────
    supply_cols = ["shop_id", "month", "allocation", "distribution",
                   "beneficiaries", "utilization_ratio", "per_capita",
                   "excess_distribution", "normalized_distribution", "variance"]
    supply_df = df[[c for c in supply_cols if c in df.columns]]
    supply_df = supply_df.where(pd.notnull(supply_df), None)

    cur.executemany(
        """INSERT OR REPLACE INTO monthly_supply
           (shop_id, month, allocation, distribution, beneficiaries,
            utilization_ratio, per_capita, excess_distribution,
            normalized_distribution, variance)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        supply_df.values.tolist()
    )
    print(f"✅ [SQLite] monthly_supply   → {len(supply_df)} rows")

    # ── anomaly_flags ─────────────────────────────────────────────────────
    if "distribution_type" in df.columns:
        anom_df = df[["shop_id", "month",
                      "distribution_anomaly_z",
                      "distribution_anomaly_iqr",
                      "distribution_type"]].copy()
        anom_df.columns = ["shop_id", "month", "zscore_flag", "iqr_flag", "anomaly_type"]
        anom_df["column_name"] = "distribution"
        anom_df["zscore_flag"] = anom_df["zscore_flag"].astype(int)
        anom_df["iqr_flag"]    = anom_df["iqr_flag"].astype(int)

        cur.executemany(
            """INSERT INTO anomaly_flags
               (shop_id, month, column_name, anomaly_type, zscore_flag, iqr_flag)
               VALUES (?,?,?,?,?,?)""",
            anom_df[["shop_id","month","column_name",
                      "anomaly_type","zscore_flag","iqr_flag"]].values.tolist()
        )
        print(f"✅ [SQLite] anomaly_flags    → {len(anom_df)} rows")

    # ── risk_scores ───────────────────────────────────────────────────────
    if "risk_score" in df.columns:
        risk_df = df[["shop_id", "month",
                      "risk_score_raw", "risk_score", "risk_level"]].copy()
        risk_df["risk_level"] = risk_df["risk_level"].astype(str)

        cur.executemany(
            """INSERT OR REPLACE INTO risk_scores
               (shop_id, month, risk_score_raw, risk_score, risk_level)
               VALUES (?,?,?,?,?)""",
            risk_df.values.tolist()
        )
        print(f"✅ [SQLite] risk_scores      → {len(risk_df)} rows")

    # ── district_summary ──────────────────────────────────────────────────
    grp = df.groupby(["district", "month"])
    dist_rows = []
    for (district, month), g in grp:
        anomaly_count = int(g["distribution_anomaly_z"].sum()) \
                        if "distribution_anomaly_z" in g.columns else 0
        high_risk     = int((g["risk_level"] == "High").sum()) \
                        if "risk_level" in g.columns else 0
        dist_rows.append((
            district,
            month,
            float(g["allocation"].sum()),
            float(g["distribution"].sum()),
            int(g["beneficiaries"].sum()),
            float(g["utilization_ratio"].mean()) if "utilization_ratio" in g.columns else None,
            anomaly_count,
            high_risk,
        ))

    cur.executemany(
        """INSERT OR REPLACE INTO district_summary
           (district, month, total_allocation, total_distribution,
            total_beneficiaries, avg_utilization, anomaly_count, high_risk_shops)
           VALUES (?,?,?,?,?,?,?,?)""",
        dist_rows
    )
    print(f"✅ [SQLite] district_summary → {len(dist_rows)} rows")

    conn.commit()
    conn.close()
    print(f"💾 [SQLite] Database saved → {db_path}")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 3 — HBASE SIMULATION (CSV wide-column store)
# ════════════════════════════════════════════════════════════════════════════

HBASE_PATH = "data/processed/hbase_simulation.csv"

# Column-family mapping mirrors real HBase CF design:
#   CF:qualifier  →  pandas column name
CF_MAPPING = {
    # shop_info family
    "shop_info:district"                : "district",
    "shop_info:shop_name"               : "shop_name",
    # supply_data family
    "supply_data:month"                 : "month",
    "supply_data:allocation"            : "allocation",
    "supply_data:distribution"          : "distribution",
    "supply_data:beneficiaries"         : "beneficiaries",
    # analytics family
    "analytics:utilization_ratio"       : "utilization_ratio",
    "analytics:per_capita"              : "per_capita",
    "analytics:excess_distribution"     : "excess_distribution",
    "analytics:normalized_distribution" : "normalized_distribution",
    "analytics:variance"                : "variance",
    # anomaly family
    "anomaly:distribution_type"         : "distribution_type",
    "anomaly:zscore_flag"               : "distribution_anomaly_z",
    "anomaly:iqr_flag"                  : "distribution_anomaly_iqr",
    "anomaly:risk_score"                : "risk_score",
    "anomaly:risk_level"                : "risk_level",
}


def store_to_hbase_simulation(df: pd.DataFrame,
                               output_path: str = HBASE_PATH) -> None:
    """
    Simulates HBase wide-column storage as a CSV.

    Row key: shop_id#month  (composite, lexicographically sortable)
    Column families:
      • shop_info    — static shop metadata
      • supply_data  — raw allocation/distribution figures
      • analytics    — derived ratios and scores
      • anomaly      — detection flags and risk classification

    Only columns present in df are written; missing ones are silently skipped.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    hbase_df = df.copy()

    # Composite row key
    hbase_df["row_key"] = (
        hbase_df["shop_id"].astype(str) + "#" + hbase_df["month"].astype(str)
    )

    # Build rename map for columns that actually exist
    rename_map   = {v: k for k, v in CF_MAPPING.items() if v in hbase_df.columns}
    hbase_df     = hbase_df.rename(columns=rename_map)

    # Keep only row_key + CF columns
    cf_columns   = [c for c in hbase_df.columns if ":" in c]
    hbase_df     = hbase_df[["row_key"] + cf_columns].set_index("row_key")

    hbase_df.to_csv(output_path)
    print(f"✅ [HBase]  Simulation saved → {output_path}")
    print(f"   Row key  : shop_id#month  |  {len(hbase_df)} rows")
    print(f"   Families : shop_info | supply_data | analytics | anomaly")
    print(f"   Columns  : {len(cf_columns)} qualifiers")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 4 — UNIFIED PIPELINE ENTRY POINT
# ════════════════════════════════════════════════════════════════════════════

def store_all(df: pd.DataFrame,
              use_mongo : bool = True,
              use_sqlite: bool = True,
              use_hbase : bool = True) -> None:
    """
    Run all storage backends in sequence.

    Args:
        df         : Fully processed DataFrame (post feature engineering
                     and anomaly detection).
        use_mongo  : Toggle MongoDB storage.
        use_sqlite : Toggle SQLite storage.
        use_hbase  : Toggle HBase CSV simulation.
    """
    print("\n" + "═" * 60)
    print(" PDS LEAKAGE DETECTION — STORAGE PIPELINE")
    print("═" * 60)

    if use_sqlite:
        print("\n[1/3] SQLite Relational Store")
        print("─" * 40)
        store_to_sqlite(df)

    if use_mongo:
        print("\n[2/3] MongoDB Document Store")
        print("─" * 40)
        store_to_mongodb(df)

    if use_hbase:
        print("\n[3/3] HBase Wide-Column Simulation")
        print("─" * 40)
        store_to_hbase_simulation(df)

    print("\n" + "═" * 60)
    print(" ✅ All storage backends complete.")
    print("═" * 60 + "\n")


# ════════════════════════════════════════════════════════════════════════════
# SECTION 5 — QUERY UTILITIES (SQLite)
# ════════════════════════════════════════════════════════════════════════════

def query_high_risk_shops(db_path: str = SQLITE_PATH) -> pd.DataFrame:
    """Return all shops with risk_level = 'High'."""
    conn = _get_sqlite_conn(db_path)
    df   = pd.read_sql_query("""
        SELECT s.shop_id, s.shop_name, s.district,
               r.risk_score, r.risk_level, r.month
        FROM   risk_scores r
        JOIN   shops s ON s.shop_id = r.shop_id
        WHERE  r.risk_level = 'High'
        ORDER  BY r.risk_score DESC
    """, conn)
    conn.close()
    return df


def query_district_leakage_summary(db_path: str = SQLITE_PATH) -> pd.DataFrame:
    """Return district-level leakage summary sorted by anomaly count."""
    conn = _get_sqlite_conn(db_path)
    df   = pd.read_sql_query("""
        SELECT district, month,
               total_allocation, total_distribution,
               ROUND(total_distribution - total_allocation, 2) AS net_excess,
               anomaly_count, high_risk_shops,
               ROUND(avg_utilization, 3)                        AS avg_utilization
        FROM   district_summary
        ORDER  BY anomaly_count DESC, net_excess DESC
    """, conn)
    conn.close()
    return df


def query_anomaly_breakdown(db_path: str = SQLITE_PATH) -> pd.DataFrame:
    """Return count of anomalies grouped by type."""
    conn = _get_sqlite_conn(db_path)
    df   = pd.read_sql_query("""
        SELECT anomaly_type,
               COUNT(*) AS count,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
        FROM   anomaly_flags
        GROUP  BY anomaly_type
        ORDER  BY count DESC
    """, conn)
    conn.close()
    return df