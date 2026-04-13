from src.preprocessing import preprocess_data
from src.EDA import perform_eda
from src.feature_engineering import create_features


def main():
    print("🚀 Starting PDS Data Pipeline...\n")

    # Step 1: Preprocessing
    print("📥 Loading & preprocessing data...")
    df = preprocess_data("data/raw/tn_pds_fairprice_shops_1.csv")
    print("✅ Preprocessing complete\n")

    # Step 2: EDA
    print("📊 Performing EDA...")
    perform_eda(df)
    print("✅ EDA complete\n")

    # Step 3: Feature Engineering
    print("⚙️ Creating features...")
    df = create_features(df)
    print("✅ Feature engineering complete\n")

    # Step 4: Save processed data
    output_path = "data/processed/pds_data.csv"
    df.to_csv(output_path, index=False)

    print(f"\n💾 Data saved to: {output_path}")

    print("\n🔍 Final Dataset Preview:")
    print(df.head())


if __name__ == "__main__":
    main()