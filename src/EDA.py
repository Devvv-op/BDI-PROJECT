import matplotlib.pyplot as plt
import seaborn as sns


def perform_eda(df):
    """
    Perform Exploratory Data Analysis.
    """

    print("\n🔍 Dataset Info:")
    print(df.info())

    print("\n📊 Summary Statistics:")
    print(df.describe())

    # -----------------------------
    # Distribution Histogram
    # -----------------------------
    plt.figure()
    sns.histplot(df['distribution'], bins=20, kde=True)
    plt.title("Distribution of Food Supply")
    plt.xlabel("Distribution")
    plt.ylabel("Frequency")
    plt.show()

    # -----------------------------
    # Utilization Ratio Distribution
    # -----------------------------
    plt.figure()
    sns.histplot(df['distribution'] / df['allocation'], bins=20, kde=True)
    plt.title("Utilization Ratio Distribution")
    plt.show()

    # -----------------------------
    # Top 10 Shops
    # -----------------------------
    top_shops = df.groupby('shop_id')['distribution'].sum().nlargest(10)

    plt.figure()
    top_shops.plot(kind='bar')
    plt.title("Top 10 Shops by Distribution")
    plt.xlabel("Shop ID")
    plt.ylabel("Total Distribution")
    plt.show()

    # -----------------------------
    # District-wise Analysis
    # -----------------------------
    top_districts = df.groupby('district')['distribution'].sum().nlargest(10)

    plt.figure()
    top_districts.plot(kind='bar')
    plt.title("Top Districts by Distribution")
    plt.xlabel("District")
    plt.ylabel("Total Distribution")
    plt.show()

    # -----------------------------
    # Correlation Heatmap
    # -----------------------------
    plt.figure()
    sns.heatmap(
        df[['allocation', 'distribution', 'beneficiaries']].corr(),
        annot=True,
        cmap='coolwarm'
    )
    plt.title("Correlation Matrix")
    plt.show()