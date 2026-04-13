# 📊 PDS Leakage Detection System (Big Data Ingestion Project)

## 📌 Overview
This project focuses on analyzing India's Public Distribution System (PDS) to identify irregularities in food grain distribution.  
The system processes shop-level data and transforms it into meaningful insights through data preprocessing, exploratory analysis, and feature engineering.

---

## 🎯 Objective
- Clean and process raw PDS data  
- Analyze distribution patterns across shops and districts  
- Generate meaningful features for further analysis  

---

## 🧩 My Contribution
- Data Collection (Data.gov.in Tamil Nadu Fair Price Shop dataset)  
- Data Preprocessing  
- Exploratory Data Analysis (EDA)  
- Feature Engineering  

---

## ⚙️ Tech Stack
- Python  
- Pandas  
- NumPy  
- Matplotlib  
- Seaborn  

---

## 📁 Project Structure

pds-leakage-detection/
│
├── data/
│   ├── raw/
│   │   └── tn_pds_fairprice_shops_1.csv
│   └── processed/
│       └── pds_data.csv
│
├── src/
│   ├── preprocessing.py
│   ├── eda.py
│   ├── feature_engineering.py
│
├── main.py
├── requirements.txt
└── README.md

---

## 🔄 Workflow

### 1. Data Collection
- Loaded real Fair Price Shop (FPS) data

### 2. Data Preprocessing
- Cleaned and standardized dataset  
- Removed duplicates and missing values  
- Simulated allocation, distribution, and beneficiary data  

### 3. Exploratory Data Analysis (EDA)
- Distribution analysis  
- Shop-level and district-level insights  
- Correlation analysis  

### 4. Feature Engineering
- Utilization Ratio  
- Per Capita Distribution  
- Excess Distribution  
- Normalized Distribution  

---

## 📊 Output
- Cleaned and processed dataset  
- Visualizations for analysis  
- Engineered features for further modeling  

---

## ▶️ How to Run

1. Install dependencies:
pip install -r requirements.txt

2. Run the pipeline:
python main.py

---

## 🧠 Key Insights
- Most shops follow a normal distribution pattern  
- Some shops show higher distribution relative to allocation  
- District-level variations can be observed  

---

## 📌 Note
- Real FPS data was used for structure  
- Allocation and distribution values were simulated to create realistic analysis scenarios  

