# Saudi Retail Sales ETL Pipeline

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using PySpark to process and prepare Saudi retail sales data. It focuses on building a professional, scalable data processing workflow for retail analytics use cases.

# Project Overview
The ETL pipeline performs the following tasks:

- **Extracts** raw retail data from CSV format.
- **Transforms** the data by cleaning, wrangling, and applying feature engineering.
- **Loads** the processed data into a structured format for future analytics, forecasting, or dashboarding.

# Project Structure
retail-etl-project/
│
├── config/
│ └── config.yaml # Central configuration file
│
├── data/
│ ├── raw/ # Raw input data
│ └── processed/ # Output data after processing
│
├── notebooks/
│ ├── 01_data_wrangling.ipynb # Data cleaning and transformation
│ └── 02_feature_engineering.ipynb # Feature creation logic
│
├── src/
│ ├── extract.py # Functions to read raw data
│ ├── transform.py # Data wrangling and cleaning logic
│ ├── feature_engineering.py # Feature engineering methods
│ ├── utils.py # Utility functions
│
├── main.py # Main pipeline entry point
├── requirements.txt # Python dependencies
└── README.md # Project documentation

# Configuration

All paths and parameters (like file locations, Spark config, and logging settings) are stored in `config/config.yaml`. Update this file to change data source or processing behavior.

# How to Run

### 1. Install Requirements

bash
pip install -r requirements.txt
2. Run the ETL Pipeline
python main.py
Make sure the raw CSV file exists at the path defined in config.yaml under data.raw_dir.

# Notebooks
This project is modular. All functions in the ETL pipeline can also be tested step-by-step using:
## 01_data_wrangling.ipynb – for initial data cleaning
## 02_feature_engineering.ipynb – for creating new features

# Technologies Used
Python
PySpark
YAML (for configuration)
Jupyter Notebook
Git (version control)

#Use Cases
This pipeline sets the foundation for:
Forecasting future sales
Creating business dashboards
Customer behavior analytics
Inventory and supply chain insights

Author
Sudais Shah – Data Scientist
