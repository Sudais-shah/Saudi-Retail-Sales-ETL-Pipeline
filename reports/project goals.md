# Project Name 
Saudi Retail Data ETL Pipeline

## Project Goals
This project aims to design a reusable, modular data pipeline for processing raw retail sales data. The primary goals of this pipeline are:

1 - Data Extraction
Automatically read raw daily sales data from folder-based sources.

2 - Data Exploration
Perform exploratory analysis to understand structure, missing values, anomalies, and schema irregularities.

3 - Data Wrangling (Cleaning & Structuring)
Use SPARQ (for experiment and learning) alongside Pandas for:
 Handling missing values
Fixing date/time formats
Removing duplicates
Standardizing column types and labels
Ensuring consistent structure across files

4 - Feature Engineering
Derive meaningful variables to enhance downstream usability such as:
Aggregated sales by store/category/day
Time-based features (like day, week, month names)
Lag or rolling-based sales summaries where appropriate
(Note: Features like holiday flags were excluded due to unavailability in the dataset.)

5 - Data Exportation
Deliver clean, well-structured datasets in multiple forms:
Cleaned daily data
Feature-engineered daily data
Weekly aggregated version
Monthly aggregated version

6 - Maintain Modular Code Design
Follow a folder-based modular code architecture (src/) to mimic professional, production-level pipeline development.

## Deliverables
Output File	Description:
cleaned_sales_daily.csv	Raw data cleaned and preprocessed (daily)
featured_sales_daily.csv	Cleaned + feature-engineered (daily)
cleaned_sales_weekly.csv	Weekly-level cleaned data
featured_sales_weekly.csv	Weekly-level featured data
cleaned_sales_monthly.csv	Monthly-level cleaned data
featured_sales_monthly.csv	Monthly-level featured data

## Project Scope
This project aims to design a reusable, modular data pipeline for processing raw retail sales data. The pipeline is built as a standalone ETL (Extract, Transform, Load) pipeline to feed downstream users like:

Data analysts
BI dashboard creators
Data scientists (for future modeling)
Business teams for decision-making

