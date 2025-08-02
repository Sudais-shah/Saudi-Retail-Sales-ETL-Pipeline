import pandas as pd
import numpy as np
import os
from src.utils.pandas_helper import handle_missing_values_pandas, generate_weekly_summaries_pandas
from src.loaders.config_loader import get_config

class DataWranglingWithPandas:
    # Initialize with input Pandas DataFrame
    def __init__(self, df: pd.DataFrame):
        self.df = df
    # ------------------------------------------------------------------------------------------------------
    # Cleans and standardizes the dataset by handling missing values, duplicates, types, and text formatting
    # ------------------------------------------------------------------------------------------------------
    def clean_and_preprocess(self):
        """Cleans and standardizes the dataset by handling missing values,
           duplicates, types, and text formatting."""
          
        print("Starting Data Cleaning and Preprocessing...")

        # Define columns to drop
        columns_to_drop = ['Invoice ID', 'Customer Type', 'Customer Name', 'Customer Gender',
                           'Employee Name', 'Product Name', 'Manager Name', 'Channel', 'Customer Satisfaction']

        # Drop unnecessary columns
        self.df = self.df.drop(columns=columns_to_drop, errors='ignore')
        print(f"Dropped columns: {columns_to_drop}")
        print(f"Current columns: {self.df.columns.tolist()}")

        # Handle missing values
        self.df = handle_missing_values_pandas(self.df)
        print(" Missing values handled")

        # drop duplicates
        initial_count = len(self.df)
        self.df = self.df.drop_duplicates()
        final_count = len(self.df)
        print(f" Duplicates removed: {initial_count - final_count} rows dropped")

        # Trim whitespace from City and Product Category
        self.df['City'] = self.df['City'].str.strip()
        self.df['Product Category'] = self.df['Product Category'].str.strip()
        print(" Whitespace trimmed in 'City' and 'Category'")

        # Convert City to lowercase
        self.df['City'] = self.df['City'].str.lower()
        self.df['Product Category'] = self.df['Product Category'].str.lower()
        print(" Converted text to lowercase for 'City' and 'Product Category'")

        # Remove special characters
        self.df['City'] = self.df['City'].str.replace(r'[^a-zA-Z0-9\- ]', '', regex=True)
        self.df['Product Category'] = self.df['Product Category'].str.replace(r'[^a-zA-Z0-9\- ]', '', regex=True)
        print(" Removed special characters from 'City' and 'Product Category'")

        # Display final DataFrame shape
        print(f" Final DataFrame shape: {self.df.shape}")
        print(" Data Cleaning and Preprocessing Complete!")

        # Return self for method chaining
        return self
    
    # ------------------------------------------------------------------
    # Validates required columns and data types, handles missing columns
    # ------------------------------------------------------------------
    def validate_schema(self):
        # Check for missing columns
        required_columns = ['Invoice Date', 'City', 'Product Category', 'Total Sales']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        if missing_columns:
            raise ValueError(f"The following required columns are missing: {', '.join(missing_columns)}")
        
        # validate data types by castinge
        required_columns = {'Invoice Date': 'string','City': 'string','Product Category': 'string','Total Sales': 'float'}
        for col, dtype in required_columns.items():
            if col not in self.df.columns:
                self.df[col] = np.nan
                self.df[col] = self.df[col].astype(dtype)
        
        # Return self for method chaining
        return self

    # --------------------------------------------------------------------------------------------------------------
    # Ensures date consistency and continuity by checking for missing weeks and sorting the DataFrame chronologically
    # ---------------------------------------------------------------------------------------------------------------
    def ensure_date_continuity(self):
        # Ensure Invoice Date is datetime
        self.df['Invoice Date'] = pd.to_datetime(self.df['Invoice Date'])

        # Find the min and max dates in the DataFrame
        min_date = self.df['Invoice Date'].min()
        max_date = self.df['Invoice Date'].max()

        # Create full weekly date range
        full_date_range = pd.date_range(start=min_date, end=max_date, freq='W-MON')

        # Create DataFrame with all weeks
        all_weeks = pd.DataFrame({'Invoice Date': full_date_range})

        # Left join with the original DataFrame to find missing weeks and sort by Invoice Date
        self.df = pd.merge(all_weeks, self.df, on='Invoice Date', how='left')
        self.df = self.df.sort_values('Invoice Date')

        # Identify missing weeks and log them if any
        missing_weeks = self.df[self.df['City'].isnull()]['Invoice Date']
        if not missing_weeks.empty:
            print("Missing Weeks Detected:")
            print(missing_weeks)

        # Return self for method chaining
        return self
    
    # -------------------------------------------------
    # Detects and corrects outliers in numerical fields
    # -------------------------------------------------
    def detect_and_handle_outliers(self):
        # Identify numeric columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            # # Calculate quantiles
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)

            # Compute interquartile range
            IQR = Q3 - Q1

            # Define lower bound and upper bond for outliers
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            print(f"Detecting outliers for column '{col}':")
            print(f"   Lower Bound: {lower_bound}, Upper Bound: {upper_bound}")

            # Identify outliers
            outliers = self.df[(self.df[col] < lower_bound) | (self.df[col] > upper_bound)]
            print(f"   Outliers detected: {len(outliers)}")
            # Cap outliers to bounds
            self.df[col] = np.clip(self.df[col], lower_bound, upper_bound)
            print(f" Outliers handled for column '{col}'.")

        # Return self for method chaining
        return self

    # -----------------------------------------------------------
    # Aggregates data at the required granularity for forecasting
    # -----------------------------------------------------------
    def aggregate_for_forecasting(self):

        # Ensure Invoice Date is datetime and extract week start
        self.df['Invoice Date'] = pd.to_datetime(self.df['Invoice Date'])
        self.df['Week Start'] = self.df['Invoice Date'].dt.to_period('W').apply(lambda r: r.start_time)

        # Aggregate city-level and category-level weekly sales
        self.city_weekly_df = self.df.groupby(['City', 'Week Start'])['Total Sales'].sum().\
            reset_index().rename(columns={'Total Sales': 'City Weekly Sales'})
        self.category_weekly_df = self.df.groupby(['Product Category', 'Week Start'])['Total Sales'].sum().\
        reset_index().rename(columns={'Total Sales': 'Category Weekly Sales'})

        # Generate weekly summaries
        self.city_weekly_summaries, self.product_category_weekly_summaries = generate_weekly_summaries_pandas(self.df)

        # Display results to verify
        print(self.city_weekly_df.head(14))
        print(self.category_weekly_df.head(14))
        print(self.city_weekly_summaries.head(5))
        print(self.product_category_weekly_summaries.head(5))

        # Return self for method chaining
        return self
    
    # ------------------------------------------------
    # Performs final validation checks on cleaned data
    # ------------------------------------------------
    def run_quality_checks(self):
        print("Null Counts:")
        # Display null counts
        print(self.df.isnull().sum())
        
        print("Data Types:")
        # Display data types
        print(self.df.dtypes)\
        
        print("\nChecking for Negative or Zero Sales:")
        # Check for non-positive sales
        print(self.df[self.df['Total Sales'] <= 0])

        print("\nChecking for Duplicates:")
        # Count duplicate rows
        duplicates = self.df.duplicated().sum()
        print(f"Found {duplicates} duplicate rows.")

        print("\nDate Continuity Check:")
        # Verify date continuity
        print(self.df['Invoice Date'].sort_values().head(5))

        print("\nCity Names Check:")
        # List unique city names
        print(self.df['City'].unique())

        print("\nProduct Categories Check:")
        # List unique product categories
        print(self.df['Product Category'].unique())

        print("\nUnique City and Category Counts:")
        # Count unique cities
        print(f"Unique Cities: {self.df['City'].nunique()}")
        # Count unique categories
        print(f"Unique Product Categories: {self.df['Product Category'].nunique()}")

        print("\nSample Review:")
        # Display 10 random rows
        print(self.df.sample(10))
        print("\nData Quality Checks Completed.")

        # Return self for method chaining
        return self
  
    # -----------------------------------------------
    # Exports clean data and maintains version history
    # ------------------------------------------------
    def export_and_version_data(self):
        # Load configuration
        config = get_config()
        # Get processed data directory
        processed_dir = config['data']['processed_dir']

        # Create directory if it doesn't exist
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
            print(f"Created missing directory: {processed_dir}")
        else:
            print(f"Directory already exists: {processed_dir}")

#    ==============================================================================================================
        # Determine main data version
        main_version = len([f for f in os.listdir(processed_dir) if f.startswith('main_data_v')]) + 1
        # Create main data filename
        main_filename = f'main_data_v{main_version}.csv'
        # Define main data file path
        main_path = os.path.join(processed_dir, main_filename)
        # Export main DataFrame to CSV
        self.df.to_csv(main_path, index=False)
        print(f"Main Data saved as: {main_path}")

#    ==============================================================================================================
        # Determine city sales version
        city_version = len([f for f in os.listdir(processed_dir) if f.startswith('city_sales_v')]) + 1
        # Create city sales filename
        city_filename = f'city_sales_v{city_version}.csv'
        # Define city sales file path
        city_path = os.path.join(processed_dir, city_filename)
        # Export city weekly sales to CSV
        self.city_weekly_df.to_csv(city_path, index=False)
        print(f"City Weekly Sales saved as: {city_path}")

#    ==============================================================================================================
        # Determine category sales version
        category_version = len([f for f in os.listdir(processed_dir) if f.startswith('category_sales_v')]) + 1
        # Create category sales filename
        category_filename = f'category_sales_v{category_version}.csv'
        # Define category sales file path
        category_path = os.path.join(processed_dir, category_filename)
        # Export category weekly sales to CSV
        self.category_weekly_df.to_csv(category_path, index=False)
        print(f"Category Weekly Sales saved as: {category_path}")

        print("\nAll exports completed successfully.")
        print(f"\nYou can check the processed data here: {processed_dir}")

        # Return self for method chaining
        return self