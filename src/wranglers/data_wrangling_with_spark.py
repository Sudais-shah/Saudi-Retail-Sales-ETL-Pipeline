from turtle import pd
from src.utils.spark_helper import handle_missing_values_spark, generate_weekly_summaries
from pyspark.sql import functions as F
import os
from src.loaders.config_loader import get_config

# ===============================
#  Data Wrangling with Spark
# ===============================
class DataWranglingWithSpark:
    def __init__(self, df):
        self.df = df
    
   # ------------------------------------------------------------------------------------------------------
    # Cleans and standardizes the dataset by handling missing values, duplicates, types, and text formatting
    # ------------------------------------------------------------------------------------------------------
    def clean_and_preprocess(self):
        """Cleans and standardizes the dataset by handling missing values,
           duplicates, types, and text formatting."""
        
        print("Starting Data Cleaning and Preprocessing...")
        
        # 1 Drop unnecessary columns
        columns_to_drop = ['Invoice ID', 'Customer Type', 'Customer Name', 'Customer Gender',
                            'Employee Name', 'Product Name', 'Manager Name', 'Channel', 'Customer Satisfaction']
        self.df = self.df.drop(*columns_to_drop)
        print(f"Dropped columns: {columns_to_drop}")
        print(f"Current columns: {self.df.columns}")
        
        # 2 Handle missing values
        self.df = handle_missing_values_spark(self.df)  # Imported from spark_helper
        print(" Missing values handled")

        # 3 Drop duplicates
        initial_count = self.df.count()
        self.df = self.df.dropDuplicates()
        final_count = self.df.count()
        print(f" Duplicates removed: {initial_count - final_count} rows dropped")

        # 4 Trim Whitespace for text columns
        self.df = self.df.withColumn('City', F.trim(self.df['City']))
        self.df = self.df.withColumn('Product Category', F.trim(self.df['Product Category']))
        print(" Whitespace trimmed in 'City' and 'Category'")

        # 5 Convert text columns to lowercase for consistency
        self.df = self.df.withColumn('City', F.lower(self.df['City']))
        self.df = self.df.withColumn('Product Category', F.lower(self.df['Product Category']))
        print(" Converted text to lowercase for 'City' and 'Product Category'")

        # 6 Remove special characters (i will be keeping hyphens and spaces)
        self.df = self.df.withColumn('City', F.regexp_replace(self.df['City'], r'[^a-zA-Z0-9\- ]', ''))
        self.df = self.df.withColumn('Product Category', F.regexp_replace(self.df['Product Category'], r'[^a-zA-Z0-9\- ]', ''))
        print(" Removed special characters from 'City' and 'Product Category'")

        print(f" Final DataFrame schema:")
        self.df.printSchema()
        print(f"Total Rows: {self.df.count()} | Total Columns: {len(self.df.columns)}")
        print(" Data Cleaning and Preprocessing Complete!")
        
        return self # Return self for method chaining
    
    # ------------------------------------------------------------------
    # Validates required columns and data types, handles missing columns
    # ------------------------------------------------------------------
    def validate_schema(self):
        """Validates required columns and data types, handles missing columns."""
        required_columns = {'Invoice Date': 'string','City': 'string','Product Category': 'string','Total Sales': 'float'}
        missing_columns = [col for col in required_columns if col not in self.df.columns] # Check for missing columns
        if missing_columns:
            raise ValueError(f"The following required columns are missing: {', '.join(missing_columns)}")   
 
        # validate data types by casting
        for col, dtype in required_columns.items():  # If all columns exist, cast them to the right types
            self.df = self.df.withColumn(col, self.df[col].cast(dtype))

        return self # Return self for method chaining
    
    # --------------------------------------------------------------------------------------------------------------
    # Ensures date consistency and continuity by checking for missing weeks and sorting the DataFrame chronologically
    # ---------------------------------------------------------------------------------------------------------------
    def ensure_date_continuity(self):
        """Ensures date consistency and continuity by checking for missing weeks
           and sorting the DataFrame chronologically."""
        
         # Find the min and max dates in the DataFrame
        min_date, max_date = self.df.select(F.min("Invoice Date"), F.max("Invoice Date")).first()

        # Create a full date range      
        full_date_range = pd.date_range(start=min_date, end=max_date, freq='W-MON')
        date_df = self.df.sql_ctx.createDataFrame(pd.DataFrame({'Invoice Date': full_date_range}))

        # Left join with the original DataFrame to find missing weeks
        self.df = date_df.join(self.df, on='Invoice Date', how='left')
        self.df = self.df.orderBy(F.col("Invoice Date").asc())

        missing_weeks = self.df.filter(F.col("City").isNull()).select("Invoice Date")
        if missing_weeks.count() > 0:
            print("Missing Weeks Detected:")
            missing_weeks.show(truncate=False)   # Log missing weeks

        return self # Return self for method chaining

    # -------------------------------------------------
    # Detects and corrects outliers in numerical fields
    # -------------------------------------------------
    def detect_and_handle_outliers(self):
        """Detects and corrects outliers in numerical fields."""
    
        # Identify numeric columns only
        numeric_cols = [field.name for field in self.df.schema.fields 
                        if field.dataType.typeName() in ('double', 'float', 'int', 'long')]
        for col in numeric_cols:
            # Calculate quantiles
            quantiles = self.df.approxQuantile(col, [0.25, 0.75], 0.05)  # 0.05 is a tolerance level
            Q1, Q3 = quantiles[0], quantiles[1]
            # Calculate IQR
            IQR = Q3 - Q1
            # Define the bounds
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
        
            print(f"Detecting outliers for column '{col}':")
            print(f"   Lower Bound: {lower_bound}, Upper Bound: {upper_bound}")
        
            # Count the number of outliers
            outlier_count = self.df.filter((F.col(col) < lower_bound) | (F.col(col) > upper_bound)).count()
            print(f"   Outliers detected: {outlier_count}")
        
            # lastly Cap outliers to boundary values
            self.df = self.df.withColumn(col, F.when(F.col(col) < lower_bound, lower_bound).
                                         when(F.col(col) > upper_bound, upper_bound).otherwise(F.col(col)))
            print(f" Outliers handled for column '{col}'.")

        return self # Return self for method chaining



    # -----------------------------------------------------------
    # Aggregates data at the required granularity for forecasting
    # -----------------------------------------------------------
   
    def aggregate_for_forecasting(self):
        """
        Aggregates data at the required granularity for forecasting.
        - City-Level Weekly Sales
        - Product Category Weekly Sales
        """
        # Ensure Invoice Date is properly converted to date format
        self.df = self.df.withColumn('Invoice Date', F.to_date('Invoice Date', 'MM/dd/yyyy'))
        

        # Extract the week start date (Monday of the week) for each Invoice Date
        self.df = self.df.withColumn('Week Start', F.date_trunc('week', self.df['Invoice Date']))

        # 1 City-Level Weekly Sales
        self.city_weekly_df = (self.df.groupBy('City', 'Week Start').agg(F.sum('Total Sales').alias('City Weekly Sales'))
                .orderBy('Week Start'))

        # 2 Product Category Weekly Sales
        self.category_weekly_df = (self.df.groupBy('Product Category', 'Week Start')
                                   .agg(F.sum('Total Sales').alias('Category Weekly Sales')).orderBy('Week Start'))

         # Generate Weekly Summaries
        self.city_weekly_summaries, self.product_category_weekly_summaries = generate_weekly_summaries(self.df)# <-- Imported from spark_helper
        
        # Remove timestamp from Week Start
        self.city_weekly_df = self.city_weekly_df.withColumn('Week Start', F.to_date('Week Start'))
        self.category_weekly_df = self.category_weekly_df.withColumn('Week Start', F.to_date('Week Start'))
        self.city_weekly_summaries = self.city_weekly_summaries.withColumn('Week Start', F.to_date('Week Start'))
        self.product_category_weekly_summaries = self.product_category_weekly_summaries.withColumn('Week Start', F.to_date('Week Start'))

        # Show to verify
        self.city_weekly_df.show(14, truncate=False)
        self.category_weekly_df.show(14, truncate=False)
        self.city_weekly_summaries.show(5, truncate=False)
        self.product_category_weekly_summaries.show(5, truncate=False)

        return self # Return self for method chaining


    # ------------------------------------------------
    # Performs final validation checks on cleaned data
    # ------------------------------------------------
    def run_quality_checks(self):
        """Performs final validation checks on cleaned data."""
        
        # . Null Checks
        null_counts = self.df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in self.df.columns])
        print("Null Counts:")
        null_counts.show()

        # 2. Data Type Validation
        expected_types = {'Invoice Date': 'date', 'City': 'string', 'Product Category': 'string', 'Total Sales': 'float'}
        print("Data Types:")
        for col, dtype in expected_types.items():
            actual_dtype = self.df.schema[col].dataType
            print(f"{col}: Expected={dtype}, Actual={actual_dtype}")
        
        # 3. Range Validation for Total Sales
        print("\n Checking for Negative or Zero Sales:")
        self.df.filter(F.col('Total Sales') <= 0).show()

        # 4. Duplicate Rows Check
        print("\n Checking for Duplicates:")
        duplicate_count = self.df.groupBy(self.df.columns).count().filter(F.col("count") > 1)
        if duplicate_count.count() > 0:
            print(f"Found {duplicate_count.count()} duplicate rows.")
            duplicate_count.show()
        else:
            print("No duplicate rows found.")

        # 5. Date Continuity Check
        print("\n Date Continuity Check:")
        date_gaps = self.df.select("Invoice Date").orderBy("Invoice Date")
        date_gaps.show(5)  # Just a quick check for visualization
        
        # 6. Category and City Validation
        print("\n City Names Check:")
        self.df.select("City").distinct().show()

        print("\n Product Categories Check:")
        self.df.select("Product Category").distinct().show()

        # 7. Unique Counts
        print("\n Unique City and Category Counts:")
        print(f"Unique Cities: {self.df.select('City').distinct().count()}")
        print(f"Unique Product Categories: {self.df.select('Product Category').distinct().count()}")
        
        print("\n Sample Review:")
        sample_df = self.df.orderBy(F.rand()).limit(10)
        print(f"Displaying {10} random rows:")
        sample_df.show(truncate=False)
        
        print("\n Data Quality Checks Completed.")

        return self # Return self for method chaining

     # -----------------------------------------------
    # Exports clean data and maintains version history
    # ------------------------------------------------
    def export_and_version_data(self):
        """Exports clean data and maintains version history."""
        
        # Get the paths from the config
        config = get_config()
        processed_dir = config['data']['processed_dir']
        
        # Create the directory if it doesn't exist
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
            print(f"Created missing directory: {processed_dir}")
        else:
            print(f"Directory already exists: {processed_dir}")

        # Export Main Processed DataFrame
        print("\nExporting the main processed DataFrame...")
        main_version = len([f for f in os.listdir(processed_dir) if f.startswith('main_data_v')]) + 1
        main_filename = f'main_data_v{main_version}.csv'
        main_path = os.path.join(processed_dir, main_filename)
        self.df.toPandas().to_csv(main_path, index=False)
        print(f"Main Data saved as: {main_path}")

        #  Export City-Level Weekly Sales DataFrame
        print("\nExporting the city-level weekly sales DataFrame...")
        city_version = len([f for f in os.listdir(processed_dir) if f.startswith('city_sales_weekly_v')]) + 1
        city_filename = f'city_sales_weekly_v{city_version}.csv'
        city_path = os.path.join(processed_dir, city_filename)
        self.city_weekly_df.toPandas().to_csv(city_path, index=False)
        print(f"City Weekly Sales saved as: {city_path}")

        # Export Category-Level Weekly Sales DataFrame
        print("\nExporting the category-level weekly sales DataFrame...")
        category_version = len([f for f in os.listdir(processed_dir) if f.startswith('category_sales_weekly_v')]) + 1
        category_filename = f'category_sales_weekly_v{category_version}.csv'
        category_path = os.path.join(processed_dir, category_filename)
        self.category_weekly_df.toPandas().to_csv(category_path, index=False)
        print(f"Category Weekly Sales saved as: {category_path}")

        print("\nAll exports completed successfully.")
        print(f"\nYou can check the processed data here: {processed_dir}")



