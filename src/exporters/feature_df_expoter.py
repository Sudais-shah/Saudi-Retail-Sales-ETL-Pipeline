import os
from src.loaders.config_loader import get_config

class FeatureExporter:
    def __init__(self, main_df=None, city_df=None, category_df=None):
        """
        Initializes the FeatureExporter with Pandas DataFrames.
        """
        self.main_df = main_df
        self.city_df = city_df
        self.category_df = category_df

    def export_and_version_features(self):
        """
        Exports all feature-engineered datasets to CSV with versioning.
        """
        # Get processed directory from config
        config = get_config()
        rename_dir = config['data']['processed_dir']

        # Create directory if it doesn't exist
        if not os.path.exists(rename_dir):
            os.makedirs(rename_dir)
            print(f"[INFO] Created directory: {rename_dir}")
        else:
            print(f"[INFO] Using existing directory: {rename_dir}")

        def save(df, prefix):
            """
            Saves a Pandas DataFrame to a CSV file with versioning.
            """
            version = len([f for f in os.listdir(rename_dir) if f.startswith(prefix)]) + 1
            filename = f"{prefix}_v{version}.csv"
            path = os.path.join(rename_dir, filename)

            df.to_csv(path, index=False)
            print(f"[SAVED] {prefix} => {path}")

        # Save each DataFrame if available
        if self.main_df is not None:
            save(self.main_df, "main_data_features")

        if self.city_df is not None:
            save(self.city_df, "city_sales_weekly_features")

        if self.category_df is not None:
            save(self.category_df, "category_sales_weekly_features")

        print("\n All feature-engineered datasets saved with versioning.")
        print(f"Check the outputs in: {rename_dir}")
        return self
