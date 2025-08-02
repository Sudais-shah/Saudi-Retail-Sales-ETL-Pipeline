from src.loaders.config_loader import get_config
from src.loaders.load_data import DataLoader
from src.wranglers.dynamic_wrangler_selecter import get_wrangler
from src.utils.load_by_prefix import load_latest_file_by_prefix
from src.utils.spark_session import get_spark_session
from src.feature_engineering.feature_engineering_base import FeatureEngineering
from src.exporters.feature_df_expoter import FeatureExporter

def run_data_wrangling(config):
    print("Starting Data Wrangling...")
    loader = DataLoader()
    df = loader.load_data(config['data']['raw_dir'])

    wrangler = get_wrangler(df)
    wrangler = wrangler.clean_and_preprocess()
    wrangler = wrangler.validate_schema()
    wrangler = wrangler.detect_and_handle_outliers()
    wrangler = wrangler.aggregate_for_forecasting()
    wrangler = wrangler.run_quality_checks()
    wrangler = wrangler.export_and_version_data()
    print("Data Wrangling Completed.\n")

def run_feature_engineering(config):
    print("Starting Feature Engineering...")
    spark = get_spark_session()
    path = config["data"]["processed_dir"]

    df_category = load_latest_file_by_prefix(path, prefix="category_sales_weekly")
    df_city = load_latest_file_by_prefix(path, prefix="city_sales_weekly")

    fe = FeatureEngineering(df=df_category)
    df_category = fe.run_all(
        group_col="Product Category",
        target_col="Category Weekly Sales",
        lags=[1, 2, 4],
        windows=[2, 4]
    )

    exporter = FeatureExporter(
        main_df=None,
        city_df=df_city,
        category_df=df_category
    )
    exporter.export_and_version_features()
    print("Feature Engineering Completed.\n")

def main():
    config = get_config()
    run_data_wrangling(config)
    run_feature_engineering(config)

if __name__ == "__main__":
    main()
