from src.feature_engineering.feature_engineering_with_pandas import FeatureEngineeringWithPandas
from src.feature_engineering.feature_engineering_with_spark import FeatureEngineeringWithSpark 

class FeatureEngineering:
    def __init__(self, df):
        self.df = df

        if 'pyspark.sql.dataframe.DataFrame' in str(type(df)):
            print("[INFO] Detected Spark DataFrame")
            self.engine = FeatureEngineeringWithSpark(df)
        elif 'pandas.core.frame.DataFrame' in str(type(df)):
            print("[INFO] Detected Pandas DataFrame")
            self.engine = FeatureEngineeringWithPandas(df)
        else:
            raise TypeError("Unsupported data type for feature engineering")

    def run_all(self, group_col, target_col, lags=[1, 2, 4], windows=[1, 2, 4]):
        self.df = self.engine.add_date_parts() # Conversion handled inside if needed
        self.df = self.engine.add_lag_features(group_col, target_col, lags)
        self.df = self.engine.add_rolling_features(group_col, target_col, windows)
        self.df = self.engine.add_trend_features(target_col)
        return self.df
