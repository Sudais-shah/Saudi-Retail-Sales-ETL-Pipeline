from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_date, weekofyear, month, year, quarter,
    lag, avg, stddev, max as spark_max, min as spark_min,
    col
)
from pyspark.sql.window import Window

class FeatureEngineeringWithSpark:
    def __init__(self, df: DataFrame):
        """
        df: Spark DataFrame with at least [group_col, date_col, target_col]
        """
        self.df = df

    def sort_by_group_and_date(self, group_col: str, date_col: str):
        # Spark is unordered, but our windows will use orderBy on date_col
        # Ensure date_col is DateType
        self.df = self.df.withColumn(date_col, to_date(col(date_col)))
        return self

    def add_date_parts(self, date_col: str = "Week Start"):
        """
        Adds: week_of_year, month, year, quarter
        """
        # ensure date_col exists and is date
        self.df = self.df.withColumn(date_col, to_date(col(date_col)))
        self.df = (
            self.df
            .withColumn("week_of_year", weekofyear(col(date_col)))
            .withColumn("month", month(col(date_col)))
            .withColumn("year", year(col(date_col)))
            .withColumn("quarter", quarter(col(date_col)))
        )
        return self

    def add_lag_features(self, group_col: str, target_col: str, lags: list, date_col: str = "Week Start"):
        """
        Adds {target_col}_lag{n} for each n in lags
        """
        window_spec = Window.partitionBy(group_col).orderBy(col(date_col))
        df = self.df
        for n in lags:
            df = df.withColumn(f"{target_col}_lag{n}", lag(col(target_col), n).over(window_spec))
        self.df = df
        return self

    def add_rolling_features(self, group_col: str, target_col: str, windows: list, date_col: str = "Week Start"):
        """
        Adds rolling mean and std over previous 'w' rows for each w in windows
        """
        df = self.df
        for w in windows:
            w_spec = (
                Window.partitionBy(group_col)
                      .orderBy(col(date_col))
                      .rowsBetween(-w, -1)
            )
            df = (
                df
                .withColumn(f"{target_col}_rollmean{w}", avg(col(target_col)).over(w_spec))
                .withColumn(f"{target_col}_rollstd{w}", stddev(col(target_col)).over(w_spec))
            )
        self.df = df
        return self

    def add_trend_features(self, target_col: str, lag1: int = 1, lag2: int = 2):
        """
        Adds difference and ratio:
         - {target_col}_lag_diff{lag1}_{lag2}
         - {target_col}_lag_ratio{lag1}_{lag2}
        Requires that lag1 and lag2 columns already exist.
        """
        df = self.df
        c1 = f"{target_col}_lag{lag1}"
        c2 = f"{target_col}_lag{lag2}"
        df = (
            df
            .withColumn(f"{target_col}_lag_diff{lag1}_{lag2}", col(c1) - col(c2))
            .withColumn(
                f"{target_col}_lag_ratio{lag1}_{lag2}",
                col(c1) / (col(c2))
            )
        )
        self.df = df
        return self

    def add_interaction_features(self, target_col: str):
        """
        Adds interaction features:
         - {target_col}_lag1_x_week_of_year
         - {target_col}_lag1_x_month
        Requires week_of_year and month present.
        """
        df = self.df
        df = (
            df
            .withColumn(f"{target_col}_lag1_x_week_of_year", col(f"{target_col}_lag1") * col("week_of_year"))
            .withColumn(f"{target_col}_lag1_x_month", col(f"{target_col}_lag1") * col("month"))
        )
        self.df = df
        return self

    def add_all(self,
                group_col: str,
                target_col: str,
                date_col: str = "Week Start",
                lags: list = [1, 2, 4],
                windows: list = [2, 4]):
        """
        Runs the full sequence:
          1. sort_by_group_and_date
          2. add_date_parts
          3. add_lag_features
          4. add_rolling_features
          5. add_trend_features
          6. add_interaction_features
        """
        return (
            self
            .sort_by_group_and_date(group_col, date_col)
            .add_date_parts(date_col)
            .add_lag_features(group_col, target_col, lags, date_col)
            .add_rolling_features(group_col, target_col, windows, date_col)
            .add_trend_features(target_col, lag1=lags[0], lag2=lags[1])
            .add_interaction_features(target_col)
        )

    def to_df(self) -> DataFrame:
        """Returns the transformed Spark DataFrame."""
        return self.df
