import pandas as pd
import numpy as np

class FeatureEngineeringWithPandas:
    def __init__(self, df):
        self.df = df.copy()

    def sort_by_group_and_date(self, group_col, date_col="Week Start"):
        # Ensure the date column is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.df[date_col]):
             print(f"Warning: Converting '{date_col}' to datetime in sort_by_group_and_date.")
             self.df[date_col] = pd.to_datetime(self.df[date_col], errors="coerce")
             nat_count = self.df[date_col].isna().sum()
             if nat_count > 0:
                 print(f"Warning: {nat_count} NaT values found in '{date_col}' after conversion.")

        self.df = self.df.sort_values([group_col, date_col]).reset_index(drop=True)
        return self.df

    def add_date_parts(self, date_col="Week Start"):
        # CRITICAL: Ensure the column is datetime before using .dt
        # This guarantees the conversion happens if needed.
        if not pd.api.types.is_datetime64_any_dtype(self.df[date_col]):
             print(f"Warning: Converting '{date_col}' to datetime in add_date_parts.")
             self.df[date_col] = pd.to_datetime(self.df[date_col], errors="coerce")
             nat_count = self.df[date_col].isna().sum()
             if nat_count > 0:
                 print(f"Warning: {nat_count} NaT values found in '{date_col}' after conversion. Date features might be incorrect.")

        # Check again after conversion attempt
        if not pd.api.types.is_datetime64_any_dtype(self.df[date_col]):
            raise TypeError(f"Column '{date_col}' could not be converted to datetime. Check data format.")

        # Now it's safe to use .dt (assuming conversion worked for most rows)
        try:
            # Use .dt.isocalendar().week for correct ISO week calculation
            self.df['week_of_year'] = self.df[date_col].dt.isocalendar().week
            self.df['month'] = self.df[date_col].dt.month
            self.df['year'] = self.df[date_col].dt.year
            self.df['quarter'] = self.df[date_col].dt.quarter
        except Exception as e: # Catch potential issues with .dt access even after conversion
            raise RuntimeError(f"Error accessing datetime properties of '{date_col}'. "
                               f"Column dtype is {self.df[date_col].dtype}. "
                               f"Original error: {e}")

        return self.df

    def add_lag_features(self, group_col, target_col, lags, date_col="Week Start"):
        # Ensure data is sorted and date is datetime before shifting
        self.sort_by_group_and_date(group_col, date_col)
        for lag in lags:
            self.df[f'{target_col}_lag{lag}'] = (
                self.df.groupby(group_col)[target_col].shift(lag)
            )
        return self.df

    def add_rolling_features(self, group_col, target_col, windows, date_col="Week Start"):
        # Ensure data is sorted and date is datetime before rolling
        self.sort_by_group_and_date(group_col, date_col)
        for w in windows:
            grp = self.df.groupby(group_col)[target_col]
            # Shift by 1 before rolling to avoid data leakage
            self.df[f'{target_col}_rollmean{w}'] = grp.shift(1).rolling(w, min_periods=1).mean()
            self.df[f'{target_col}_rollstd{w}']  = grp.shift(1).rolling(w, min_periods=1).std()
        return self.df

    def add_trend_features(self, target_col, lag1=1, lag2=2):
        c1 = f"{target_col}_lag{lag1}"
        c2 = f"{target_col}_lag{lag2}"
        # Check if required lag columns exist
        if c1 not in self.df.columns or c2 not in self.df.columns:
            raise ValueError(f"Trend features require lag columns {c1} and {c2}. Ensure lag features are added first.")
        self.df[f"{target_col}_lag_diff{lag1}_{lag2}"]  = self.df[c1] - self.df[c2]
        # Add small epsilon to prevent division by zero
        self.df[f"{target_col}_lag_ratio{lag1}_{lag2}"] = self.df[c1] / (self.df[c2] + 1e-6)
        return self.df

   

