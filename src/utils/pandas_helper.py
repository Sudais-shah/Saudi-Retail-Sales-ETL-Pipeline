import pandas as pd
import numpy as np
from sklearn.preprocessing import OneHotEncoder
from sklearn.linear_model import LinearRegression

def handle_missing_values_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values in the DataFrame based on column-specific strategies.
    """
    for column in df.columns:
        missing_count = df[column].isnull().sum()  # Count missing values in column
        missing_percent = (missing_count / len(df)) * 100  # Calculate missing percentage

        if column == 'Invoice Date':
            if missing_percent < 5:
                df = df.dropna(subset=[column])  # Drop rows if missing < 5%
            else:
                df[column] = df[column].fillna(method='ffill')  # Forward fill if missing >= 5%

        elif column == 'City':
            if missing_percent > 5:
                df[column] = df[column].fillna('Unknown')  # Fill with 'Unknown' if missing > 5%
            else:
                mode_city = df[column].mode()[0]  # Get most frequent city
                df[column] = df[column].fillna(mode_city)  # Fill with mode if missing <= 5%

        elif column == 'Product Category':
            if missing_percent > 1:
                df[column] = df[column].fillna('UnCategorized')  # Fill with 'UnCategorized' if missing > 1%
            else:
                mode_category = df[column].mode()[0]  # Get most frequent category
                df[column] = df[column].fillna(mode_category)  # Fill with mode if missing <= 1%

        elif column == 'Total Sales':
            df = predict_missing_sales_pandas(df)  # Predict missing sales using regression

    return df

def predict_missing_sales_pandas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Impute missing values in 'Total Sales' using a regression model.
    """
    # Separate rows with and without missing 'Total Sales'
    df_non_missing = df[df['Total Sales'].notnull()]
    df_missing = df[df['Total Sales'].isnull()]

    if df_missing.empty:
        return df  # Nothing to fill

    # One-hot encode categorical features (City, Product Category)
    encoder = OneHotEncoder(sparse=False, handle_unknown='ignore')
    categorical_cols = ['City', 'Product Category']
    encoded_features = encoder.fit_transform(df_non_missing[categorical_cols])
    encoded_missing = encoder.transform(df_missing[categorical_cols])

    # Train the regression model
    lr_model = LinearRegression()
    lr_model.fit(encoded_features, df_non_missing['Total Sales'])

    # Predict missing values
    df_missing['Total Sales'] = lr_model.predict(encoded_missing)

    # Concatenate the DataFrames
    return pd.concat([df_non_missing, df_missing], axis=0).sort_index()

def generate_weekly_summaries_pandas(df: pd.DataFrame) -> tuple:
    """
    Generates weekly summaries:
    - Total Sales
    - Average Sales
    - Count of Transactions
    """
    df['Invoice Date'] = pd.to_datetime(df['Invoice Date'])
    df['Week Start'] = df['Invoice Date'].dt.to_period('W').apply(lambda r: r.start_time)

    # City-Level Weekly Summaries
    city_weekly_summary = df.groupby(['Week Start', 'City']).agg({
        'Total Sales': ['sum', 'mean', 'count']
    }).reset_index()
    city_weekly_summary.columns = ['Week Start', 'City', 'Total Sales', 'Average Sales', 'Transaction Count']
    city_weekly_summary = city_weekly_summary.sort_values('Week Start')

    # Product Category Weekly Summaries
    category_weekly_summary = df.groupby(['Week Start', 'Product Category']).agg({
        'Total Sales': ['sum', 'mean', 'count']
    }).reset_index()
    category_weekly_summary.columns = ['Week Start', 'Product Category', 'Total Sales', 'Average Sales', 'Transaction Count']
    category_weekly_summary = category_weekly_summary.sort_values('Week Start')

    return city_weekly_summary, category_weekly_summary