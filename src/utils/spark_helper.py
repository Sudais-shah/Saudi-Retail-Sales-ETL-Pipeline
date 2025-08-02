from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Window
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def handle_missing_values_spark(df: DataFrame):
    """
    Handles missing values in the DataFrame based on column-specific strategies.
    """
    for column in df.columns:
        missing_count = df.filter(F.col(column).isNull()).count()  # Count missing values in column
        missing_percent = (missing_count / df.count()) * 100  # Calculate missing percentage

        if column == 'Invoice Date':
            if missing_percent < 5:
                df = df.dropna(subset=[column])  # Drop rows if missing < 5%
            else:
                window_spec = Window.orderBy(F.monotonically_increasing_id())  # Define window for forward fill
                df = df.withColumn(column, F.last(F.col(column), ignorenulls=True).over(window_spec))  # Forward fill if missing >= 5%

        elif column == 'City':
            if missing_percent > 5:
                df = df.withColumn(column, F.when(F.col(column).isNull(), 'Unknown').otherwise(F.col(column)))  # Fill with 'Unknown' if missing > 5%
            else:
                mode_city = df.groupBy(column).count().orderBy('count', ascending=False).first()[0]  # Get most frequent city
                df = df.na.fill({column: mode_city})  # Fill with mode if missing <= 5%

        elif column == 'Product Category':
            if missing_percent > 1:
                df = df.withColumn(column, F.when(F.col(column).isNull(), 'UnCategorized').otherwise(F.col(column)))  # Fill with 'UnCategorized' if missing > 1%
            else:
                mode_category = df.groupBy(column).count().orderBy('count', ascending=False).first()[0]  # Get most frequent category
                df = df.na.fill({column: mode_category})  # Fill with mode if missing <= 1%

        elif column == 'Total Sales':
            df = predict_missing_sales_spark(df)  # Predict missing sales using regression

    return df  # Return DataFrame with handled missing values

def predict_missing_sales_spark(df: DataFrame) -> DataFrame:
    """Impute missing values in 'Total Sales' using a regression model."""

    #  Separate rows with and without missing 'Total Sales'
    df_non_missing = df.filter(F.col('Total Sales').isNotNull())
    df_missing = df.filter(F.col('Total Sales').isNull())
    
    if df_missing.count() == 0:
        return df  # Nothing to fill
    
    # Encode categorical features (City, Product Category)
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_index") for col in ['City', 'Product Category']]
    encoders = [OneHotEncoder(inputCol=f"{col}_index", outputCol=f"{col}_vec") for col in ['City', 'Product Category']]

    for indexer, encoder in zip(indexers, encoders):
        df_non_missing = encoder.fit(indexer.fit(df_non_missing).transform(df_non_missing)).transform(df_non_missing)
        df_missing = encoder.fit(indexer.fit(df_missing).transform(df_missing)).transform(df_missing)

    # Assemble features and train the model
    assembler = VectorAssembler(inputCols=['City_vec', 'Product Category_vec'], outputCol='features')
    lr_model = LinearRegression(featuresCol='features', labelCol='Total Sales').fit(assembler.transform(df_non_missing))

    # Predict missing values and merge the DataFrames
    df_missing = lr_model.transform(assembler.transform(df_missing)).withColumn('Total Sales', F.col('prediction'))

    return df_non_missing.unionByName(df_missing.select(df.columns))


def generate_weekly_summaries(df: DataFrame):
    """
    Generates weekly summaries:
    - Total Sales
    - Average Sales
    - Count of Transactions
    """
      # City-Level Weekly Summaries
    city_weekly_summary = (df.groupBy(F.date_trunc('week', 'Invoice Date').alias('Week Start'), 'City').agg(
            F.sum('Total Sales').alias('Total Sales'), F.avg('Total Sales').alias('Average Sales'),
            F.count('Total Sales').alias('Transaction Count')).orderBy('Week Start'))

    #  Product Category Weekly Summaries
    category_weekly_summary = (df.groupBy(F.date_trunc('week', 'Invoice Date').alias('Week Start'), 'Product Category').agg(
            F.sum('Total Sales').alias('Total Sales'),F.avg('Total Sales').alias('Average Sales'),
            F.count('Total Sales').alias('Transaction Count')).orderBy('Week Start'))

    return city_weekly_summary, category_weekly_summary


