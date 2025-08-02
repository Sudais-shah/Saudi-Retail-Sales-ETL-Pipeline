from pyspark.sql import DataFrame as SparkDF
import pandas as pd

def get_wrangler(df):
    if isinstance(df, SparkDF):
        print("Using Spark DataFrame wrangler")
        from src.wranglers.data_wrangling_with_spark import DataWranglingWithSpark
        return DataWranglingWithSpark(df)
    
    elif isinstance(df, pd.DataFrame):
        print("Using Pandas DataFrame wrangler")
        from src.wranglers.data_wrangling_with_pandas import DataWranglingWithPandas
        return DataWranglingWithPandas(df)

    else:
        raise TypeError("Unsupported dataframe type")
    

