import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, count, when

class DataExplorer:
    def __init__(self, dataframe):
        """
        Initializes the DataExplorer with a DataFrame Pandas or Spark.
        """
        self.df = dataframe
        self.is_spark = isinstance(dataframe, SparkDataFrame)

# =====================================================================================
    def explore_data_with_spark(self, schema=False, overview=False, summary=False, 
                     nulls=False, duplicates=False, value_counts=False, all=False):
        """
        Displays information about the DataFrame.
        """
        if all:
            schema = overview = summary = nulls = duplicates = value_counts = True
        
        if self.is_spark:
            if schema:
                self.df.printSchema()
            if overview:
                print(f"Rows: {self.df.count()}, Columns: {self.df.columns}")
            if summary:
                self.df.summary().show()
            if nulls:
                null_counts = self.df.select([count(when(col(c).isNull(), c)).alias(c) for c in self.df.columns])
                null_counts.show()
            if duplicates:
                duplicates = self.df.groupBy(self.df.columns).count().filter("count > 1").count()
                print(f"Duplicate Rows: {duplicates}")
            if value_counts:
                for column in self.df.columns:
                    print(f"\n--- {column} ---")
                    self.df.groupBy(column).count().orderBy('count', ascending=False).show()
        
        else:
            self.explore_data_with_pandas(schema, overview, summary, nulls, duplicates, value_counts, all)

# =====================================================================================

    def explore_data_with_pandas(self, schema=False, overview=False, summary=False, 
                                 nulls=False, duplicates=False, value_counts=False, all=False):
        """
        Displays information for Pandas DataFrame.
        """
        
        if all:
            schema = overview = summary = nulls = duplicates = value_counts = True
        if schema:
            print(self.df.dtypes)
        if overview:
            print(f"Rows: {self.df.shape[0]}, Columns: {self.df.shape[1]}")
            print(f"Column Names: {list(self.df.columns)}")
        if summary:
            print(self.df.describe())
        if nulls:
            print(self.df.isnull().sum())
        if duplicates:
            print(f"Duplicate Rows: {self.df.duplicated().sum()}")
        if value_counts:
            for column in self.df.columns:
                print(f"\n--- {column} ---")
                print(self.df[column].value_counts())
