from pyspark.sql import SparkSession
import pandas as pd
import os

class DataLoader:
   def __init__(self, app_name = 'Saudi Retail Demand Forecasting'):
       self.spark = SparkSession.builder. \
         appName(app_name).\
         getOrCreate()
    
       print(f"Spart Session created with app name: {app_name}")


   def load_csv(self, path, header=True, inferSchema=True, schema=None):
     """ 
     Load a csv file into a spark dataframe
     """
     if not os.path.exists(path):
          raise FileNotFoundError(f"file {path} does not exist")
     
     print(f"Loading CSV file from: {path}")
     df = self.spark.read.csv(path=path, header=header, inferSchema=inferSchema, schema=schema 
                              ) ## In pandas this is read_csv
     return df

   def load_parquet(self, path):
     """ 
     Load a parquet file into a spark dataframe
     """
     if not os.path.exists(path):
          raise FileNotFoundError(f"file {path} does not exist")
     
     print(f"Loading parquet file from: {path}")
     df = self.spark.read.parquet(path=path) ## In pandas this is read_parquet
     return df
