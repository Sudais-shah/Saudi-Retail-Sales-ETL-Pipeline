import os

class DataLoader:
    def __init__(self, engine="spark"):
        self.engine = engine.lower()
        if self.engine == "spark":
            from src.utils.spark_session import get_spark_session
            self.spark = get_spark_session()

    def _check_file(self, path):
        if not os.path.exists(path):
            raise FileNotFoundError(f"File {path} does not exist")

    def load_data(self, path, file_type="csv", **options):
        """
        Dynamically loads CSV or Parquet file using pandas or spark based on engine.
        """
        self._check_file(path)
        print(f"Loading {file_type.upper()} file from: {path}")

        if self.engine == "pandas":
            import pandas as pd
            if file_type == "csv":
                return pd.read_csv(path, encoding="utf-8", **options)
            elif file_type == "parquet":
                return pd.read_parquet(path, **options)
            else:
                raise ValueError(f"Unsupported file type: {file_type} for pandas")
            

        elif self.engine == "spark":
            if file_type == "csv":
                options.setdefault("header", True)
                options.setdefault("inferSchema", True)
                return self.spark.read.csv(path, **options)
            elif file_type == "parquet":
                return self.spark.read.parquet(path, **options)
            else:
                raise ValueError(f"Unsupported file type: {file_type} for spark")
            


        else:
            raise ValueError(f"Unsupported engine: {self.engine}")
