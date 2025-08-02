from pyspark.sql import SparkSession
from src.loaders.config_loader import get_config

_spark = None

_spark = None

def get_spark_session():
    global _spark
    if _spark is None:
        cfg = get_config()

        # Create the builder and set the config BEFORE calling getOrCreate()
        builder = (SparkSession.builder
                   .appName(cfg['spark']['app_name'])
                   .config("spark.sql.legacy.timeParserPolicy", "LEGACY"))

        _spark = builder.getOrCreate()

    return _spark
