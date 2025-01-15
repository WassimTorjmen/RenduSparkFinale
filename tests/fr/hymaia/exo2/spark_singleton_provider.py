from pyspark.sql import SparkSession

class SparkSessionProvider:
    _instance=None

    @classmethod
    def get_spark_session(cls):
        if cls._instance is None:
            cls._instance=SparkSession.builder \
            .appName("SharedSparkProvider") \
            .master("local[*]") \
            .getOrCreate()
        return cls._instance
        
