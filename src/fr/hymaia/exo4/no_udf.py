import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("no_udf").master("local[*]").getOrCreate()

def main():
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    résultat = df.withColumn("category_name", f.when(f.col("category")< "6", "food").otherwise("furniture"))
    #résultat.show()
    #print(résultat.count())
    résultat.write.parquet("output/exo4/no_udf")
