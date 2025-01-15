import pyspark.sql.functions as f 
from pyspark.sql import SparkSession 
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("python_udf").master("local[*]") .getOrCreate()

def main():
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    résultat = df.withColumn("category_name", ajout_category_name(f.col("category")))
    #résultat.show()
    #print(résultat.count())
    résultat.write.parquet("output/exo4/python_udf")


@f.udf(returnType=StringType())   
def ajout_category_name(category):
    if category < "6":
        return "food"
    else: 
        return "furniture"
