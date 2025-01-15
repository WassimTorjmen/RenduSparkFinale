import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("no_udf").master("local[*]").getOrCreate()

def main():
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    
    résultat = df.withColumn("category_name", f.when(f.col("category")< "6", "food").otherwise("furniture"))
    
    résultat.show()
    
    df = df.withColumn("date", f.to_date("date", "yyyy-MM-dd")) # il faut la conversion car le type etait stringtype()

    
    daily_window = Window.partitionBy("category", "date")

    df = df.withColumn("total_price_per_category_per_day",f.sum("price").over(daily_window))

    last_30_days_window = Window.partitionBy("category").orderBy("date").rowsBetween(-29, 0)
    
    df = df.withColumn(
        "total_price_per_category_per_day_last_30_days",
        f.sum("price").over(last_30_days_window)
    )
    
    df.show()
