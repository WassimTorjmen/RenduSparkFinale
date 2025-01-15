import pyspark.sql.functions as f
from pyspark.sql import SparkSession


spark = SparkSession.builder \
.appName("aggregate") \
.master("local[*]") \
.getOrCreate()

df_clean=spark.read.parquet("data/exo2/clean")


def main():
    df_final=PopulationParDep(df_clean)
    df_final.write.repartition(1).mode("overwrite") \
    .option("header", "true") \
    .csv("data/exo2/aggregate")




def PopulationParDep(df):
    df_population = df.groupBy("departement").agg(f.count("*").alias("nb_people"))
    return df_population.orderBy(f.col("nb_people").desc(),f.col("departement").asc())

     