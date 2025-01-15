import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq

spark = SparkSession.builder.config('spark.jars', 'src/resources/exo4/udf.jar').appName("scala_udf").master("local[*]").getOrCreate()

def main():
    df = spark.read.csv("src/resources/exo4/sell.csv", header=True)
    resultat = df.withColumn("category_name", addCategoryName(f.col("category")))
    #résultat.show()
    #print(résultat.count())
    resultat.write.parquet("output/exo4/scala_udf")


def addCategoryName(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))
