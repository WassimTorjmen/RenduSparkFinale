import pyspark.sql.functions as f
from pyspark.sql import SparkSession

#pour lancer une session de spark
spark = SparkSession.builder \
.appName("wordcount") \
.master("local[*]") \
.getOrCreate()

def main():
    #lire le fichier csv option("header") pour indiquer qu'il y'a une colonne/ligne de header
    df=spark.read.option("header", "true").csv("src/resources/exo1/data.csv")
    #wordcount(df,"text").show() #faut toujours ajouter le .show() pour afficher les resultats
    #stocker le resultat dans un df
    wordcount_df=wordcount(df,"text")
    #ecrire le resultat en parquet en ecrasant les anciens fichiers a travers overwrite
    wordcount_df.write.mode("overwrite").partitionBy("count").parquet("data/exo1/output")




def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()




 