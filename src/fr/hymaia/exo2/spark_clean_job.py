import pyspark.sql.functions as f
from pyspark.sql import SparkSession


spark = SparkSession.builder \
.appName("clean") \
.master("local[*]") \
.getOrCreate()

df_cities=spark.read.option("header","true").csv("src/resources/exo2/city_zipcode.csv")
df_clients=spark.read.option("header","true").csv("src/resources/exo2/clients_bdd.csv")

def main():
    EncapsulationFunction(df_cities,df_clients).write.mode("overwrite").parquet("data/exo2/clean")


def AgeFilter(df):
    return df.filter(f.col("age")>=18)
    

def CityAndClientJoin(df_cities,df_clients):
    return df_cities.join(AgeFilter(df_clients),"zip")

def DepartementCheck(df):
    return df.withColumn(
        "departement",
        f.when((f.col("zip") <= "20190") & (f.col("zip") > "19999"), "2A")
        .when((f.col("zip") > "20190") & (f.col("zip") <= "29999"), "2B")
        .otherwise(f.substring(f.col("zip"), 1, 2))
    )

def EncapsulationFunction(df_cities,df_clients):
    df_clients_filtre=AgeFilter(df_clients)
    df_cities_clients=CityAndClientJoin(df_cities,df_clients_filtre)
    df_dep=DepartementCheck(df_cities_clients)
    return df_dep
    

