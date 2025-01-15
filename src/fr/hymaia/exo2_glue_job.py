import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType
 
# clean
def filtre(df_client):
    return df_client.where(f.col("age")>=18)

def join_client_city(df_filtre_client,df_city):
    return  (df_filtre_client
             .join(df_city,on="zip",how='inner')
             .select("name","age","zip","city"))


def ajout_departement(df_join_client):

   df_departement = df_join_client.withColumn(
        'departement',
        f.when(f.substring(f.col("zip"), 1, 2) == '20',  
             f.when(f.col("zip") <= 20190, "2A")
             .otherwise("2B"))
        .otherwise(f.substring(f.col("zip"), 1, 2))  
   )
   return df_departement
  
def integration_clean(df_client,df_city):
    df_filtre_client = filtre(df_client)
    df_join_client = join_client_city(df_filtre_client,df_city)
    df_departement= ajout_departement(df_join_client)

    return df_departement

# aggregate
def nbr_clients_departement(df_departement):
    if 'departement' not in df_departement.columns:
        raise ValueError("Le DataFrame d'entrÃ©e doit contenir une colonne 'departement'")
       
    df_clients_par_departement = df_departement.groupBy("departement").count()\
    .withColumnRenamed("count", "nb_people")\
    .withColumn("nb_people", f.col("nb_people").cast(IntegerType())) 

    df_client_par_departement_trie = df_clients_par_departement.orderBy(
        f.col("nb_people").desc(),
        f.col("departement").asc())

    return df_client_par_departement_trie.select("departement", "nb_people")

def integration_aggregate(df_departement):
    return nbr_clients_departement(df_departement)

if __name__ == '__main__':
    
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    
    
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH"])
    job.init(args['JOB_NAME'], args)

    
    input_path = args["INPUT_PATH"]
    output_path = args["OUTPUT_PATH"]
    
    
    df_client = spark.read.csv(f"{input_path}/clients_bdd.csv", header=True)
    df_city = spark.read.csv(f"{input_path}/city_zipcode.csv", header=True)

    
    df_clean = integration_clean(df_client, df_city)
    clean_output_path = f"{output_path}/clean"
    df_clean.write.mode('overwrite').parquet(clean_output_path)
    
    
    nbr_clients_par_departement = integration_aggregate(df_clean)
    aggregate_output_path = f"{output_path}/aggregate"
    nbr_clients_par_departement.write.mode('overwrite').csv(aggregate_output_path, header=True)

    # Fin du job Glue
    job.commit()
