import unittest
from pyspark.sql import SparkSession
import pyspark.sql.functions as f 
from src.fr.hymaia.exo2.spark_aggregate_job import PopulationParDep
from spark_singleton_provider import SparkSessionProvider

class TestIntegrationAggregate(unittest.TestCase):
    spark = SparkSessionProvider.get_spark_session()


    def test_Population_Par_Dep(self):
        # Données de test
        data = [
            ("Charles", 25, "75020", "Paris", "75"),
            ("Thomas", 30, "69001", "Lyon", "69"),
            ("Tom", 35, "69001", "Lyon", "69"),
            ("David", 40, "75020", "Paris", "75"),
            ("Momo", 50, "20200", "Bastia", "2B"),
            ("Mimi", 30, "20200", "Bastia", "2B")
        ]
        
        df = self.spark.createDataFrame(data, ["name", "age", "zip", "city", "departement"])

        expected = [
            ("2B", 2), 
            ("69", 2),
            ("75", 2) 
        ]
        expected_df = self.spark.createDataFrame(expected, ["departement", "nb_people"])

        actual_df = PopulationParDep(df)

        self.assertEqual(actual_df.collect(), expected_df.collect(), f"Expected {expected_df.collect()} but got {actual_df.collect()}")


