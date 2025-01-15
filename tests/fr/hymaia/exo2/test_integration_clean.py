import unittest
import pyspark.sql.functions as f
from src.fr.hymaia.exo2.spark_clean_job import EncapsulationFunction
from spark_singleton_provider import SparkSessionProvider

class TestIntegrationEncapsulationFunction(unittest.TestCase):
    spark = SparkSessionProvider.get_spark_session()

    # Test d'intégration pour EncapsulationFunction
    def test_EncapsulationFunction(self):
        # Données de test pour les villes
        city_data = [
            ("75020", "Paris"),
            ("20145", "Porto Vecchio"),
            ("20200", "Bastia"),
            ("69001", "Lyon")
        ]
        df_cities = self.spark.createDataFrame(city_data, ["zip", "city"])

        # Données de test pour les clients
        clients_data = [
            ("Alice", 25, "75020"),
            ("Bob", 35, "20200"),
            ("Charlie", 30, "69001"),
            ("David", 17, "20145")  # Client mineur, ne doit pas être inclus
        ]
        df_clients = self.spark.createDataFrame(clients_data, ["name", "age", "zip"])

        # Appel de la fonction EncapsulationFunction
        af_actual = EncapsulationFunction(df_cities, df_clients)

        # Résultat attendu
        expected_data = [
            ("75020", "Paris", "Alice", 25, "75"),
            ("20200", "Bastia", "Bob", 35, "2B"),
            ("69001", "Lyon", "Charlie", 30, "69")
        ]
        df_expected = self.spark.createDataFrame(expected_data, ["zip", "city", "name", "age", "departement"])


        # Comparer les résultats
        self.assertEqual(af_actual.collect(), df_expected.collect(), f"Expected {af_actual.collect()}, but got {df_expected.collect()}")

