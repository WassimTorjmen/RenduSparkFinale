import unittest
import pyspark.sql.functions as f
from pyspark.sql.utils import AnalysisException
from spark_singleton_provider import SparkSessionProvider
from src.fr.hymaia.exo2.spark_clean_job import CityAndClientJoin, DepartementCheck, AgeFilter

class TestSparkCleanJob(unittest.TestCase):
    spark = SparkSessionProvider.get_spark_session()

    
    # Test de AgeFilter
    def test_Age_Filter(self):
        # Données de test pour AgeFilter
        original_data = [("Tom", 25, 75020),
                         ("Thomas", 17, 69008),
                         ("Charles", 30, 69008),
                         ("Momo", 16, 69001)]
        
        df_non_filtre = self.spark.createDataFrame(original_data, ["name", "age", "zip"])

        expected = [("Tom", 25, 75020),
                    ("Charles", 30, 69008)]
        
        expected_df = self.spark.createDataFrame(expected, ["name", "age", "zip"])

        actual = AgeFilter(df_non_filtre)

        # Comparer les résultats attendus
        assert actual.collect() == expected_df.collect(), f"Expected {expected_df.collect()} but got {actual.collect()}"

    # Test du cas d'erreur pour AgeFilter
    def test_Age_Filter_Error_Case(self):
        # Créer un DataFrame sans la colonne 'age'
        data = [("Tom", 75020)]
        df_sans_age = self.spark.createDataFrame(data, ["name", "zip"])

        with self.assertRaises(AnalysisException):
            AgeFilter(df_sans_age)

    # Test de CityAndClientJoin
    def test_CityAndClientJoin(self):

        # Données de test pour CityAndClientJoin
        df_city = [(17540, "VILLE DU PONT"),
                   (11410, "VILLERS GRELOT"),
                   (61570, "SOLAURE EN DIOIS"),
                   (62310, "ALEYRAC")]
        df_city = self.spark.createDataFrame(df_city, ["zip", "city"])

        df_clients = [("Latch", 67, 17540),
                      ("Paquette", 46, 11410),
                      ("Whitson", 24, 61570),
                      ("Pumper", 39, 62310)]
        df_clients = self.spark.createDataFrame(df_clients, ["name", "age", "zip"])

        # Appliquer CityAndClientJoin
        df_city_client_actual = CityAndClientJoin(df_city, df_clients)

        expected = [(17540, "VILLE DU PONT", "Latch", 67),
                    (11410, "VILLERS GRELOT", "Paquette", 46),
                    (61570, "SOLAURE EN DIOIS", "Whitson", 24),
                    (62310, "ALEYRAC", "Pumper", 39)]
        
        df_expected = self.spark.createDataFrame(expected, ["zip", "city", "name", "age"])

        # Comparer les résultats attendus
        assert df_city_client_actual.collect() == df_expected.collect(), f"Expected {df_expected.collect()}, but got {df_city_client_actual.collect()}"

    # Test de DepartementCheck
    def test_Departement_Check(self):

        # Données de test pour DepartementCheck
        city_data = [("75020", "Paris"),
                     ("20145", "Porto Vecchio"),
                     ("20200", "Bastia"),
                     ("69001", "Lyon")]
        df_city = self.spark.createDataFrame(city_data, ["zip", "city"])

        # Appliquer DepartementCheck
        actual = DepartementCheck(df_city)

        # Résultat attendu
        expected = [("75020", "Paris", "75"),
                    ("20145", "Porto Vecchio", "2A"),
                    ("20200", "Bastia", "2B"),
                    ("69001", "Lyon", "69")]
        
        df_expected = self.spark.createDataFrame(expected, ["zip", "city", "departement"])

        # Comparer les résultats attendus
        self.assertEqual( actual.collect(),df_expected.collect(), f"Expected {df_expected.collect()}, but got {actual.collect()}")

