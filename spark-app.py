import sys
from src.fr.hymaia.exo1.main import main as wordcount_main
from src.fr.hymaia.exo2.spark_clean_job import main as clean_main
from src.fr.hymaia.exo2.spark_aggregate_job import main as aggregate_main
from src.fr.hymaia.exo4.python_udf import main as python_udf_main
from src.fr.hymaia.exo4.scala_udf import main as scala_udf_main
from src.fr.hymaia.exo4.no_udf import main as no_udf_main

def run_job(job_name):
    if job_name == "wordcount":
        wordcount_main()
    elif job_name == "clean":
        clean_main()
    elif job_name == "aggregate":
        aggregate_main()
    elif job_name == "python_udf":
        python_udf_main()
    elif job_name == "scala_udf":
        scala_udf_main()
    elif job_name == "no_udf":
        no_udf_main()
    else:
        print(f"Unknown job: {job_name}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-app.py <job_name>")
        sys.exit(1)
    
    job_name = sys.argv[1]
    
    # Exécuter le job demandé
    run_job(job_name)
