from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.scrapper import scrapping
from utils.load_in_database import load_in_postgre
from utils.extract_data import extraction
from utils.transformation import transformation
from utils.compute_kpis import compute_kpis
import polars as pl
import os


# Chemins des fichiers
filename = "data/linkedin_posts.parquet"
transformation_output_path = "data/transformed_date.parquet"

# Créer le répertoire data
os.makedirs("data", exist_ok=True)

# Fonctions wrappers pour les tâches Airflow
def scrapping_task():
    df = scrapping()
    df.write_parquet(filename)
    return filename

def load_in_postgre_task():
    df = pl.read_parquet(filename)
    load_in_postgre(df)

def extraction_task():
    extraction(filename)

def transformation_task():
    df = pl.read_parquet(filename)
    transformation(df, transformation_output_path)

def compute_kpis_task():
    compute_kpis(transformation_output_path)

# Paramètres du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG
with DAG(
    'linkedin_pipeline',
    default_args=default_args,
    description='Pipeline pour scraper, transformer et calculer les KPIs des posts LinkedIn',
    schedule_interval='@daily',  # Exécution quotidienne
    start_date=datetime(2025, 4, 30),
    catchup=False,
) as dag:

    # Tâche 1 : Scrapping
    t1 = PythonOperator(
        task_id='scrapping',
        python_callable=scrapping_task,
    )

    # Tâche 2 : Load in Postgre
    t2 = PythonOperator(
        task_id='load_in_postgre',
        python_callable=load_in_postgre_task,
    )

    # Tâche 3 : Extraction
    t3 = PythonOperator(
        task_id='extraction',
        python_callable=extraction_task,
    )

    # Tâche 4 : Transformation
    t4 = PythonOperator(
        task_id='transformation',
        python_callable=transformation_task,
    )

    # Tâche 5 : Compute KPIs
    t5 = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis_task,
    )

    # Définir les dépendances
    t1 >> t2 >> t3 >> t4 >> t5