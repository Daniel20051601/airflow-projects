import sys 
sys.path.append("/opt/cine-recommender")
from etl.extract import extract_recent_movies
from etl.transform import transform_movies
from etl.load import load_new_movies
import pandas as pd
from io import StringIO
from datetime import timedelta, datetime
from airflow.providers.standard.operators.python import PythonOperator

from airflow.models import DAG


def extract(**context):
    
    print('Extrayendo peliculas recientes')
    
    df_movies  = extract_recent_movies()
    
    print(f"Peliculas extraidas: {len(df_movies)}")
    
    if df_movies.empty:
        return "No se encontraron peliculas nuevas"
    
    context['ti'].xcom_push(key='movies', value=df_movies.to_json())


def transform(**context):
        
    print("Transformando los datos")
    
    movies_json = context['ti'].xcom_pull(key='movies', task_ids='extract')
    df_movies = pd.read_json(StringIO(movies_json))
    
    transformed_data = transform_movies(df_movies)
    
    print("Datos transformados")
    print(transformed_data.head())
    
    context['ti'].xcom_push(key='transformed', value=transformed_data.to_json())
    
    
def load(**context):
    print('Cargando datos')
    
    movies_json = context['ti'].xcom_pull(key='transformed', task_ids='transform')
    transformed_data = pd.read_json(StringIO(movies_json))
    
    if 'release_date' in transformed_data.columns:
        transformed_data['release_date'] = pd.to_datetime(
            transformed_data['release_date'], unit='ms', errors='coerce'
        ).dt.date
    
    if load_new_movies(transformed_data):
        print("Catálogo actualizado correctamente ✅")
    
    
default_args = {
    'owner' : 'Emilio',
    'start_date' : datetime(2025,1,1),
    'email' : ['danielvilopez@gmail.com'],
    'retries' : 1,
    'retry_delay' : timedelta(minutes= 5)
}

with DAG(
    dag_id = 'Update-Movie-Catalog',
    default_args = default_args,
    description = 'Agrega nuevo contenido a la base de datos de Cine Recommender',
    schedule = timedelta(days = 1)
) as dag:
    
    extract_data = PythonOperator(
        task_id = 'extract',
        python_callable = extract,
        execution_timeout = timedelta(hours = 1)
    )
    
    transform_data = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        execution_timeout = timedelta(hours = 1)
    )
    
    load_data = PythonOperator(
        task_id = 'load',
        python_callable = load,
        execution_timeout = timedelta(hours = 1)
    )
    
    extract_data >> transform_data >> load_data
    
    
    