import os
import sys
from airflow import DAG
from etl import extract_grammy_ds,transformations_grammy_ds,extraction_spotify_ds,transformations_spotify_ds,merge,load
from datetime import timedelta
from datetime import datetime
from airflow.operators.python import PythonOperator
sys.path.append(os.path.abspath("/opt/airflow/dags/ds1_dag/"))

default_args = {
    
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 15),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    
}

with DAG(
    
    'workshop_dag',
    default_args = default_args,
    description = 'The Workshop DAG which contains the ETL process',
    schedule_interval = '@daily'
    
) as dag:
    
    exctract_grammy_ds = PythonOperator(
        
        task_id ='DS1_Extraction',
        python_callable = extract_grammy_ds(),
        provide_content = True,
        
    )
    
    transformation_gammy_ds = PythonOperator(
        
        task_id = 'DF1_Transformations',
        python_callable = transformations_grammy_ds(),
        provide_content = True,
        
    )
    
    extract_spotify_ds = PythonOperator(
        
        task_id = 'DS2 Extraction',
        python_callable = extraction_spotify_ds(),
        provide_content = True
        
    )
    
    transformation_spotify_df = PythonOperator(
        
        task_id = 'DF2_Transformations',
        python_callable = transformations_spotify_ds(),
        provide_content = True,
        
    )
    
    merge_task = PythonOperator(
        
        task_id = 'Merge',
        python_callable = merge(),
        provide_content = True
        
    )

    load_task = PythonOperator(
        
        task_id = 'Load',
        python_callable = load(),
        provide_content = True
        
    )
    
    exctract_grammy_ds >> transformation_gammy_ds >> merge_task
    extract_spotify_ds >> transformation_spotify_df >> merge_task
    merge_task >> load_task