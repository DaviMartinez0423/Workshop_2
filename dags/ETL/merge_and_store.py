import os
import json
import logging
import psycopg2
import pandas as pd
from psycopg2 import Error
from sqlalchemy import create_engine
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pydrive2.files import FileNotDownloadableError

credentials_drive = './dags/credentials_module.json'
JSON_PATH = os.getenv("JSON_PATH")

with open(JSON_PATH, encoding = 'utf-8') as f:
    config = json.load(f)
    
user = config['POSTGRES_USER']
password = config['POSTGRES_PASSWORD']
host = config['POSTGRES_HOST']
port = config['POSTGRES_PORT']
database = config['POSTGRES_DB']
table = config['POSTGRES_TABLE']
merge_table = config['POSTGRES_MERGE_TABLE']

def login_drive():
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_drive
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth(credentials_drive)

    if gauth.credentials is None:
        logging.info('Starting Authentication')
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        logging.info('Updating the access credential')
        gauth.Refresh()
    else:
        logging.info('Authentication complete')
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_drive)
    credenciales = GoogleDrive(gauth)
    return credenciales

def upload(merge_path, folder):
    credentials = login_drive()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink", 'id': folder}]})
    file['title'] = merge_path.split('/')[-1]
    file.SetContentFile(merge_path)
    file.Upload()
    logging.info(f'The file {merge_path} file has been successfully uploaded to Google Drive')

def merge(**kwargs):
    ti = kwargs['ti']
    df1 = kwargs['ti'].xcom_pull(key='grammy_dataframe')
    df1 = pd.json_normalize(data=df1)
    
    df2 = kwargs['ti'].xcom_pull(key='spotify_dataframe')
    df2 = pd.json_normalize(data=df2)
    
    merged_df = pd.merge(df1, df2, left_on='artist', right_on='artists' , how='inner')
    logging.info('Data frame merging performed')
    to_eliminate = ['artists', 'updated_at', 'published_at']
    merged_df = merged_df.drop(columns=to_eliminate)
    for i in merged_df.columns:
        if merged_df[i].dtype == 'object':
            merged_df[i] = merged_df[i].astype('string')
    logging.info('Transformations of merged dataframe performed')
    
    kwargs['ti'].xcom_push(key='merge_dataframe', value=merged_df)

def load(**kwargs):
    ti = kwargs['ti']
    merge_df = kwargs['ti'].xcom_push(key='merge_dataframe')
    
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()
        
        table_query = f"""
            CREATE TABLE IF NOT EXISTS {merge_table}(
                "year" INTEGER,
                "title" VARCHAR(255),
                "category" VARCHAR(255),
                "nominee" VARCHAR(255),
                "artist" VARCHAR(255),
                "workers" TEXT,
                "album_name" VARCHAR(255),
                "track_name" VARCHAR(255),
                "popularity" INTEGER,
                "duration_ms" INTEGER,
                "explicit" VARCHAR(255),
                "danceability" REAL,
                "energy" REAL,
                "key" INTEGER,
                "mode" INTEGER,
                "speechiness" REAL,
                "acousticness" REAL,
                "instrumentalness" REAL,
                "liveness" REAL,
                "valence" REAL,
                "tempo" REAL,
                "time_signature" INTEGER,
                "track_genre" VARCHAR(255)
            )
        """
        cursor.execute(table_query)
        connection.commit()
        logging.info('Table created successfully')
        
    except(Exception, psycopg2.Error) as error:
        logging(f'Error: , {error}')
        
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            logging.info('PostgreSQL connection closed')

    merge_df = kwargs['ti'].xcom_pull(key='merged_dataframe')
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    df = pd.read_csv(merge_df, delimiter = ',')

    df.to_sql(name = table, con = engine, if_exists = 'append', index = False)
    logging.info('The Merged Dataframe was saved in the dataset successfully')