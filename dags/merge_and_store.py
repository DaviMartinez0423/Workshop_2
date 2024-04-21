import os
import json
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

load_dotenv()

MERGE_PATH = os.getenv("MERGE_PATH")
JSON_PATH = os.getenv("JSON_PATH")
MODULE_PATH = os.getenv('MODULE_PATH')
ID_FOLDER = os.getenv('ID_FOLDER')

with open(JSON_PATH, encoding = 'utf-8') as f:
    config = json.load(f)

user = config['POSTGRES_USER']
password = config['POSTGRES_PASSWORD']
host = config['POSTGRES_HOST']
port = config['POSTGRES_PORT']
database = config['POSTGRES_DB2']
table_merged =   config['POSTGRES_MERGE_TABLE']
grammy_transformed = config['POSTGRES_GRAMMY_TRANS']
spotify_transformed =  config['POSTGRES_SPOTIFY_TRANS']

def login_drive():
    credentials_drive = MODULE_PATH
    GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = credentials_drive
    gauth = GoogleAuth()
    gauth.LoadCredentialsFile(credentials_drive)
    
    if gauth.credentials is None:
        gauth.LocalWebserverAuth(port_numbers=[8092])
    elif gauth.access_token_expired:
        gauth.Refresh()
    else:
        gauth.Authorize()
        
    gauth.SaveCredentialsFile(credentials_drive)
    credentials = GoogleDrive(gauth)
    return credentials

def upload(merge_path, folder):
    credentials = login_drive()
    file = credentials.CreateFile({'parents': [{"kind": "drive#fileLink", 'id': folder}]})
    file['title'] = merge_path.split('/')[-1]
    file.SetContentFile(merge_path)
    file.Upload()
    logging.info(f'The file {merge_path} file has been successfully uploaded to Google Drive')

def merge():
    # Grammy Table Extraction
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    query = f"SELECT * FROM {grammy_transformed};"

    df1 = pd.read_sql(query, engine)
    
    # Spotify Table Extraction
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    query = f"SELECT * FROM {spotify_transformed};"

    df2 = pd.read_sql(query, engine)
    
    
    merged_df = pd.merge(df1, df2, left_on='artist', right_on='artists' , how='inner')
    logging.info('Data frame merging performed')
    to_eliminate = ['artists', 'updated_at', 'published_at']
    merged_df = merged_df.drop(columns=to_eliminate)
    for i in merged_df.columns:
        if merged_df[i].dtype == 'object':
            merged_df[i] = merged_df[i].astype('string')
    logging.info('Transformations of merged dataframe performed')
    
    return merged_df

def DB_load(merged_df):
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
            CREATE TABLE IF NOT EXISTS {table_merged}(
                year INTEGER,
                title VARCHAR(1000),
                category VARCHAR(1000),
                nominee VARCHAR(1000),
                artist VARCHAR(1000),
                workers VARCHAR(1000),
                album_name VARCHAR(1000),
                track_name VARCHAR(1000),
                popularity INTEGER,
                duration_ms INTEGER,
                explicit BOOLEAN,
                danceability REAL,
                energy REAL,
                key INTEGER,
                loudness REAL,
                mode INTEGER,
                speechiness REAL,
                acousticness REAL,
                instrumentalness REAL,
                liveness REAL,
                valence REAL,
                tempo REAL,
                time_signature INTEGER,
                track_genre VARCHAR(1000)
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

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    merged_df.to_sql(name = table_merged, con = engine, if_exists = 'append', index = False)
    logging.info('The Merged Dataframe was saved in the dataset successfully')
    
def main():
    merged_df = merge()
    DB_load(merged_df)
    login_drive()
    upload(MERGE_PATH, ID_FOLDER)

main()