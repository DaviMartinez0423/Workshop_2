import os
import json
import logging
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
DATA_PATH2 = os.getenv("DATA_PATH2")
DATA_PATH3 = os.getenv('DATA_PATH3')
DATA_PATH4 = os.getenv("DATA_PATH4")
JSON_PATH = os.getenv("JSON_PATH")

with open(JSON_PATH, encoding = 'utf-8') as f:
    config = json.load(f)

user = config['POSTGRES_USER']
password = config['POSTGRES_PASSWORD']
host = config['POSTGRES_HOST']
port = config['POSTGRES_PORT']
database = config['POSTGRES_DB2']
table =   config['POSTGRES_GRAMMY']
grammy_transformed = config['POSTGRES_GRAMMY_TRANS']
spotify_transformed =  config['POSTGRES_SPOTIFY_TRANS']

def extract_grammy_ds():
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                "year" INTEGER,
                "title" VARCHAR,
                "published_at" TIMESTAMP WITH TIME ZONE,
                "updated_at" TIMESTAMP WITH TIME ZONE,
                "category" VARCHAR,
                "nominee" VARCHAR,
                "artist" VARCHAR,
                "workers" VARCHAR,
                "img" VARCHAR,
                "winner" BOOLEAN
            )
        """

        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Table created successfully")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection closed")
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    grammy_ds = pd.read_csv(DATA_PATH, delimiter = ',')

    grammy_ds.to_sql(name = table, con = engine, if_exists = 'append', index = False)
    return grammy_ds

def transformations_grammy_ds(grammy_ds):
    # Null Management
    grammy_ds['nominee'] = grammy_ds['nominee'].fillna('Unknown Nominee')
    grammy_ds['artist'] = grammy_ds['artist'].fillna('Unknown Artist')
    grammy_ds['workers'] = grammy_ds['workers'].fillna('Unknown Workers')
    grammy_ds['img'] = grammy_ds['img'].fillna('No Image')
    # Data Type Management
    grammy_ds = grammy_ds.astype({'title': 'string', 'category': 'string', 'nominee': 'string', 'artist': 'string', 'workers': 'string', 'img': 'string', 'winner': 'string'})
    # Removal of Unnecesary Columns
    to_eliminate = ['img', 'winner']
    grammy_ds = grammy_ds.drop(columns=to_eliminate)
    logging.info('Grammy Awards dataframe transformations performed')
    
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {grammy_transformed} (
                year INTEGER,
                title VARCHAR(1000),
                published_at TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE,
                category VARCHAR(1000),
                nominee VARCHAR(1000),
                artist VARCHAR(1000),
                workers VARCHAR(1000)
            )
        """

        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Table created successfully")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection closed")
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    grammy_ds.to_sql(name = grammy_transformed, con = engine, if_exists = 'append', index = False)
    
    return grammy_ds

def extraction_spotify_ds():
    spotify_ds = pd.read_csv(DATA_PATH2, delimiter=',', encoding='utf-8')
    logging.info('The Spotify dataset has been succesfully imported')
    return spotify_ds
        
def transformations_spotify_ds(spotify_ds):
    # Null Management
    spotify_ds.fillna(value={'artists': 'Unknown Artist', 'album_name': 'Unknown Album', 'track_name': 'Unknown Track'}, inplace=True)
    # Data Type Management
    for i in spotify_ds.columns:
        if spotify_ds[i].dtype == 'object':
            spotify_ds[i] = spotify_ds[i].astype('string')
    # Removal Unnecessary Columns
    to_eliminate = ['Unnamed: 0', 'track_id']
    spotify_ds = spotify_ds.drop(columns= to_eliminate)
    logging.info('Spotify data frame transformations performed')
    
    try:
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            database=database
        )
        cursor = connection.cursor()

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {spotify_transformed} (
                "artists" VARCHAR(1000),
                "album_name" VARCHAR(1000),
                "track_name" VARCHAR(1000),
                "popularity" INTEGER,
                "duration_ms" INTEGER,
                "explicit" BOOLEAN,
                "danceability" REAL,
                "energy" REAL,
                "key" INTEGER,
                "loudness" REAL,
                "mode" INTEGER,
                "speechiness" REAL,
                "acousticness" REAL,
                "instrumentalness" REAL,
                "liveness" REAL,
                "valence" REAL,
                "tempo" REAL,
                "time_signature" INTEGER,
                "track_genre" VARCHAR(1000)
            )
        """

        cursor.execute(create_table_query)
        connection.commit()
        logging.info("Table created successfully")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)

    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            logging.info("PostgreSQL connection closed")
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    spotify_ds.to_sql(name = spotify_transformed, con = engine, if_exists = 'append', index = False)
    
    return spotify_ds
    
def main():
    grammy_ds = extract_grammy_ds()
    transformations_grammy_ds(grammy_ds)
    spotify_ds = extraction_spotify_ds()
    transformations_spotify_ds(spotify_ds)
main()