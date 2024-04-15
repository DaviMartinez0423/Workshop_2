import os
import json
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

DATA_PATH = os.getenv("DATA_PATH")
JSON_PATH = os.getenv("JSON_PATH")
DATA_PATH2 = os.getenv("DATA_PATH2")
DATA_PATH3 = os.getenv('DATA_PATH3')
DATA_PATH4 = os.getenv("DATA_PATH4")
MERGE_PATH = os.getenv("MERGE_PATH")

with open(JSON_PATH, encoding = 'utf-8') as f:
    config = json.load(f)
    
user = config['POSTGRES_USER']
password = config['POSTGRES_PASSWORD']
host = config['POSTGRES_HOST']
port = config['POSTGRES_PORT']
database = config['POSTGRES_DB']
table =   config['POSTGRES_TABLE']

def extract_grammy_ds(**kwargs):
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
            CREATE TABLE IF NOT EXISTS {table}(
                "year" INTEGER,
                "title" VARCHAR(255),
                "published_at" TIMESTAMP WITH TIME ZONE,
                "updated_at" TIMESTAMP WITH TIME ZONE,
                "category" VARCHAR(255),
                "nominee" VARCHAR(255),
                "artist" VARCHAR(255),
                "workers" TEXT,
                "img" TEXT,
                "winner" VARCHAR(255)
            )
        """
        cursor.execute(table_query)
        connection.commit()
        print("Table created successfully")
        
    except(Exception, psycopg2.Error) as error:
        print("Error: ", error)
        
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            print("PostgreSQL connection closed")
            
    ds_location = DATA_PATH
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    df = pd.read_csv(ds_location, delimiter=',')
    df.to_sql(name=table, con=engine, if_exists='append', index=False)

    query = f"SELECT * FROM {table}"
    df1 = pd.read_sql(query, engine)
    kwargs['ti'].xcom_push(key='grammy_dataframe', value=df1)

def transformations_grammy_ds(**kwargs):
    df1 = kwargs['ti'].xcom_pull(key='grammy_dataframe')
    # Null Management
    df1['nominee'] = df1['nominee'].fillna('Unknown Nominee')
    df1['artist'] = df1['artist'].fillna('Unknown Artist')
    df1['workers'] = df1['workers'].fillna('Unknown Workers')
    df1['img'] = df1['img'].fillna('No Image')
    # Data Type Management
    df1 = df1.astype({'title': 'string', 'category': 'string', 'nominee': 'string', 'artist': 'string', 'workers': 'string', 'img': 'string', 'winner': 'string'})
    # Removal of Unnecesary Columns
    to_eliminate = ['img', 'winner']
    df1 = df1.drop(columns=to_eliminate)
    
    kwargs['ti'].xcom_push(key='grammy_dataframe', value=df1)

def extraction_spotify_ds(**kwargs):
    df2 = pd.read_csv(DATA_PATH2, delimiter=',', encoding='utf-8')
    kwargs['ti'].xcom_push(key='spotify_dataframe', value=df2)
    
def transformations_spotify_ds(**kwargs):
    df2 = kwargs['ti'].xcom_pull(key='spotify_dataframe')
    # Null Management
    df2.fillna(value={'artists': 'Unknown Artist', 'album_name': 'Unknown Album', 'track_name': 'Unknown Track'}, inplace=True)
    # Data Type Management
    for i in df2.columns:
        if df2[i].dtype == 'object':
            df2[i] = df2[i].astype('string')
    # Removal Unnecessary Columns
    to_eliminate = ['Unnamed: 0', 'track_id']
    df2 = df2.drop(columns= to_eliminate)
    
    kwargs['ti'].xcom_push(key='spotify_dataframe', value=df2)
       
def merge(**kwargs):
    df1 = kwargs['ti'].xcom_pull(key='grammy_dataframe')
    df2 = kwargs['ti'].xcom_pull(key='spotify_dataframe')
    merged_df = pd.merge(df1, df2, left_on='artist', right_on='artists' , how='inner')
    to_eliminate = ['artists', 'updated_at', 'published_at']
    merged_df = merged_df.drop(columns=to_eliminate)
    for i in merged_df.columns:
        if merged_df[i].dtype == 'object':
            merged_df[i] = merged_df[i].astype('string')
            
    kwargs['ti'].xcom_push(key='merge_dataframe', value=merged_df)
            
def load(**kwargs):
    merge_df = kwargs['ti'].xcom_push(key='merge_dataframe')
    
