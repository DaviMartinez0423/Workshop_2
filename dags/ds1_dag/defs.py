import os
import json
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

def postgres_saving():
    load_dotenv()
    DATA_PATH = os.getenv("DATA_PATH")
    JSON_PATH = os.getenv("JSON_PATH")
    
    with open(JSON_PATH, encoding = 'utf-8') as f:
        config = json.load(f)
    
    user = config['POSTGRES_USER']
    password = config['POSTGRES_PASSWORD']
    host = config['POSTGRES_HOST']
    port = config['POSTGRES_PORT']
    database = config['POSTGRES_DB']
    table =   config['POSTGRES_TABLE']
        
    try:
        connection = psycopg2.connect(
            user = user,
            password = password,
            host = host,
            port = port,
            database = database
        )
        cursor = connection.cursor()
        
        table_query = f"""
            CREATE TABLE IF NOT EXISTS {table}(
                "year" INTEGER,
                "title" VARCHAR(255),
                "published_at" timestamp with time zone,
                "updated_at" timestamp with time zone,
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
            print("PostgreSQL conne ction closed")
    
    ds_location = DATA_PATH
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    df = pd.read_csv(ds_location, delimiter = ',')

    df.to_sql(name = table, con = engine, if_exists = 'append', index = False)

def transformation():
    JSON_PATH = os.getenv("JSON_PATH")
    DATA_PATH = os.getenv('DATA_PATH')
    DATA_PATH3 = os.getenv('DATA_PATH3')
    with open(JSON_PATH, 'r', encoding='utf-8') as f:
        config = json.load(f)

    user = config['POSTGRES_USER']
    password = config['POSTGRES_PASSWORD']
    host = config['POSTGRES_HOST']
    port = config['POSTGRES_PORT']
    database = config['POSTGRES_DB']
    table =   config['POSTGRES_TABLE']
    
    