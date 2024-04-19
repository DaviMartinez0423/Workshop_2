import pandas as pd
import psycopg2
import logging
import json
import os

JSON_PATH = os.getenv('JSON_PATH')



def conn():
    try:
        with open(JSON_PATH, encoding = 'utf-8') as f:
            config = json.load(f)
            
        user = config['POSTGRES_USER']
        password = config['POSTGRES_PASSWORD']
        host = config['POSTGRES_HOST']
        port = config['POSTGRES_PORT']
        database = config['POSTGRES_DB']
        table =   config['POSTGRES_TABLE']
        
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
        logging.info('Table created successfully')
        
    except(Exception, psycopg2.Error) as error:
        logging(f'Error: , {error}')
        
    finally:
        if 'connection' in locals():
            cursor.close()
            connection.close()
            logging.info('PostgreSQL connection closed')