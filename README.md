# Music and Grammy Awards analyse

In this repository, we will see how we use transform datasets (spotity_dataset and the_grammy_awards).  The main objective of this proyect, is analyze the most nominated artists songs characteristics.

## About the datasets

### spotify_dataset.csv

    •	Number of Columns: 21 (track_id, artists, album_name, track_name, popularity, duration_ms, explicit, danceability, energy, key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, time_signature, track_genre)

    •	Number of Rows: 114.000 (Number of songs)

### the_grammy_awards.csv

    •	Number of Columns: 10 (year, title, published_at, updated_at, category, nominee, artist, workers, img, winner)

    •	Number of Rows: 38480 (Number of nominees)


## Libraries Instalation

The libraries needed to run the project are listed in 'requeriments.txt', you can install them using a package manager such as pip and run the next command:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        pip install - r requeriments.txt
    </pre>
</div>

It's recommended that you enable the digital environment (venv) and run the command to conserve your PC's resources.
However, you can run the command without using the environment just once and continue using the project without any problems.

## Content

You will find the following files and folders in the repository:

1. Migration_Database: This Jupyter notebook contains the connection to the PostgreSQL database and the creation of the table that will contain the dataset.

2. EDA: This file contains the EDA process applied to the dataset.

3. requirements: This file contains the information about the libraries that will be used.

4. dashboard: This is the final dashboard.

5. notebooks: This contains the notebooks that will be used to analyze the data set.

6. dags: This contains the task for the airflow pipeline and the order that must be followed.

7. Dockerfile: This file contains the instructions that docker must follow to build the container.

8. docker-compose: Allow to execute docker applications.

9. settings: This file contains the needed settings to save files in google drive

## How use it

1.  Create a json file in the folder 'dags' called 'config.json', this must contain the credentials of postgreSQL and the names of the tables, like this

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        {
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_HOST": host,
            "POSTGRES_PORT": port,
            "POSTGRES_DB2": Database name,
            "POSTGRES_GRAMMY": "grammy_table",
            "POSTGRES_GRAMMY_TRANS": "grammy_transformed",
            "POSTGRES_SPOTIFY_TRANS": "spotify_transformed",
            "POSTGRES_MERGE_TABLE": "merge_table"
        }
    </pre>
</div>

2.  Create a .env file in the root like this:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        DATA_PATH = "../dataset/the_grammy_awards.csv"
        JSON_PATH = "../dags/config.json"
        DATA_PATH2 = "../dataset/spotify_dataset.csv"
        DATA_PATH3 = "../dataset/DS1_Transformed.csv"
        DATA_PATH4 = "../dataset/DS2_Transformed.csv"
        MERGE_PATH = "../dataset/DS_Merge.csv"
        MODULE_PATH = "../dags/credentials_module.json"
        ID_FOLDER = "Google Drive folder"
    </pre>
</div>

1.  Clone the repository using the command:

<div style="background-color: #000000;font-size: 14px ;color: #FFFFFF; padding: 10px; border: 1px solid #ccc">
    <pre>
        git clone "https://github.com/DaviMartinez0423/Workshop_2.git"
    </pre>
</div>

2.  Execute the file 'etl.py' this will extract and transformate the datasets

3.  Run the file 'merge_and_store.py' this will perform a merge with the datasets and upload it to google drive