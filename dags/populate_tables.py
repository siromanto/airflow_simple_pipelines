from datetime import datetime, timedelta
from time import time

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2

from processing.spotify_processing import Spotify


default_args = {
    "owner": "nastiuwka",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}

# should i create separate function spotify data retrieval and connection
current_date = datetime.now() - timedelta(hours=1)
# print(f"current date: {current_date}")
spotify_obj = Spotify(current_date)
recently_played_lst = spotify_obj.recently_played_tracks
audio_features_lst = spotify_obj.audio_features


def populate_listening_history():
    """
    Task for populating the main table that contains the info on the track and the time when it was listened
    Runs every hour.
    """

    # lists = spotify_obj.get_recently_played_tracks(current_date)
    
    connection = psycopg2.connect(
        host="host.docker.internal",
        database="airflow", 
        user="airflow", 
        password="airflow", 
        port="5432"
    )

    # # TODO create cursor - what's cursor?
    main_query = """
    insert into listening_history (track_id, played_at, track_name, artists, album, track_popularity) values(%s, %s, %s, %s, %s, %s);
    """        

    crsr = connection.cursor()
    crsr.executemany(main_query, (recently_played_lst))
    connection.commit()
    crsr.close()
    connection.close()

def populate_track_features():
    """
    Task for populating the features table that contains the info on the track and the time when it was listened
    Runs every hour.
    """
    connection = psycopg2.connect(
        host="host.docker.internal",
        database="airflow", 
        user="airflow", 
        password="airflow", 
        port="5432"
    )

    main_query = """
    insert into track_features (track_id, danceability, energy, track_key, loudness, track_mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, duration_ms, time_signature) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    on conflict (track_id) do nothing;
    """        

    crsr = connection.cursor()
    crsr.executemany(main_query, (audio_features_lst))
    connection.commit()
    crsr.close()
    connection.close()


with DAG(
    default_args=default_args,
    dag_id="tables_population_dag",
    description="Main dag for scheduling the retieval of data and its population to the tables",
    start_date=datetime(2022, 9, 23),
    schedule_interval="0 * * * *",
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id="populate_listening_history",
        python_callable=populate_listening_history
    )

    task2 = PythonOperator(
        task_id="populate_track_features",
        python_callable=populate_track_features
    )

    task1 >> task2

