from datetime import datetime, timedelta
from time import time

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
# from config import config

default_args = {
    "owner": "nastiuwka",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


def create_tables():
    """
    Initial task needed to create the base tables, which would be further populated.
    Runs only once on the initial startup of the pipeline.
    """
    # params = config()
    # print("Connecting to the postgresql database")
    connection = psycopg2.connect(
        host="host.docker.internal", 
        database="airflow", 
        user="airflow", 
        password="airflow", 
        port="5432"
    )
    # TODO create cursor - what's cursor?
    crsr = connection.cursor()
    crsr.execute(
        """
        create table listening_history (
        track_id char(23),
        played_at timestamptz,
        track_name text,
        artists text,
        album text,
        track_popularity integer
        );
        """
    )
    crsr.execute(
        """
        create table track_features (
        track_id char(23) primary key,
        danceability numeric,
        energy numeric,
        track_key integer,
        loudness numeric,
        track_mode numeric,
        speechiness numeric,
        acousticness numeric,
        instrumentalness numeric, 
        liveness numeric, 
        valence numeric,
        tempo numeric,
        duration_ms integer,
        time_signature integer
        );
        """
    )
    # TODO read about the logic of closing, commiting etc
    crsr.close()
    connection.commit()
    connection.close()

with DAG(
    default_args=default_args,
    dag_id="tables_creation_dag",
    description="Dag for initial setup of the tables in the database",
    start_date=datetime(2022, 9, 23),
    schedule_interval="@once"
) as dag:

    task1 = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    task1
