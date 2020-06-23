from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'catchup':True,
    'depends_on_past':True,
    'retries' : 3,
    'retry_delay': timedelta(seconds=300)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

# Add create Table Task(New added)
"""
create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)
"""

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_key = "log/data",
    s3_bucket = "udacity-dend",
    table = "staging_events"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    s3_key = "song/data",
    s3_bucket = "udacity-dend",
    table = "staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    load_fact_sql =  SqlQueries.songplay_table_insert,
    table = "songplays",
    append_data = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    load_fact_sql =  SqlQueries.user_table_insert,
    table = "users",
    append_data = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    load_fact_sql =  SqlQueries.song_table_insert,
    table = "songs",
    append_data = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    load_fact_sql =  SqlQueries.artist_table_insert,
    table = "artisits",
    append_data = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    load_fact_sql =  SqlQueries.time_table_insert,
    table = "time",
    append_data = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table = 'staging_events',
    redshift_conn_id = "redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift 
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table 
load_songplays_table >> load_song_dimension_table 
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator


