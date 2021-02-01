from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_origin = 'udacity-dend',
    s3_prefix = 'log_data',
    aws_connection_id = 'aws_credentials',
    redshift_connection_id = 'redshift',
    schema_target = 'public',
    table_target = 'staging_events',
    options = ["json 's3://udacity-dend/log_json_path.json'"]
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_origin = 'udacity-dend',
    s3_prefix = 'song_data',
    aws_connection_id = 'aws_credentials',
    redshift_connection_id = 'redshift',
    schema_target = 'public',
    table_target = 'staging_songs',
    options = ["json 'auto'"]
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table_target = 'songplays',
    dag=dag,
    redshift_connection_id = 'redshift',
    query = SqlQueries.songplay_table_insert,
    truncate_before = True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    table_target = 'users',
    dag=dag,
    redshift_connection_id = 'redshift',
    query = SqlQueries.user_table_insert,
    truncate_before = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    table_target = 'songs',
    dag=dag,
    redshift_connection_id = 'redshift',
    query = SqlQueries.song_table_insert,
    truncate_before = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    table_target = 'artists',
    dag=dag,
    redshift_connection_id = 'redshift',
    query = SqlQueries.artist_table_insert,
    truncate_before = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    table_target = 'time',
    dag=dag,
    redshift_connection_id = 'redshift',
    query = SqlQueries.time_table_insert,
    truncate_before = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_connection_id = 'redshift',
    tables=['songplays', 'users', 'songs', 'artists', 'time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator.set_downstream(
    [stage_events_to_redshift, stage_songs_to_redshift])
load_songplays_table.set_upstream(
    [stage_events_to_redshift, stage_songs_to_redshift])
load_songplays_table.set_downstream(
    [load_song_dimension_table, load_user_dimension_table, 
     load_artist_dimension_table, load_time_dimension_table])
run_quality_checks.set_upstream(
    [load_song_dimension_table, load_user_dimension_table,
     load_artist_dimension_table, load_time_dimension_table])
end_operator.set_upstream(run_quality_checks)
