from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    'depends_on_past' : False, 
    'start_date': datetime(2023, 2, 22),
    'email_on_retry' : False,
    'catchup' : False,
    'retries' : 3,
    'retry_delay' : timedelta(minutes = 5)
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",    
    table = "staging_events",
    s3_path = "s3://udacity-dend/log_data",
    region="us-west-2",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_songs",
    s3_path = "s3://udacity-dend/song_data",
    region = "us-west-2",
    json_path = "auto"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert,
    append_only=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.user_table_insert,
    append_only=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.song_table_insert,
    append_only=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.artist_table_insert,
    append_only=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.time_table_insert,
    append_only=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    tables=['songplays','users', 'songs','artists','time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator  >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
load_songplays_table >> [ load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks >> end_operator
