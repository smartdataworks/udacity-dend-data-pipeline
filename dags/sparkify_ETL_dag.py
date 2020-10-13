#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 11 23:59:47 2020

@author: Emanuel Blei
"""
# =============================================================================
# Import packages
# =============================================================================
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries

# =============================================================================
# Set default arguments
# =============================================================================
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# =============================================================================
# Task definitions
# =============================================================================
dag = DAG('sparkify_ELT',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly')

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_key="log_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    json_path="s3://udacity-dend/log_json_path.json",
    region="us-west-2",
    overwrite=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    s3_key="song_data",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    json_path="auto",
    region="us-west-2",
    overwrite=True
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    sql=SqlQueries.songplay_table_insert,
    redshift_conn_id="redshift",
    target_table="public.songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    sql=SqlQueries.user_table_insert,
    redshift_conn_id="redshift",
    target_table="public.users",
    overwrite=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    sql=SqlQueries.song_table_insert,
    redshift_conn_id="redshift",
    target_table="public.songs",
    overwrite=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    sql=SqlQueries.artist_table_insert,
    redshift_conn_id="redshift",
    target_table="public.artists",
    overwrite=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    sql=SqlQueries.time_table_insert,
    redshift_conn_id="redshift",
    target_table="public.time",
    overwrite=True
)

important_columns = {'artists': 'artistid',
                     'songs': ['songid', 'artistid'],
                     'songplays': 'playid',
                     'time': 'start_time',
                     'users': 'userid'}

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    table_columns_dict=important_columns
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# =============================================================================
# Task dependencies
# =============================================================================
# Step 1 to 3
start_operator >> stage_events_to_redshift >> load_songplays_table
start_operator >> stage_songs_to_redshift >> load_songplays_table

# Step 3 to 5
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks

# Step 5 to 6
run_quality_checks >> end_operator
