from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018,11,1),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5) ,
    "catchup" : False    }

dag = DAG('udac_example_dag',
          default_args=default_args,
          schedule_interval= '@hourly',
          description='Load and transform data in Redshift with Airflow' 
         )
                   
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-01-events.json",
    json_path="s3://udacity-dend/log_json_path.json",
    provide_context=True                           )

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/",
    json_path = "auto",
    provide_context  = True                       )
  

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag ,
    table="songplays",
    append = "True",    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query= f"""(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent){SqlQueries.songplay_table_insert} """  )

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    append = "False",    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query= f"""(userid ,first_name ,last_name ,gender ,"level" ){SqlQueries.user_table_insert} """                )

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    append = "False",    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query= f"""(songid,title,artistid,year,duration)
    {SqlQueries.song_table_insert} """            )

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    append = "False",    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query= f"""(artistid,name,location,lattitude,longitude)
    {SqlQueries.artist_table_insert} """            )


load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    append = "False",    
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    sql_query= f"""(start_time,hour,day,week,month,year,weekday)
    {SqlQueries.time_table_insert} """            )


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables = ['users','artists','songs','time'] )

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songplays_table
stage_events_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
