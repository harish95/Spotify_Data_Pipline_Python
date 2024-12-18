from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta
import json
import boto3
from io import StringIO
import pandas as pd
import requests
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def _fetch_spotify_data(**kwargs):
    
    client_id = Variable.get("spotify_client_id")
    client_secret = Variable.get("spotify_client_secret")
    

    url = "https://accounts.spotify.com/api/token"
    
    headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,}

    response = requests.post(url, headers=headers, data=data)

    # Check the response
    if response.status_code == 200:
        bearer_flag = response.json().get("access_token")
        print("Access token:", response.json().get("access_token"))
    else:
        print(f"Failed to fetch token: {response.status_code}, {response.text}")
    
    response = requests.get(url="https://api.spotify.com/v1/playlists/2sOMIgioNPngXojcOuR4tn/tracks",
                        headers= {"Authorization": f"Bearer {bearer_flag}"})

    data = json.dumps(response.json())
    filename = "spotify_raw_"+str(datetime.now())+ ".json"
    #print(data)
    kwargs["ti"].xcom_push(key="spotify_filename",value=filename)
    kwargs["ti"].xcom_push(key="spotify_data",value=data)
    #s3_client = boto3.client('s3')
    #filename = "raw/to_process/spotify_raw_"+str(datetime.now())+ ".json"
    #s3_client.put_object(Bucket="hbx-spotify", Key=filename, Body=data)

def _read_data_from_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id="AWS_Connection")
    bucket_name = "hbx-spotify"
    prefix = "raw/to_process_airflow/"
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix=prefix)
    print("Available keys: ",keys)
    spotify_data = []
    for key in keys:
        if key.endswith(".json"):
            print("File Available:",key)
            data = s3_hook.read_key(key,bucket_name)
            spotify_data.append(json.loads(data))
    
    kwargs["ti"].xcom_push(key="spotify_data",value=spotify_data)
    


def album(**kwargs):
    spotify_data = kwargs['ti'].xcom_pull(task_ids="Read_Data_from_S3",key="spotify_data")
    
    album_list = []

    for data in spotify_data:
        for row in data['items']:
            album_id = row['track']['album']['id']
            album_name = row['track']['album']['name']
            album_release_date = row['track']['album']['release_date']
            album_total_tracks = row['track']['album']['total_tracks']
            album_url = row['track']['album']['external_urls']['spotify']

            album_element = {'album_id':album_id,'name' :album_name, 'release_date':album_release_date,
                        'total_tracks':album_total_tracks,'url':album_url}
            album_list.append(album_element)

    album_df = pd.DataFrame.from_dict(album_list)
    album_df = album_df.drop_duplicates(subset=['album_id'])
    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    
    album_buffer = StringIO()
    album_df.to_csv(album_buffer,index=False)
    album_content = album_buffer.getvalue()
    kwargs['ti'].xcom_push(key="album_content",value=album_content)


def artist(**kwargs):
    spotify_data = kwargs['ti'].xcom_pull(task_ids="Read_Data_from_S3",key="spotify_data")
    artist_list = []
    
    for data in spotify_data:
        for row in data['items']:
            for key,value in row.items():
                if key == "track":
                    for artist in value['artists']:
                        #print(artist)
                        artist_dict = {'artist_id':artist['id'],'artist_name':artist['name'],'artist_url':artist['href']}
                        artist_list.append(artist_dict)
                        
    artist_df = pd.DataFrame.from_dict(artist_list)
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])
    
    artist_buffer = StringIO()
    artist_df.to_csv(artist_buffer,index=False)
    artist_content = artist_buffer.getvalue()
    kwargs['ti'].xcom_push(key="artist_content",value=artist_content)
    


def song(**kwargs):
    spotify_data = kwargs['ti'].xcom_pull(task_ids="Read_Data_from_S3",key="spotify_data")
    song_list = []
    
    for data in spotify_data:
        for row in data['items']:
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity = row['track']['popularity']
            song_added = row['added_at']

            song_element = {'song_id':song_id,'name':song_name,'duration':song_duration,'url':song_url,
                            'popularity':song_popularity,'song_added':song_added}
            song_list.append(song_element)
    
    song_df = pd.DataFrame.from_dict(song_list)
    song_df['song_added'] = pd.to_datetime(song_df['song_added'])
        
    song_buffer = StringIO()
    song_df.to_csv(song_buffer,index=False)
    song_content = song_buffer.getvalue()
    kwargs["ti"].xcom_push(key="song_content",value=song_content)
        

def _move_processed_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id="AWS_Connection")
    bucket_name = "hbx-spotify"
    prefix = "raw/to_process_airflow/"
    target_prefix = "raw/processed/"
    
    keys = s3_hook.list_keys(bucket_name=bucket_name, prefix = prefix)

    for key in keys:
        if key.endswith(".json"):
            new_key = key.replace(prefix,target_prefix)
            s3_hook.copy_object(source_bucket_key=key,dest_bucket_key=new_key,
                                source_bucket_name=bucket_name,dest_bucket_name=bucket_name)
            s3_hook.delete_objects(bucket=bucket_name,keys=key)
            
            
default_args = {
    "owner":"harish",
    "depends_on_past":False,
    "start_date": datetime(2024,12,15)
    }

dag = DAG(
    dag_id="spotify_etl",
    default_args=default_args,
    description="ETL process for Spotify data",
    schedule_interval=timedelta(days=1),
    catchup=False
)

fetch_data = PythonOperator(
    task_id = "fetch_spotify_data",
    python_callable=_fetch_spotify_data,
    dag = dag
)

store_raw_data_s3 = S3CreateObjectOperator(
    task_id = "Upload_to_S3_raw_data",
    aws_conn_id="AWS_Connection",
    s3_bucket= "hbx-spotify",
    s3_key= "raw/to_process_airflow/{{ task_instance.xcom_pull(task_ids='fetch_spotify_data',key='spotify_filename')}}",
    data="{{task_instance.xcom_pull(task_ids='fetch_spotify_data',key='spotify_data') }}",
    replace=True,
    dag = dag
)
    
read_data_from_s3 = PythonOperator(
    task_id="Read_Data_from_S3",
    python_callable=_read_data_from_s3,
    provide_context=True,
    dag = dag
)

process_album = PythonOperator(
    task_id = "process_album",
    python_callable=album,
    provide_context = True
)

store_album_to_s3 = S3CreateObjectOperator(
    task_id = "store_album_to_S3",
    aws_conn_id="AWS_Connection",
    s3_bucket="hbx-spotify",
    s3_key="transformed/album_data/album_transformed_{{ ts_nodash }}.csv",
    data= " {{task_instance.xcom_pull(task_ids='process_album',key='album_content') }} ",
    replace=True,
    dag = dag
)


process_artist = PythonOperator(
    task_id = "process_artist",
    python_callable=artist,
    provide_context = True
)

store_artist_to_s3 = S3CreateObjectOperator(
    task_id = "store_artist_to_S3",
    aws_conn_id="AWS_Connection",
    s3_bucket="hbx-spotify",
    s3_key="transformed/artist_data/artist_transformed_{{ ts_nodash }}.csv",
    data= " {{task_instance.xcom_pull(task_ids='process_artist',key='artist_content') }} ",
    replace=True,
    dag = dag
)


process_song = PythonOperator(
    task_id = "process_song",
    python_callable=song,
    provide_context = True
)



store_song_to_s3 = S3CreateObjectOperator(
    task_id = "store_song_to_S3",
    aws_conn_id="AWS_Connection",
    s3_bucket="hbx-spotify",
    s3_key="transformed/song_data/song_transformed_{{ ts_nodash }}.csv",
    data= " {{task_instance.xcom_pull(task_ids='process_song',key='song_content') }} ",
    replace=True,
    dag = dag
)


move_processed_data  = PythonOperator(
    task_id = "move_processed_data",
    python_callable=_move_processed_data,    
    provide_context = True,
    dag = dag
)

fetch_data >> store_raw_data_s3 >> read_data_from_s3
read_data_from_s3 >> process_album >> store_album_to_s3
read_data_from_s3 >> process_artist >> store_artist_to_s3
read_data_from_s3 >> process_song >> store_song_to_s3

store_album_to_s3 >> move_processed_data
store_artist_to_s3 >> move_processed_data
store_song_to_s3 >> move_processed_data
