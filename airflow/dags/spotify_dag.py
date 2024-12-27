from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import base64
import logging
from typing import List, Dict

# Configuration
SPOTIFY_CONFIG = {
    'client_id': '9a4669ba4dab4c38af3a23eee73ebf00',
    'client_secret': '3dd78949576b4f0e92233fd9f78c535f',
    'base_url': 'https://api.spotify.com/v1/',
    'playlist_ids': [
        '17WrF3wsE02f0B711n9Dfh',  # My pop
        '6KZBOppEoNs230tigr5ptQ'   # Ishq Murshid
    ]
}

SNOWFLAKE_CONFIG = {
    'conn_id': 'snowflake_default',
    'table': 'PLAYLIST_TRACKS'
}

def get_spotify_token(**context):
    """Fetch Spotify access token"""
    try:
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_header = base64.b64encode(
            f"{SPOTIFY_CONFIG['client_id']}:{SPOTIFY_CONFIG['client_secret']}".encode()
        ).decode()
        
        headers = {
            'Authorization': f'Basic {auth_header}'
        }
        data = {'grant_type': 'client_credentials'}
        
        response = requests.post(auth_url, headers=headers, data=data)
        response.raise_for_status()
        
        access_token = response.json()['access_token']
        context['task_instance'].xcom_push(key='spotify_token', value=access_token)
        logging.info("Successfully obtained Spotify access token")
        return access_token
        
    except Exception as e:
        logging.error(f"Error getting Spotify token: {str(e)}")
        raise

def fetch_spotify_data(**context):
    """Fetch track data from Spotify API"""
    try:
        access_token = context['task_instance'].xcom_pull(
            task_ids='get_spotify_token', 
            key='spotify_token'
        )
        headers = {'Authorization': f'Bearer {access_token}'}
        # logging.info(f"Successfully obtained Spotify access token: {str(access_token)}")

        all_tracks = []
        for playlist_id in SPOTIFY_CONFIG['playlist_ids']:
            url = f"{SPOTIFY_CONFIG['base_url']}playlists/{playlist_id}/tracks"
            logging.info(f"URL hitting: {str(url)}")
            response = requests.get(url, headers=headers)
            # response.raise_for_status()
            if response.status_code != 200:
                continue
            else:
                for item in response.json()['items']:
                    track = item['track']
                    track_data = {
                        'track_id': track['id'],
                        'name': track['name'],
                        'artist': track['artists'][0]['name'],
                        'album': track['album']['name'],
                        'duration_ms': track['duration_ms'],
                        'popularity': track['popularity'],
                        'processed_at': datetime.now().isoformat()
                    }
                    all_tracks.append(track_data)
        
        context['task_instance'].xcom_push(key='tracks_data', value=all_tracks)
        logging.info(f"Successfully fetched {len(all_tracks)} tracks")
        return all_tracks
        
    except Exception as e:
        logging.error(f"Error in fetch_spotify_data: {str(e)}")
        return None

def load_to_snowflake(**context) -> int:
    """Load tracks data to Snowflake"""
    try:
        tracks_data = context['task_instance'].xcom_pull(
            task_ids='fetch_spotify_data', 
            key='tracks_data'
        )
        
        if not tracks_data:
            logging.warning("No tracks data to load")
            return 0
            
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONFIG['conn_id'])
        logging.info(f"Successfully obtained Snowflake Hook: {str(snowflake_hook)}")
        
        # Prepare batch insert query
        insert_query = f"""
        INSERT INTO {SNOWFLAKE_CONFIG['table']} (
            track_id, name, artist, album, 
            duration_ms, popularity, processed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        logging.info(f"insert query: {insert_query}")
        # Convert tracks to tuple format for insertion
        records = [(
            track['track_id'],
            track['name'],
            track['artist'],
            track['album'],
            track['duration_ms'],
            track['popularity'],
            track['processed_at']
        ) for track in tracks_data]
        
        # Execute batch insert
        snowflake_hook.run(
            insert_query,
            parameters=records
        )
        
        logging.info(f"Successfully loaded {len(records)} records to Snowflake")
        return len(records)
        
    except Exception as e:
        logging.error(f"Error in load_to_snowflake: {str(e)}")
        raise

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30)
}

with DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='Spotify ETL Pipeline with Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spotify', 'etl', 'snowflake']
) as dag:
    
    get_token_task = PythonOperator(
        task_id='get_spotify_token',
        python_callable=get_spotify_token
    )
    
    fetch_data_task = PythonOperator(
        task_id='fetch_spotify_data',
        python_callable=fetch_spotify_data
    )
    
    load_snowflake_task = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )
    
    # Set task dependencies
    get_token_task >> fetch_data_task >> load_snowflake_task
