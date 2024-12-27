# Importing necessary libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests
import json
import base64
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Importing Kafka producers and consumers classes
from spotify_kafka_producer import SpotifyKafkaProducer
from spotify_kafka_consumer import SpotifyKafkaConsumer

# Setting up configurations for Spotify, Snowflake and Kafka
SPOTIFY_CONFIG = {
    'client_id': os.getenv('CLIENT_ID'),
    'client_secret': os.getenv('CLIENT_SECRET'),
    'base_url': 'https://api.spotify.com/v1/',
    'playlist_ids': [
        '17WrF3wsE02f0B711n9Dfh',  # My pop
        '6KZBOppEoNs230tigr5ptQ'   # Ishq Murshid
    ]
}

SNOWFLAKE_CONFIG = {
    'conn_id': 'snowflake_default',
    'table': ['SPOTIFY_TRACKS', 'ARTIST_DETAILS']
}

KAFKA_CONN_ID = "kafka_default"

# Function to authenticate the spotify account
def get_spotify_token(**context):
    """
    Fetch Spotify access token
    Args:
        :context (dict): Airflow context containing the task instance
    Rturns:
        str: Spotify access token
    """
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

# Function to fetch data from Spotify API
def fetch_spotify_data(table_name, **context):
    """
    Fetch track data from Spotify API
    Args:
        :context (dict): Airflow context containing the task instance
        :table_name (str): Target table name in Snowflake
    Returns:
        dct: Collected data dictionary from Spotify API
    """
    topic = f"{table_name}_topic"
    try:
        access_token = context['task_instance'].xcom_pull(
            task_ids='get_spotify_token', 
            key='spotify_token'
        )
        headers = {'Authorization': f'Bearer {access_token}'}
        logging.info(f"Successfully obtained Spotify access token")

        data_dict = {
            'spotify_playlist' : [], 
            'all_tracks': [],
            'all_artists': [],
            'all_albums': []
        }

        for playlist_id in SPOTIFY_CONFIG['playlist_ids']:
            url = f"{SPOTIFY_CONFIG['base_url']}playlists/{playlist_id}/tracks"
            logging.info(f"URL hitting: {str(url)}")
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                continue
            else:
                # Send to Kafka
                producer = SpotifyKafkaProducer(kafka_conn_id=KAFKA_CONN_ID)
                if table_name == "SPOTIFY_TRACKS":
                    for item in response.json()['items']:
                        track = item['track']
                        track_data = {
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'artist': track['artists'][0]['name'],
                            'album': track['album']['name'],
                            'duration_ms': track['duration_ms'],
                            'popularity': track['popularity'],
                            'processed_at': datetime.now().isoformat()
                        }
                        data_dict['spotify_playlist'].append(track_data)
                        producer.send_track_data(topic=topic, track_data=data_dict['spotify_playlist'])
                        logging.info(f"Successfully sent {len(data_dict['spotify_playlist'])} tracks to Kafka")

                elif table_name == "ARTISTS":
                    for item in response.json()['items']:
                        artist = item['track']['artists']
                        artist_data = {
                            'artist_id': artist['id'],
                            'artist_name': artist['name'],
                            'genres': artist['release_date'],
                            'followers': artist['total_tracks'],
                            'popularity': artist['album_type'],
                            'created_at': datetime.now().isoformat()
                        }
                        data_dict['all_artists'].append(artist_data)
                        producer.send_track_data(topic=topic, track_data= data_dict['all_artists'])
                        logging.info(f"Successfully sent {len(data_dict['all_artists'])} tracks to Kafka")
                
                elif table_name == "TRACKS":
                    for item in response.json()['items']:
                        track = item['track']
                        track_data = {
                            'track_id': track['id'],
                            'track_name': track['name'],
                            'track_url': track['external_urls']['spotify'],
                            'album_id': track['album']['id'],
                            'album_name': track['album']['name'],
                            'artist_id': track['artists'][0]['id'],
                            'artist_name': track['artists'][0]['name'],
                            'duration_ms': track['duration_ms'],
                            'popularity': track['popularity'],
                            'release_date': track['album']['release_date'],
                            'explicit': track['explicit'],
                            'added_at': item['added_at'],
                            'created_at': datetime.now().isoformat()
                        }
                        data_dict['all_tracks'].append(track_data)
                        producer.send_track_data(topic=topic, track_data=data_dict['all_tracks'])
                        logging.info(f"Successfully sent {len(data_dict['all_tracks'])} tracks to Kafka")
            
                elif table_name == "ALBUMS":
                    for item in response.json()['items']:
                        album = item['track']['album']
                        album_data = {
                            'album_id': album['id'],
                            'album_name': album['name'],
                            'release_date': album['release_date'],
                            'total_tracks': album['total_tracks'],
                            'album_type': album['album_type'],
                            'artist_id': album['artists'][0]['id'],
                            'created_at': datetime.now().isoformat()
                        }
                        data_dict['all_albums'].append(album_data)
                        producer.send_track_data(topic=topic, track_data=data_dict['all_albums'])
                        logging.info(f"Successfully sent {len(data_dict['all_albums'])} tracks to Kafka")

            return data_dict
        
    except Exception as e:
        logging.error(f"Error in fetch_spotify_data: {str(e)}")
        return None

# Function to load data into Snowflake
def load_to_snowflake(table_name, **context):
    """Load tracks data to Snowflake
    Args:
        :context (dict): Airflow context
        :table_name (str): Target table name in Snowflake
    Returns:
        int: Number of records loaded to Snowflake"""
    try:        
        consumer = SpotifyKafkaConsumer(kafka_conn_id=KAFKA_CONN_ID)
        topic = f"{table_name}_topic"
        tracks_batch = consumer.consume_messages(topic=topic, batch_size=100)
        if not tracks_batch:
            logging.warning("No tracks data to load")
            return 0
            
        snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONFIG['conn_id'])
        logging.info(f"Successfully obtained Snowflake Hook")
        
        # Prepare batch insert query
        if table_name == "SPOTIFY_TRACKS":
            insert_query = f"""
            INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Convert tracks to tuple format for insertion
            records = [(
                track['track_id'],
                track['track_name'],
                track['artist'],
                track['album'],
                track['duration_ms'],
                track['popularity'],
                track['processed_at']
            ) for track in tracks_batch]
            
            # Execute batch insert
            snowflake_hook.run(
                insert_query,
                parameters=records
            )

            logging.info(f"Successfully loaded {len(records)} records to Snowflake")
        
        elif table_name == "ARTISTS":
            insert_query = f"""
            INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s)
            """

            # Convert tracks to tuple format for insertion
            records = [(
                track['artist_id'],
                track['artist_name'],
                track['genres'],
                track['followers'],
                track['popularity'],
                track['created_at']
            ) for track in tracks_batch]
            
            # Execute batch insert
            snowflake_hook.run(
                insert_query,
                parameters=records
            )

            logging.info(f"Successfully loaded {len(records)} records to Snowflake")
        
        elif table_name == "TRACKS":
            insert_query = f"""
            INSERT INTO {table_name}  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            # Convert tracks to tuple format for insertion
            records = [(
                track['track_id'],
                track['track_name'],
                track['track_url'],
                track['album_id'],
                track['album_name'],
                track['artist_id'],
                track['artist_name'],
                track['duration_ms'],
                track['popularity'],
                track['release_date'],
                track['explicit'],
                track['added_at'],
                track['created_at']
            ) for track in tracks_batch]
            
            # Execute batch insert
            snowflake_hook.run(
                insert_query,
                parameters=records
            )

            logging.info(f"Successfully loaded {len(records)} records to Snowflake")

        elif table_name == "ALBUMS":
            insert_query = f"""
            INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s)
            """

            # Convert tracks to tuple format for insertion
            records = [(
                track['album_id'],
                track['album_name'],
                track['release_date'],
                track['total_tracks'],
                track['album_type'],
                track['artist_id'],
                track['created_at']
            ) for track in tracks_batch]
            
            # Execute batch insert
            snowflake_hook.run(
                insert_query,
                parameters=records
            )

            logging.info(f"Successfully loaded {len(records)} records to Snowflake")

    except Exception as e:
        logging.error(f"Error in load_to_snowflake: {str(e)}")
        raise

# DAG definitions
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
    
    produce_tasks = []
    consume_tasks = []

    for table_name in SNOWFLAKE_CONFIG['table']:
        get_token_task = PythonOperator(
            task_id='get_spotify_token',
            python_callable=get_spotify_token
        )
        
        fetch_data_task = PythonOperator(
            task_id='fetch_spotify_data',
            python_callable=fetch_spotify_data,
            op_args=[table_name]
        )
        produce_tasks.append(fetch_data_task)
        
        load_snowflake_task = PythonOperator(
            task_id='load_to_snowflake',
            python_callable=load_to_snowflake,
            op_args=[table_name]
        )
        consume_tasks.append(load_snowflake_task)

        # Set task dependencies
        get_token_task >> fetch_data_task >> load_snowflake_task