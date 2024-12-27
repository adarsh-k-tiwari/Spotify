# from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
import json
from kafka import KafkaProducer

class SpotifyKafkaProducer:
    def __init__(self, kafka_conn_id='kafka_default'):
        self.kafka_hook = KafkaProducer(
            kafka_conn_id=kafka_conn_id,
            config={
                'bootstrap.servers': 'localhost:9092',
                'client.id': 'spotify_producer'
            }
        )
        self.topic = 'spotify-tracks'

    def send_track_data(self, track_data):
        try:
            self.kafka_hook.send(
                topic=self.topic,
                value=json.dumps(track_data).encode('utf-8')
            )
            return True
        except Exception as e:
            print(f"Error sending data to Kafka: {str(e)}")
            return False
