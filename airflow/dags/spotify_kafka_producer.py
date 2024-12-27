# from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
import json
from kafka import KafkaProducer

class SpotifyKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            client_id='spotify_producer',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send_track_data(self, topic, track_data):
        """
        Sends track data to a specified Kafka topic.
        :param topic: The Kafka topic to which data should be sent.
        :param track_data: List of track data dictionaries.
        """
        try:
            for record in track_data:
                self.producer.send(topic, record)
            self.producer.flush()
            print(f"Successfully sent {len(track_data)} records to topic {topic}")
            return True
        except Exception as e:
            print(f"Error sending data to Kafka: {str(e)}")
            return False