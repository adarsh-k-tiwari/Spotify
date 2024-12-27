# from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
import json
from kafka import KafkaConsumer

class SpotifyKafkaConsumer:
    def __init__(self, kafka_conn_id='kafka_default'):
        self.kafka_hook = KafkaConsumer(
            kafka_conn_id=kafka_conn_id,
            config={
                'bootstrap.servers': 'localhost:9092',
                'group.id': 'spotify_consumer_group',
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True
            }
        )
        self.topic = 'spotify-tracks'

    def consume_messages(self, batch_size=100):
        try:
            consumer = self.kafka_hook.get_consumer()
            consumer.subscribe([self.topic])
            
            messages = []
            while len(messages) < batch_size:
                msg = consumer.poll(timeout_ms=1000)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue
                    
                try:
                    value = json.loads(msg.value().decode('utf-8'))
                    messages.append(value)
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    continue
                    
            return messages
        except Exception as e:
            print(f"Error consuming messages: {str(e)}")
            return []
