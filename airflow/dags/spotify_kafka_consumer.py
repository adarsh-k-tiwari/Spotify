# Kafka Consumer
import json
from kafka import KafkaConsumer

class SpotifyKafkaConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(
            bootstrap_servers='localhost:9092',
            group_id='spotify_consumer_group',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def consume_messages(self, topic, batch_size=100):
        """
        Consumes messages from a specified Kafka topic in batches.
        :param topic: The Kafka topic to consume data from.
        :param batch_size: The maximum number of messages to consume in one batch.
        :return: List of consumed messages.
        """
        try:
            self.consumer.subscribe([topic])
            
            messages = []
            while len(messages) < batch_size:
                for message in self.consumer:
                    messages.append(message.value)
                    if len(messages) >= batch_size:
                        break

            print(f"Successfully consumed {len(messages)} messages from topic {topic}")
            return messages
        except Exception as e:
            print(f"Error consuming messages: {str(e)}")
            return []
