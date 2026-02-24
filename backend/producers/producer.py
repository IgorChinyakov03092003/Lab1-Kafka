import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from utils.logger import logger
from utils.config import BOOTSTRAP_SERVERS


class BaseProducer:
    def __init__(self):
        self.producer = None

        retries = 20
        while retries > 0:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    acks='all',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: str(k).encode('utf-8') if k else None
                )
                logger.info(f"Producer connected to {BOOTSTRAP_SERVERS}")
                break
            except NoBrokersAvailable:
                logger.warning(f"Kafka brokers not available yet. Retrying in 5 seconds... ({retries} attempts left)")
                time.sleep(5)
                retries -= 1

        if not self.producer:
            raise ConnectionError("Failed to connect to Kafka after multiple retries.")

    def send_message(self, topic: str, value: dict, key: str = None):
        self.producer.send(topic, value=value, key=key)
        self.producer.flush()

    def close(self):
        if self.producer:
            self.producer.close()
