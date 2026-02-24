import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from utils.logger import logger
from utils.config import BOOTSTRAP_SERVERS

class BaseConsumer:
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.consumer = None

        retries = 20
        while retries > 0:
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    group_id=group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    metadata_max_age_ms=10000,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                )
                logger.info(f"Consumer ({group_id}) connected to {topic}")
                break
            except NoBrokersAvailable:
                logger.warning(f"Kafka brokers not available yet. Retrying in 5 seconds... ({retries} attempts left)")
                time.sleep(5)
                retries -= 1

        if not self.consumer:
            raise ConnectionError("Failed to connect to Kafka after multiple retries.")

    def process_message(self, message: dict):
        raise NotImplementedError("Метод должен быть переопределен в наследнике")

    def start_consuming(self):
        logger.info(f"Started consuming from {self.topic}...")
        while True:
            try:
                records = self.consumer.poll(timeout_ms=1000)
                for tp, messages in records.items():
                    for message in messages:
                        self.process_message(message.value)
            except Exception as e:
                logger.warning(f"Ошибка соединения с кластером (перебалансировка): {e}. Ожидание 3 сек...")
                time.sleep(3)
