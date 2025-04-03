# infrastructure/kafka/consumer_config.py

from kafka import KafkaConsumer
from app.infrastructure.config import get_env

def create_consumer(topic: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=get_env("KAFKA_BOOTSTRAP_SERVERS"),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8')
    )
