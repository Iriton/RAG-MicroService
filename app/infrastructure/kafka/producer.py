# infrastructure/kafka/producer.py

import os
import json
from kafka import KafkaProducer
from app.infrastructure.config import get_env

class KafkaScoreProducer:
    def __init__(self):
        self.topic = get_env("KAFKA_OUTPUT_TOPIC")
        self.producer = KafkaProducer(
            bootstrap_servers=get_env("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send_final_scores(self, user_id: str, scores: dict, timestamp: str):
        message = {
            "user_id": user_id,
            "scores": scores,
            "status": "done",
            "timestamp": timestamp
        }

        self.producer.send(self.topic, value=message)
        self.producer.flush()  # 즉시 전송
        print(f"[Kafka] 최종 점수 전송 완료: user_id={user_id}")