# infrastructure/kafka/producer.py

import os
import json
from kafka import KafkaProducer


class KafkaScoreProducer:
    def __init__(self):
        self.topic = "chat_score_topic"
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send_final_scores(self, user_id: str, session_id: str, scores: dict, timestamp: str):
        message = {
            "user_id": user_id,
            "session_id": session_id,
            "scores": scores,
            "status": "done",
            "timestamp": timestamp
        }

        self.producer.send(self.topic, value=message)
        self.producer.flush()  # 즉시 전송
        print(f"[Kafka] 최종 점수 전송 완료: session_id={session_id}")
