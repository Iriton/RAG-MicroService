# infrastructure/kafka/producer.py

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
from app.infrastructure.config import get_env

logger = logging.getLogger(__name__)

class KafkaScoreProducer:
    def __init__(self):
        self.big5_topic = get_env("KAFKA_BIG5_TOPIC")
        self.chat_score_topic = get_env("KAFKA_SCORE_TOPIC")
        self.producer = KafkaProducer(
            bootstrap_servers=get_env("KAFKA_BOOTSTRAP_SERVERS"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def send_partial_scores(self, memberId: str, partial_scores: dict, timestamp: str = None):
        """
        SCORE_TOPIC: 중간 점수 전송
        """
        if not timestamp:
            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        message = {
            "type": "response_score",
            "memberId": memberId,
            "scores": partial_scores,
            "timestamp": timestamp
        }

        try:
            self.producer.send(self.chat_score_topic, value=message)
            self.producer.flush()
            logger.info(f"[Kafka] 중간 점수 전송 완료: memberId={memberId}, timestamp={timestamp}")
        except KafkaError as e:
            logger.warning(f"[Kafka Error] 중간 점수 전송 실패: memberId={memberId}, error={e}")
        except Exception as e:
            logger.error(f"[Kafka Error] 예기치 못한 오류 발생: memberId={memberId}, error={e}")


    def send_final_scores(self, memberId: str, scores: dict, timestamp: str = None):
        """
        BIG5_TOPIC: 최종 점수 전송
        """
        if not timestamp:
            timestamp = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

        message = {
            "memberId": memberId,
            "scores": scores,
            "timestamp": timestamp
        }

        try:
            self.producer.send(self.big5_topic, value=message)
            self.producer.flush()
            logger.info(f"[Kafka] 최종 점수 전송 완료: memberId={memberId}, timestamp={timestamp}")
        except KafkaError as e:
            logger.warning(f"[Kafka Error] 점수 전송 실패: memberId={memberId}, error={e}")
        except Exception as e:
            logger.error(f"[Kafka Error] 예기치 못한 오류 발생: memberId={memberId}, error={e}")
