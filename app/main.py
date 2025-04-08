import logging
import time
import json
from kafka import KafkaConsumer
from app.application.chat_consumer import ChatMessageConsumer
from app.infrastructure.config import get_env

# 전역 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def create_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    """Kafka Consumer 생성"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=get_env("KAFKA_GROUP_ID"),
        auto_offset_reset="earliest",
        value_deserializer=lambda v: safe_json_loads(v)
    )

def safe_json_loads(v):
    """안전하게 JSON 디코딩 처리"""
    if v:
        try:
            return json.loads(v.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"[Kafka] JSON 디코딩 실패: {e}")
            return None
    return None

def consume_messages(consumer: KafkaConsumer, handler: ChatMessageConsumer):
    """Kafka 메시지 수신 후 처리"""
    logger.info("[Kafka] Consumer 대기 중...")

    for message in consumer:
        start_time = time.time()
        msg = message.value

        if not msg:
            logger.warning(f"[Kafka] 빈 메시지 수신 (partition={message.partition}, offset={message.offset})")
            continue

        handler.handle_message(msg)

        memberId = msg.get("memberId", "unknown")
        duration = time.time() - start_time
        logger.info(f"세션 {memberId} 처리 시간: {duration:.4f}초")

def main():
    """애플리케이션 메인 실행"""
    topic = get_env("KAFKA_INPUT_TOPIC")
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")

    consumer = create_consumer(topic, bootstrap_servers)
    handler = ChatMessageConsumer()

    consume_messages(consumer, handler)

if __name__ == "__main__":
    main()