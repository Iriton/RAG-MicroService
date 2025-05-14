# app/main.py

import logging
import time
import json
from kafka import KafkaConsumer

from app.application.chat_input_consumer import ChatInputConsumer
from app.application.chat_output_consumer import ChatOutputConsumer
from app.application.session_manager import SessionManager
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer
from app.infrastructure.config import get_env

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def safe_json_loads(v):
    try:
        return json.loads(v.decode("utf-8"))
    except Exception as e:
        logger.error(f"[Kafka] JSON 파싱 실패: {e}")
        return None

def main():
    # 환경 변수에서 설정 값 가져오기
    topics = [get_env("KAFKA_INPUT_TOPIC"), get_env("KAFKA_OUTPUT_TOPIC")]
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")
    group_id = get_env("KAFKA_GROUP_ID")

    # KafkaConsumer 초기화
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=safe_json_loads
    )

    # 공용 인스턴스 생성
    shared_session_manager = SessionManager()
    shared_rag_service = RAGService(shared_session_manager)
    shared_producer = KafkaScoreProducer()

    # Consumer 핸들러 인스턴스 초기화 (공용 인스턴스 주입)
    input_handler = ChatInputConsumer(shared_rag_service)
    output_handler = ChatOutputConsumer(shared_rag_service, shared_producer)

    logger.info(f"[Kafka] topics 구독 시작: {topics}")

    # 메시지 루프
    for message in consumer:
        topic = message.topic
        msg = message.value
        member_id = msg.get("memberId", "unknown")
        start_time = time.time()

        if not msg:
            logger.warning(f"[Kafka] 빈 메시지: {message}")
            continue

        if topic == get_env("KAFKA_INPUT_TOPIC"):
            input_handler.handle_message(msg)
        elif topic == get_env("KAFKA_OUTPUT_TOPIC"):
            output_handler.handle_message(msg)
        else:
            logger.warning(f"[Kafka] 알 수 없는 토픽: {topic}")

        duration = time.time() - start_time

if __name__ == "__main__":
    main()