# app/main.py

import logging
import time
import json
from kafka import KafkaConsumer
from app.application.chat_consumer import ChatMessageConsumer
from app.infrastructure.config import get_env

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def create_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    """Kafka Consumer 생성"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=get_env("KAFKA_GROUP_ID"),
        auto_offset_reset="earliest",
        value_deserializer=lambda v: safe_json_loads(v)  # 수정된 부분
    )

def safe_json_loads(v):
    """안전하게 JSON 디코딩 처리"""
    if v:
        try:
            return json.loads(v.decode("utf-8"))
        except json.JSONDecodeError as e:
            logging.error(f"JSONDecodeError 발생: {e}")
            return None  # JSON 디코딩 실패 시 None 반환
    return None  # 빈 값이 들어오면 None 반환

def consume_messages(consumer: KafkaConsumer, handler: ChatMessageConsumer):
    """Kafka 메시지 수신 후 처리"""
    logging.info("Kafka Consumer 대기 중...")

    for message in consumer:
        # 세션 메시지 처리 시작 시간 기록
        start_time = time.time()

        json_value = message.value  # 이미 dict 객체이므로 json.loads() 필요 없음

        if not json_value:  # 빈 메시지 처리
            logging.warning(f"세션 {message.partition}-{message.offset}에서 빈 메시지를 수신했습니다. 무시합니다.")
            continue

        user_id = json_value.get("user_id", "unknown_session")
        logging.info(f"세션 {user_id} 메시지 수신: {json_value}")

        try:
            handler.handle_message(json_value)
        except Exception as e:
            logging.error(f"[Consumer Error] 세션 {user_id} 메시지 처리 중 오류 발생: {e}")

        # 세션 메시지 처리 종료 시간 기록 및 속도 로그
        end_time = time.time()
        processing_time = end_time - start_time
        logging.info(f"세션 {user_id} 처리 시간: {processing_time:.4f}초")

def main():
    """애플리케이션 메인 실행"""
    topic = get_env("KAFKA_INPUT_TOPIC")
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")

    # Kafka Consumer 생성
    consumer = create_consumer(topic, bootstrap_servers)
    handler = ChatMessageConsumer()

    # 메시지 소비 시작
    consume_messages(consumer, handler)

if __name__ == "__main__":
    main()
