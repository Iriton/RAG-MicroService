# app/main.py

import logging
import time
import json
import threading
from kafka import KafkaConsumer
from app.application.chat_input_consumer import ChatMessageConsumer as InputHandler
from app.application.chat_output_consumer import ChatMessageConsumer as OutputHandler
from app.infrastructure.config import get_env

# 전역 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def safe_json_loads(v):
    if v:
        try:
            return json.loads(v.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.error(f"[Kafka] JSON 디코딩 실패: {e}")
            return None
    return None

def create_consumer(topic: str, bootstrap_servers: str) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=get_env("KAFKA_GROUP_ID"),
        auto_offset_reset="earliest",
        value_deserializer=lambda v: safe_json_loads(v)
    )

def consume_messages(consumer: KafkaConsumer, handler, topic_name: str):
    logger.info(f"[Kafka] '{topic_name}' Consumer 대기 중...")

    for message in consumer:
        start_time = time.time()
        msg = message.value

        if not msg:
            logger.warning(f"[Kafka] 빈 메시지 수신 (partition={message.partition}, offset={message.offset})")
            continue

        handler.handle_message(msg)

        member_id = msg.get("memberId", "unknown")
        duration = time.time() - start_time
        logger.info(f"[{topic_name}] 세션 {member_id} 처리 시간: {duration:.4f}초")

def main():
    bootstrap_servers = get_env("KAFKA_BOOTSTRAP_SERVERS")

    # chat_input consumer
    input_topic = get_env("KAFKA_INPUT_TOPIC")
    input_consumer = create_consumer(input_topic, bootstrap_servers)
    input_handler = InputHandler()
    input_thread = threading.Thread(target=consume_messages, args=(input_consumer, input_handler, input_topic))

    # chat_output consumer
    output_topic = get_env("KAFKA_OUTPUT_TOPIC")
    output_consumer = create_consumer(output_topic, bootstrap_servers)
    output_handler = OutputHandler()
    output_thread = threading.Thread(target=consume_messages, args=(output_consumer, output_handler, output_topic))

    # 실행
    input_thread.start()
    output_thread.start()

    input_thread.join()
    output_thread.join()

if __name__ == "__main__":
    main()