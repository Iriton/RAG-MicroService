import sys
import logging
from kafka import KafkaConsumer

# 로그 설정
root = logging.getLogger()
root.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
root.addHandler(handler)

# Kafka Consumer 설정
consumer = KafkaConsumer(
    "chat_input_topic",
    bootstrap_servers=["localhost:10000", "localhost:10001", "localhost:10002"],
    group_id="test-group",
    auto_offset_reset="earliest",
    api_version=(0, 10),
)

# 메시지 수신
logging.info("🎧 Listening for messages...")

for msg in consumer:
    message = msg.value.decode('utf-8')
    logging.info(f"📥 Received: {message}")
