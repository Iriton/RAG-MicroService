from kafka import KafkaProducer
import json
from datetime import datetime
import concurrent.futures

producer = KafkaProducer(
    bootstrap_servers=['localhost:10000'],  # Kafka 브로커 주소
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # JSON 직렬화
)

# 테스트 메시지 리스트
messages = [
    {
        "memberId": "u123",
        "message": "오늘 너무 피곤해",
        "type": "chat",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    },
    {
        "memberId": "u456",
        "message": "내일 시험인데 걱정돼",
        "type": "chat",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    },
    {
        "memberId": "u123",
        "message": "이제 다 끝났어!",
        "type": "done",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    },
    {
        "memberId": "u456",
        "message": "친구들이랑 놀고 싶다",
        "type": "done",
        "timestamp": datetime.utcnow().isoformat() + "Z"
    },
]

def send_message(msg):
    """메시지 전송 함수"""
    producer.send('chat_input_topic', msg)
    print(f" Sent: {msg}")

# 병렬 처리로 메시지 전송
with concurrent.futures.ThreadPoolExecutor() as executor:
    # 여러 메시지를 동시에 전송
    executor.map(send_message, messages)

# 버퍼 비우기 & 연결 닫기
producer.flush()
producer.close()
print("✅ Producer 종료")
