# application/chat_consumer.py

import json
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer


class ChatMessageConsumer:
    """
    Kafka 메시지 수신 후 상태(status)에 따라 처리:
    - active: 실시간 점수 계산 및 사용자 누적
    - done: 누적 점수 평균 계산 후 전송
    """
    def __init__(self):
        self.rag_service = RAGService()
        self.producer = KafkaScoreProducer()

    def handle_message(self, msg: str):
        """
        Kafka에서 받은 raw JSON 메시지를 처리
        """
        try:
            user_id = msg["user_id"]
            status = msg.get("status", "active")
            text = msg["message"]
            timestamp = msg.get("timestamp")

            if status == "active":
                self.rag_service.process_active_message(user_id, text)

            elif status == "done":
                final_scores = self.rag_service.process_done_message(user_id)
                self.producer.send_final_scores(user_id, final_scores, timestamp)

        except Exception as e:
            print(f"[Consumer Error] 메시지 처리 중 오류 발생: {e}")