# application/chat_consumer.py

import logging
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer

logger = logging.getLogger(__name__)

class ChatMessageConsumer:
    """
    Kafka 메시지 수신 후 상태(type)에 따라 처리:
    - chat: 실시간 점수 계산 및 사용자 누적
    - done: 누적 점수 평균 계산 후 전송
    """
    def __init__(self):
        self.rag_service = RAGService()
        self.producer = KafkaScoreProducer()
        self.connected_sessions = set()

    def handle_message(self, msg: dict):
        """
        Kafka에서 받은 메시지를 처리
        """
        try:
            memberId = msg.get("memberId")
            msg_type = msg.get("type", "chat")
            timestamp = msg.get("timestamp")

            if not memberId:
                logger.warning("[Consumer Warning] memberId가 누락된 메시지입니다: %s", msg)
                return

            # 최초 세션 연결 로그
            if memberId not in self.connected_sessions:
                self.connected_sessions.add(memberId)
                logger.info(f"[Kafka] 세션 연결: memberId={memberId}")

            if msg_type == "chat":
                message = msg.get("message")
                logger.debug(f"[Kafka] 채팅 수신: memberId={memberId}, message={message}")
                self.rag_service.process_active_message(memberId, message)

            elif msg_type == "done":
                final_scores = self.rag_service.process_done_message(memberId)
                self.producer.send_final_scores(memberId, final_scores, timestamp)

            else:
                logger.warning(f"[Consumer Warning] 알 수 없는 메시지 타입: {msg_type}, memberId={memberId}")

        except Exception as e:
            logger.error(f"[Consumer Error] 메시지 처리 중 오류 발생: {e}, 원본 메시지: {msg}", exc_info=True)
