# application/chat_input_consumer.py

import logging
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer

logger = logging.getLogger(__name__)

class ChatOutputConsumer:
    def __init__(self, rag_service: RAGService, producer: KafkaScoreProducer):
        self.rag_service = rag_service
        self.producer = producer

    def handle_message(self, msg: dict):
        try:
            member_id = msg.get("memberId")
            msg_type = msg.get("type")
            timestamp = msg.get("timestamp")

            if not member_id:
                logger.warning("[ChatOutput] memberId 없음. 메시지 무시: %s", msg)
                return

            if msg_type == "done":
                final_scores = self.rag_service.process_done_message(member_id)
                self.producer.send_final_scores(member_id, final_scores, timestamp)
                logger.info(f"[ChatOutput] Big5 계산 및 전송 완료: memberId={member_id}")
            else:
                logger.debug(f"[ChatOutput] type 없음 또는 무시됨: {msg_type}, memberId={member_id}")

        except Exception as e:
            logger.error(f"[ChatOutput] 처리 중 오류: {e}, 메시지: {msg}", exc_info=True)
