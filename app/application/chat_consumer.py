# application/chat_output_consumer.py

import logging
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer

logger = logging.getLogger(__name__)

class ChatOutputMessageConsumer:
    """
    Kafka 'chat_output' 토픽에서 메시지를 처리
    - type == "done"일 때만 Big5 계산 트리거
    - type 없으면 무시 (프론트 LLM 응답)
    """
    def __init__(self):
        self.rag_service = RAGService()
        self.producer = KafkaScoreProducer()

    def handle_message(self, msg: dict):
        try:
            member_id = msg.get("memberId")
            msg_type = msg.get("type")
            timestamp = msg.get("timestamp")

            if not member_id:
                logger.warning("[ChatOutputConsumer] memberId 없음. 메시지 무시: %s", msg)
                return

            if msg_type == "done":
                logger.info(f"[ChatOutput] 'done' 수신: memberId={member_id}")
                final_scores = self.rag_service.process_done_message(member_id)
                self.producer.send_final_scores(member_id, final_scores, timestamp)
                logger.info(f"[ChatOutput] Big5 계산 및 발송 완료: memberId={member_id}")
            else:
                logger.debug(f"[ChatOutput] type 없음 또는 처리 필요 없음: {msg_type}, memberId={member_id}")
                # type 없으면 무시

        except Exception as e:
            logger.error(f"[ChatOutputConsumer Error] 처리 중 오류: {e}, 메시지: {msg}", exc_info=True)