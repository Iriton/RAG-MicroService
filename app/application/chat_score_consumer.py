# application/chat_score_consumer.py

import logging
from app.application.rag_service import RAGService
from app.infrastructure.kafka.producer import KafkaScoreProducer

logger = logging.getLogger(__name__)

class ChatScoreConsumer:
    def __init__(self, rag_service: RAGService, producer: KafkaScoreProducer):
        self.rag_service = rag_service
        self.producer = producer

    def handle_message(self, msg: dict):
        try:
            member_id = msg.get("memberId")
            msg_type = msg.get("type")
            timestamp = msg.get("timestamp")

            if not member_id:
                logger.warning("[ChatScore] memberId 없음. 메시지 무시: %s", msg)
                return

            if msg_type == "done":
                # 대화 종료: 최종 Big5 점수 계산 및 전송
                final_scores = self.rag_service.process_done_message(member_id)
                self.producer.send_final_scores(member_id, final_scores, timestamp)
                logger.info(f"[ChatScore] Big5 계산 및 전송 완료: memberId={member_id}")
            elif msg_type == "request_score":
                # Prompt가 점수 요청할 경우
                partial_scores = self.rag_service.process_partial_score_request(member_id)
                self.producer.send_partial_scores(member_id, partial_scores, timestamp)
                logger.info(f"[ChatScore] 중간 점수 전송 완료: memberId={member_id}")
            else:
                logger.debug(f"[ChatScore] 처리할 수 없는 type: {msg_type}, memberId={member_id}")

        except Exception as e:
            logger.error(f"[ChatScore] 처리 중 오류: {e}, 메시지: {msg}", exc_info=True)
