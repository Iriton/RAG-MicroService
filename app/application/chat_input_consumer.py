# application/chat_input_consumer.py

import logging
from app.application.rag_service import RAGService

logger = logging.getLogger(__name__)

class ChatInputConsumer:
    def __init__(self, rag_service: RAGService):
        self.rag_service = rag_service
        self.connected_sessions = set()

    def handle_message(self, msg: dict):
        try:
            member_id = msg.get("memberId")
            msg_type = msg.get("type", "chat")
            message = msg.get("message")

            if not member_id:
                logger.warning("[ChatInput] memberId 없음. 메시지 무시: %s", msg)
                return

            if member_id not in self.connected_sessions:
                self.connected_sessions.add(member_id)
                logger.info(f"[ChatInput] 세션 연결됨: memberId={member_id}")

            if msg_type == "chat" and message:
                logger.info(f"[ChatInput] 입력 수신: memberId={member_id}, message={message}")
                self.rag_service.process_active_message(member_id, message)
            else:
                logger.debug(f"[ChatInput] type 무시됨: {msg_type}, memberId={member_id}")

        except Exception as e:
            logger.error(f"[ChatInput] 처리 중 오류: {e}, 메시지: {msg}", exc_info=True)
