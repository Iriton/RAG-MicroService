# # app/main.py

# import time
# from infrastructure.kafka.consumer_config import create_consumer
# from application.chat_consumer import ChatMessageConsumer

# TOPIC_NAME = "chat_input_topic"


# def main():
#     print("[RAG Microservice] Kafka Consumer 시작 중...")
#     consumer = create_consumer(topic=TOPIC_NAME)
#     processor = ChatMessageConsumer()

#     for msg in consumer:
#         print(f"\n[Kafka 수신] {msg.value}")
#         processor.handle_message(msg.value)
#         time.sleep(0.1)  # 너무 빠른 처리 방지


# if __name__ == "__main__":
#     main()


from application.rag_service import RAGService
import datetime

if __name__ == "__main__":
    print("[RAG Microservice] Milvus 테스트 시작")

    # ✅ 테스트용 하드코딩 메시지
    user_id = "test_user"
    session_id = "test_session"
    text = "나는 도전을 즐기는 사람이야"
    timestamp = datetime.datetime.utcnow().isoformat()

    # ✅ 서비스 인스턴스 생성
    rag = RAGService()

    # ✅ 실시간 메시지 처리 (embedding → hybrid search → score 저장)
    rag.process_active_message(user_id, session_id, text)

    # ✅ 세션 종료 후 평균 점수 계산
    final_scores = rag.process_done_message(user_id, session_id)

    print(f"\n📊 최종 점수 계산 결과 (session_id={session_id}):")
    print(final_scores)
    print("\n[RAG Microservice] Milvus 테스트 종료")