# RAG Microservice (Milvus + Kafka 기반 성격 분석기)

이 프로젝트는 실시간 채팅 메시지를 기반으로 Milvus 하이브리드 검색을 수행하여, 사용자 성향 점수를 Kafka를 통해 전송하는 RAG 마이크로서비스입니다.

---

## 기술 스택

- Python 3.10+
- Milvus (벡터 DB)
- Kafka (비동기 메시지 브로커)
- HuggingFace Transformers (BGE-M3 모델)
- Sentence Embedding + Hybrid Search (Dense + Sparse)
