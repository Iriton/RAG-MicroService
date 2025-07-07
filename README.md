# RAG Microservice (Milvus + Kafka 기반 성격 분석기)

이 프로젝트는 실시간 채팅 메시지를 기반으로 Milvus 하이브리드 검색을 수행하여, 사용자 성향 점수를 Kafka를 통해 전송하는 RAG 마이크로서비스입니다.

---

## 기술 스택

- Python 3.11.0
- Milvus (벡터 DB)
- Kafka (비동기 메시지 브로커)
- HuggingFace Transformers (BGE-M3 모델)
- Sentence Embedding + Hybrid Search (Dense + Sparse)

## 동작 플로우
1. Kafka Consumer로 작동하며 입력 문장 대기
2. 입력 문장 BGE-M3 Embedding
3. Milvus Hybrid Search
4. 상위 문장 k개의 유사도 점수 스케일링
  3-1. 긍정 문장과의 유사도가 높을수록 100점에 가까워지게
  3-2. 부정 문장과의 유사도가 높을수록 0점에 가까워지게
5. 대화 간 성향 점수 누적
6. Prompt에서 수집된 성향 전송 요청 시, 중간 점수 전달
7. 대화 종료 트리거 받을 시, 최종 성향 점수 전달
