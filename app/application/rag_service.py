# application/rag_service.py

from app.application.session_manager import SessionManager
from app.domain.embedding_model import BGEM3FlagModel
from app.domain.score_calculator import ScoreCalculator
from app.infrastructure.milvus.milvus_repository import MilvusHybridSearcher

class RAGService:
    def __init__(self):
        self.session_manager = SessionManager()
        self.embedding_model = BGEM3FlagModel("BAAI/bge-m3")
        self.milvus = MilvusHybridSearcher()
        self.calculator = ScoreCalculator()

    def process_active_message(self, user_id: str, text: str):
        """
        실시간 처리 흐름:
        - 임베딩 생성 → 하이브리드 검색 → 점수 정규화 및 요인 추출 → 세션에 저장
        """
        session_id = self.session_manager.get_or_create_session(user_id)
        print(f"[Kafka] 세션 연결: user_id={user_id}, session_id={session_id}")

        search_results = self.milvus.hybrid_search(text, self.embedding_model)

        for hit in search_results:
            entity = hit.get("entity", {})
            similarity = 1 - hit.get("distance", 1.0)  # L2 distance → 유사도 환산
            factor = entity.get("factor")
            evaluation = entity.get("evaluation")  # 긍정/부정

            if factor:
                score_entry = self.calculator.calculate_single_score(factor, similarity, evaluation)
                self.session_manager.add_score(user_id, score_entry)

    def process_done_message(self, user_id: str) -> dict:
        """
        세션 종료: 누적 점수 → 평균 점수 계산
        """
        final_scores = self.session_manager.calculate_final_scores(user_id)
        return final_scores
