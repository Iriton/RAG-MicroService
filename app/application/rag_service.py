# application/rag_service.py

from app.application.session_manager import SessionManager
from app.domain.embedding_model import BGEM3FlagModel
from app.domain.score_calculator import ScoreCalculator
from app.infrastructure.milvus.milvus_repository import MilvusHybridSearcher
import logging

logger = logging.getLogger(__name__)

class RAGService:
    def __init__(self, session_manager=None):
        self.session_manager = session_manager or SessionManager()
        self.embedding_model = BGEM3FlagModel("BAAI/bge-m3")
        self.milvus = MilvusHybridSearcher()
        self.calculator = ScoreCalculator()

    def process_active_message(self, memberId: str, text: str):
        search_results = self.milvus.hybrid_search(text, self.embedding_model)
        logger.info(f"[RAG] 검색 결과 {len(search_results)}개, 입력: {text}")

        for hit in search_results:
            entity = hit.get("entity", {})
            similarity = 1 - hit.get("distance", 1.0)
            factor = entity.get("factor")
            evaluation = entity.get("evaluation")

            if factor:
                score_entry = self.calculator.calculate_single_score(factor, similarity, evaluation)
                logger.info(f"[RAG] score_entry 추가: {score_entry}")
                self.session_manager.add_score(memberId, score_entry)
            else:
                logger.warning(f"[RAG] factor 누락 - 무시됨: {entity}")


    def process_done_message(self, memberId: str) -> dict:
        """
        세션 종료: 누적 점수 → 평균 점수 계산
        """
        final_scores = self.session_manager.calculate_final_scores(memberId)
        return final_scores
