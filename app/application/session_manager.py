# application/session_manager.py

from collections import defaultdict
from typing import Dict, List
from domain.score_calculator import ScoreCalculator


class SessionManager:
    """
    세션별 점수 누적 및 종료 감지 후 평균 점수 계산
    """
    def __init__(self):
        self.session_scores: Dict[str, List[Dict]] = defaultdict(list)
        self.calculator = ScoreCalculator()

    def add_score(self, session_id: str, score_entry: Dict):
        """
        세션별로 점수 항목을 누적
        - score_entry는 ScoreCalculator에서 나온 단일 문장 점수 결과
        """
        self.session_scores[session_id].append(score_entry)

    def calculate_final_scores(self, session_id: str) -> Dict[str, float]:
        """
        해당 세션의 누적 점수를 평균 내어 반환 후, 세션 정보는 초기화
        """
        if session_id not in self.session_scores:
            return {}

        scores = self.session_scores[session_id]
        final_result = self.calculator.calculate_average(scores)

        # 완료 후 세션 데이터 정리
        del self.session_scores[session_id]

        return final_result

    def get_session_data(self, session_id: str) -> List[Dict]:
        """
        디버깅용 - 해당 세션의 모든 점수 리스트 반환
        """
        return self.session_scores.get(session_id, [])
