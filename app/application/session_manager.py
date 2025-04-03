# application/session_manager.py

from collections import defaultdict
from typing import Dict, List
from app.domain.score_calculator import ScoreCalculator
import uuid

class SessionManager:
    """
    사용자별로 세션을 생성하고 점수를 누적하며, 세션 종료 시 평균 점수를 계산하는 클래스
    """
    def __init__(self):
        self.session_scores: Dict[str, List[Dict]] = defaultdict(list)  # 사용자별 점수 누적
        self.user_sessions: Dict[str, str] = {}  # 사용자별 세션 관리
        self.calculator = ScoreCalculator()

    def get_or_create_session(self, user_id: str) -> str:
        """
        사용자별로 세션을 새로 생성하거나 기존 세션을 반환
        - user_id별로 고유한 session_id를 할당
        """
        if user_id not in self.user_sessions:
            # 고유한 세션 ID 생성
            session_id = str(uuid.uuid4())
            self.user_sessions[user_id] = session_id
        else:
            session_id = self.user_sessions[user_id]
        return session_id

    def add_score(self, user_id: str, score_entry: Dict):
        """
        사용자별로 점수 항목을 누적
        """
        session_id = self.get_or_create_session(user_id)
        self.session_scores[session_id].append(score_entry)

    def calculate_final_scores(self, user_id: str) -> Dict[str, float]:
        """
        해당 user_id의 세션 점수를 평균 내어 반환 후, 세션 정보 초기화
        """
        session_id = self.user_sessions.get(user_id)
        if not session_id or session_id not in self.session_scores:
            return {}

        scores = self.session_scores[session_id]
        final_result = self.calculator.calculate_average(scores)

        # 세션 데이터 초기화
        del self.session_scores[session_id]
        del self.user_sessions[user_id]

        return final_result