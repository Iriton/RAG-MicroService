# application/session_manager.py

import logging
from collections import defaultdict
from typing import Dict, List
from app.domain.score_calculator import ScoreCalculator

logger = logging.getLogger(__name__)

class SessionManager:
    """
    사용자별로 세션을 생성하고 점수를 누적하며, 세션 종료 시 평균 점수를 계산하는 클래스
    """
    def __init__(self):
        self.session_scores: Dict[str, List[Dict]] = defaultdict(list)  # 사용자별 점수 누적
        self.member_sessions: Dict[str, str] = {}  # 사용자별 세션 관리
        self.calculator = ScoreCalculator()

    def get_or_create_session(self, memberId: str) -> str:
        """
        사용자별로 세션을 새로 생성하거나 기존 세션을 반환
        - memberId별로 고유한 sessionId를 할당
        """
        if memberId not in self.member_sessions:
            # 고유한 세션 ID 생성
            sessionId = memberId
            self.member_sessions[memberId] = sessionId
            logger.info(f"[세션 생성] memberId={memberId}")
        else:
            sessionId = self.member_sessions[memberId]
        return sessionId

    def add_score(self, memberId: str, score_entry: Dict):
        """
        사용자별로 점수 항목을 누적
        """
        sessionId = self.get_or_create_session(memberId)
        self.session_scores[sessionId].append(score_entry)

    def calculate_final_scores(self, memberId: str) -> Dict[str, float]:
        """
        해당 memberId의 세션 점수를 평균 내어 반환 후, 세션 정보 초기화
        """
        sessionId = self.member_sessions.get(memberId)
        if not sessionId or sessionId not in self.session_scores:
            logger.warning(f"[세션 종료 실패] memberId={memberId} → 세션 정보 없음")
            return {}

        scores = self.session_scores[sessionId]
        final_result = self.calculator.calculate_average(scores)

        # 세션 데이터 초기화
        del self.session_scores[sessionId]
        del self.member_sessions[memberId]

        logger.info(f"[세션 종료] memberId={memberId}, 최종 점수: {final_result}")
        return final_result