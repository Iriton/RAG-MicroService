# domain/score_calculator.py

from typing import List, Dict


class ScoreCalculator:
    def __init__(self):
        pass

    def normalize_score(self, similarity: float) -> float:
        """
        Milvus 유사도 점수(0~1 또는 거리값) → 0~100 점수로 정규화
        유사도가 클수록 높은 점수로 변환되도록 설정
        """
        # 유사도: Cosine 유사도 기준이라고 가정 (1에 가까울수록 유사)
        return round(similarity * 100, 2)

    def calculate_single_score(self, factor: str, similarity: float, evaluation: str) -> Dict:
        """
        유사도 점수 → 정규화 후, 긍/부정에 따라 보정 점수로 변환
        """
        base_score = self.normalize_score(similarity)

        # 긍정/부정 보정: 기본값 50에서 ±비율만큼 이동 (예: 60 or 40)
        if evaluation == "긍정":
            adjusted_score = min(100.0, base_score + 10)
        elif evaluation == "부정":
            adjusted_score = max(0.0, base_score - 10)
        else:
            adjusted_score = base_score  # 평가값이 없을 경우

        return {
            "factor": factor,
            "raw_score": base_score,
            "adjusted_score": adjusted_score,
            "evaluation": evaluation
        }

    def calculate_average(self, session_scores: List[Dict]) -> Dict[str, float]:
        """
        세션에 누적된 점수들을 평균 내어 최종 성향 점수 반환
        """
        factor_map: Dict[str, List[float]] = {}

        for entry in session_scores:
            factor = entry["factor"]
            score = entry["adjusted_score"]
            factor_map.setdefault(factor, []).append(score)

        final_scores = {}
        for factor, scores in factor_map.items():
            avg = sum(scores) / len(scores)
            final_scores[factor] = round(avg, 2)

        return final_scores
