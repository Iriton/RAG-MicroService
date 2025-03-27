# domain/embedding_model.py

import numpy as np
from transformers import AutoTokenizer, AutoModel
from typing import List


class BGEM3FlagModel:
    """
    BGE-M3 기반 임베딩 모델
    - 입력된 문장을 벡터로 변환
    """
    def __init__(self, model_name: str = "BAAI/bge-m3"):
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)

    def encode(self, texts: List[str], normalize_embeddings: bool = True) -> np.ndarray:
        """
        텍스트 리스트를 임베딩 벡터로 변환.
        L2 정규화 포함 (옵션)
        """
        inputs = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
        outputs = self.model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1).detach().numpy()

        if normalize_embeddings:
            embeddings = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)

        return embeddings