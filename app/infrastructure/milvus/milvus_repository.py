# infrastructure/milvus/milvus_repository.py

import os
from pymilvus import MilvusClient, AnnSearchRequest, WeightedRanker
from app.domain.embedding_model import BGEM3FlagModel


class MilvusHybridSearcher:
    def __init__(self):
        self.collection_name = "cornsoup_collection"
        self.client = MilvusClient(uri=os.getenv("MILVUS_URI"), token="root:Milvus")

        if self.collection_name in self.client.list_collections():
            self.client.load_collection(self.collection_name)
        else:
            raise Exception(f"Milvus 컬렉션 '{self.collection_name}'을 찾을 수 없습니다.")

        # BM25 (sparse), Dense 임베딩 혼합 → Weighted Ranker 사용
        self.ranker = WeightedRanker(0.3, 0.7)  # sparse 0.3, dense 0.7 가중치

    def hybrid_search(self, query_text: str, embedding_model: BGEM3FlagModel):
        # Dense 임베딩 생성
        query_dense = embedding_model.encode([query_text], normalize_embeddings=True).tolist()

        # BM25용 검색 요청
        bm25_search_request = AnnSearchRequest(
            data=[query_text],
            anns_field="sparse",
            param={"metric_type": "BM25"},
            limit=5
        )

        # Dense 검색 요청
        knn_search_request = AnnSearchRequest(
            data=query_dense,
            anns_field="dense",
            param={"metric_type": "L2", "params": {"nprobe": 10}},
            limit=5
        )

        reqs = [bm25_search_request, knn_search_request]

        # 하이브리드 검색 실행
        results = self.client.hybrid_search(
            collection_name=self.collection_name,
            reqs=reqs,
            ranker=self.ranker,
            limit=5,
            output_fields=["sentence", "factor", "evaluation"]
        )

        return results[0]  # 하나의 query에 대해 나온 top-k 결과 리스트
