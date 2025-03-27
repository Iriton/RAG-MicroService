# infrastructure/milvus/milvus_config.py

from pymilvus import MilvusClient
from infrastructure.config import get_env

def get_milvus_client() -> MilvusClient:
    return MilvusClient(
        uri=get_env("MILVUS_URI"),
        token=get_env("MILVUS_TOKEN")
    )
