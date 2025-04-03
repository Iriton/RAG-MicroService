# infrastructure/config.py

import os
from dotenv import load_dotenv

# 루트 파일 기준으로 .env 명시적으로 로딩
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
ENV_PATH = os.path.join(BASE_DIR, ".env")
load_dotenv(dotenv_path=ENV_PATH)

def get_env(key: str, default: str = None):
    return os.getenv(key, default)
