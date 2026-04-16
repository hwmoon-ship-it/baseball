# config.py
DB_CONFIG = {
    'host': '34.85.127.241',
    'user': 'hiball-developer',
    'password': 'hiball##dev',
    'database': 'summaring_giants',
    'port': 3306,
    'charset': 'utf8mb4'
}

# 주자 상태 매핑 (0~7 -> 시스템용 표기)
BASE_MAPPING = {0: 0, 1: 1, 2: 2, 3: 12, 4: 3, 5: 13, 6: 23, 7: 123}
INV_BASE_MAPPING = {v: k for k, v in BASE_MAPPING.items()}
LABEL_MAP = {0: '_ _ _', 1: '1 _ _', 2: '_ 2 _', 3: '1 2 _', 4: '_ _ 3', 5: '1 _ 3', 6: '_ 2 3', 7: '1 2 3'}