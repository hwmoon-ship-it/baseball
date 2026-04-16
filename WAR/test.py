import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

# DB 연결
db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(db_url)

print("🔍 조인(JOIN) 키값 정밀 진단 테스트\n")

# 1. 수비 테이블의 브릿지 키 확인
query_fr = """
    SELECT game_info_id, fr_inning_pitch_seq, hit_result_name 
    FROM hiball_fielding_record 
    WHERE season = 2025 
      AND batted_ball_type_code IN (6702, 6703, '6702', '6703')
    LIMIT 3
"""
df_fr = pd.read_sql(query_fr, engine)
print("=== ⚾ 수비 테이블 (Fielding) 샘플 ===")
print(df_fr.to_string(index=False))

# 2. 트랙맨 테이블의 브릿지 키 확인
query_tr = """
    SELECT game_info_id, inning_pitch_seq, inning, top_bottom, pa_of_inning, pitch_of_pa 
    FROM hiball_trackman_record 
    WHERE exit_speed > 0
    LIMIT 3
"""
df_tr = pd.read_sql(query_tr, engine)
print("\n=== 📡 트랙맨 테이블 (Trackman) 샘플 ===")
print(df_tr.to_string(index=False))

engine.dispose()
print("\n테스트가 완료되었습니다.")