import pandas as pd
import mysql.connector
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def calculate_count_run_value(season):
    conn = mysql.connector.connect(**DB_CONFIG)

    # 1. 볼카운트 전이 데이터 추출
    # 주의: 타석 종료(hit_result_code != 0) 건은 제외하고 '카운트만 변한' 투구 위주로 집계하여
    # 순수하게 볼/스트라이크의 가치를 측정합니다.
    query = f"""
        SELECT 
            before_strike_count as s, 
            before_ball_count as b,
            strike_count as next_s,
            ball_count as next_b,
            COUNT(*) as freq
        FROM hiball_game_record
        WHERE season = {season} AND game_type = 4201
          AND hit_result_code = 0  -- 타석 미종료건 (카운트 전이)
          AND before_strike_count <= 2 AND before_ball_count <= 3
        GROUP BY 1, 2, 3, 4
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty:
        return pd.DataFrame()

    # 2. 288 Matrix (또는 12개 카운트 Matrix) 로드
    # 이미 계산된 pitch_re_matrix_288.csv가 있다면 이를 활용하여
    # 각 카운트의 RE(득점 기대치) 평균값을 가져옵니다.
    try:
        re288 = pd.read_csv('pitch_re_matrix_288.csv')
        # 상황(OUT/STATE)을 무시하고 카운트별 평균 COUNT_VALUE를 사용
        count_val_map = re288.groupby(['STRIKE', 'BALL'])['COUNT_VALUE'].mean().to_dict()
    except:
        logging.error("pitch_re_matrix_288.csv 파일이 필요합니다.")
        return pd.DataFrame()

    results = []
    # 0-0부터 2-3까지 반복 (스트라이크-볼 순서)
    for s in range(3):
        for b in range(4):
            curr_val = count_val_map.get((s, b), 0.0)

            # 스트라이크가 되었을 때의 가치 변화 (s -> s+1)
            # 스트라이크가 늘어나면 투수에게 유리하므로 RE는 낮아짐 (수치는 음수 방향)
            val_if_strike = count_val_map.get((s + 1, b), 0.0) if s < 2 else -0.35  # 3스트라이크(아웃) 가정치
            strike_diff = val_if_strike - curr_val

            # 볼이 되었을 때의 가치 변화 (b -> b+1)
            # 볼이 늘어나면 타자에게 유리하므로 RE는 높아짐 (수치는 양수 방향)
            val_if_ball = count_val_map.get((s, b + 1), 0.0) if b < 3 else 0.35  # 4볼(출루) 가정치
            ball_diff = val_if_ball - curr_val

            results.append({
                'season': season,
                'count': f"{s}-{b}",
                'run_value': round(curr_val, 4),
                'ball': round(ball_diff, 4),
                'strike': round(strike_diff, 4),
                'ball_count': b,
                'strike_count': s
            })

    return pd.DataFrame(results)


if __name__ == "__main__":
    target_years = [2024, 2025]
    all_years_data = []

    for year in target_years:
        logging.info(f"{year} 시즌 볼카운트 Run Value 계산 중...")
        df_year = calculate_count_run_value(year)
        if not df_year.empty:
            all_years_data.append(df_year)
            print(f"\n[ {year} 시즌 볼카운트 Matrix ]")
            print(df_year[['season', 'count', 'run_value', 'ball', 'strike', 'ball_count', 'strike_count']])

    if all_years_data:
        final_df = pd.concat(all_years_data)
        final_df.to_csv("count_run_value_2024_2025.csv", index=False, encoding='utf-8-sig')