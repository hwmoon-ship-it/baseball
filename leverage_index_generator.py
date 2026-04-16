import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DB의 0~7 주자 상태를 WE 테이블의 포맷(0, 1, 2, 12, 3, 13, 23, 123)으로 매핑
DB_TO_WE_BASE = {0: 0, 1: 1, 2: 2, 3: 12, 4: 3, 5: 13, 6: 23, 7: 123}


def load_we_table(season):
    """타겟 연도에 맞는 WE 테이블을 자동으로 찾아서 로드합니다."""
    filepath = f"win_expectancy_table_{season}.csv"
    logging.info(f"[{season}] 누적 WE 테이블({filepath})을 로드합니다...")

    try:
        we_df = pd.read_csv(filepath)
    except FileNotFoundError:
        logging.error(f"{filepath} 파일을 찾을 수 없습니다. 먼저 we.py를 실행하여 {season}년 WE 테이블을 생성해주세요.")
        return None

    we_dict = {}
    for _, row in we_df.iterrows():
        inn = int(row['INNING'])
        tb = int(row['INNING_TB'])
        out = int(row['OUT_COUNT'])
        b = int(row['BASE_STATE'])

        # -5점부터 +5점까지의 승리 확률 맵핑
        for d in range(-5, 6):
            col_name = "TIE" if d == 0 else (f"UPPER_{d}" if d > 0 else f"UNDER_{abs(d)}")
            if col_name in row:
                we_dict[(inn, tb, out, b, d)] = float(row[col_name])

    return we_dict


def get_win_prob(we_dict, inn, tb, out, b_db, diff):
    """특정 상황의 기대 승리 확률을 반환합니다 (범위 초과 시 근사치 처리 포함)"""
    # 5점 차 이상은 5점 차 확률로 갈음 (WE 테이블 한계 방어)
    diff = max(-5, min(5, diff))

    # 9회를 초과하는 연장전의 경우 극단값 처리
    if inn > 9:
        return 1.0 if diff > 0 else (0.0 if diff < 0 else 0.5)

    b_we = DB_TO_WE_BASE.get(b_db, 0)

    if out < 3:
        return we_dict.get((inn, tb, out, b_we, diff), 0.5)
    else:
        # [공수 교대 로직] 3아웃 발생 시
        # 다음 이닝의 점수차는 공격팀 입장이 바뀌므로 부호가 반대(-diff)가 됩니다.
        next_diff = -diff
        next_tb = 1 if tb == 0 else 0
        next_inn = inn if tb == 0 else inn + 1

        if next_inn > 9:
            return 1.0 if diff > 0 else (0.0 if diff < 0 else 0.5)

        next_we = we_dict.get((next_inn, next_tb, 0, 0, next_diff), 0.5)
        # 현재 공격팀의 관점 = 1 - (공수 교대 후 다음 공격팀의 승리 확률)
        return 1.0 - next_we


def generate_theoretical_li(season):
    # 1. 해당 연도의 WE 테이블 로드
    we_dict = load_we_table(season)
    if not we_dict:
        return None

    logging.info(f"[{season}] DB에서 당해 연도 타격 환경(Transition Matrix) 추출 중...")
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    # 2. 당해 연도의 실제 타격 환경(전이 빈도) 추출
    query_trans = f"""
        SELECT before_out_count, before_runner_state, 
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced,
               COUNT(*) AS freq
        FROM hiball_game_record
        WHERE game_type = 4201 AND season = {season}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
          AND hit_result_name IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    df_trans = pd.read_sql(query_trans, engine)

    trans_probs = {}
    state_totals = {}
    for _, row in df_trans.iterrows():
        s = (int(row['before_out_count']), int(row['before_runner_state']))
        if s not in trans_probs:
            trans_probs[s] = []
            state_totals[s] = 0
        trans_probs[s].append({
            'next_o': int(row['next_out_count']), 'next_r': int(row['next_runner_state']),
            'runs': int(row['runs_produced']), 'freq': int(row['freq'])
        })
        state_totals[s] += int(row['freq'])

    # 3. 리그 전체 평균 pLI를 구하기 위한 상황별 발생 빈도 추출
    # 🚨 [수정 완료] 빈 값(NULL)을 거르는 IS NOT NULL 조건을 추가했습니다!
    query_freq = f"""
        SELECT inning, home_or_away, before_out_count, before_runner_state,
               (before_home_score - before_away_score) AS home_score_diff, COUNT(*) AS freq
        FROM hiball_game_record
        WHERE game_type = 4201 AND season = {season}
          AND inning <= 9
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    df_freq = pd.read_sql(query_freq, engine)
    engine.dispose()

    logging.info(f"[{season}] 이론적 기대 WPA(Expected WPA) 연산 시작 (Noise 0% 완벽 통제)...")

    # 캐싱용 딕셔너리
    pli_memo = {}

    def calc_expected_wpa(inn, tb, out, b_db, diff):
        if (inn, tb, out, b_db, diff) in pli_memo:
            return pli_memo[(inn, tb, out, b_db, diff)]

        current_we = get_win_prob(we_dict, inn, tb, out, b_db, diff)
        expected_wpa = 0.0
        state = (out, b_db)

        if state in trans_probs and state_totals[state] > 0:
            for t in trans_probs[state]:
                prob = t['freq'] / state_totals[state]
                next_diff = diff + t['runs']
                next_we = get_win_prob(we_dict, inn, tb, t['next_o'], t['next_r'], next_diff)

                expected_wpa += prob * abs(next_we - current_we)

        pli_memo[(inn, tb, out, b_db, diff)] = expected_wpa
        return expected_wpa

    # 4. 리그 평균 pLI 계산 (가중 평균)
    total_wpa_swing = 0.0
    total_plays = 0

    for _, row in df_freq.iterrows():
        # 파이썬 내부에서도 혹시 모를 결측치를 한번 더 안전하게 방어 (Safety Check)
        if pd.isna(row['before_out_count']) or pd.isna(row['before_runner_state']):
            continue

        inn = int(row['inning'])
        tb = 1 if str(row['home_or_away']).lower() == 'home' else 0
        out = int(row['before_out_count'])
        b_db = int(row['before_runner_state'])
        diff = int(row['home_score_diff'])
        diff = diff if tb == 1 else -diff
        freq = int(row['freq'])

        pLI = calc_expected_wpa(inn, tb, out, b_db, diff)
        total_wpa_swing += (pLI * freq)
        total_plays += freq

    league_avg_wpa = total_wpa_swing / total_plays if total_plays > 0 else 0.03
    logging.info(f"[{season}] 리그 평균 Expected WPA: {league_avg_wpa:.4f}")

    # 5. 모든 상황별 최종 LI 도출 및 포맷팅
    results = []
    score_mapping = {-4: 'UNDER_4', -3: 'UNDER_3', -2: 'UNDER_2', -1: 'UNDER_1',
                     0: 'TIED', 1: 'UPPER_1', 2: 'UPPER_2', 3: 'UPPER_3', 4: 'UPPER_4'}

    for inn in range(1, 10):
        for tb in [0, 1]:
            ha_str = 'home' if tb == 1 else 'away'
            for out in range(3):
                for b_db in range(8):
                    row_data = {'season': season, 'INNING': inn, 'INNING_TB': tb,
                                'HOME_AWAY': ha_str, 'RUNNER_STATE': b_db, 'OUT_COUNT': out}

                    for diff in range(-4, 5):
                        pLI = calc_expected_wpa(inn, tb, out, b_db, diff)
                        LI = pLI / league_avg_wpa if league_avg_wpa > 0 else 0
                        col_name = score_mapping[diff]
                        row_data[col_name] = round(LI, 2)

                    results.append(row_data)

    final_df = pd.DataFrame(results)
    final_cols = ['season', 'INNING', 'INNING_TB', 'HOME_AWAY', 'RUNNER_STATE', 'OUT_COUNT',
                  'UNDER_4', 'UNDER_3', 'UNDER_2', 'UNDER_1', 'TIED', 'UPPER_1', 'UPPER_2', 'UPPER_3', 'UPPER_4']
    final_df = final_df[final_cols]

    return final_df


if __name__ == "__main__":
    target_year = 2025

    li_matrix = generate_theoretical_li(target_year)

    if li_matrix is not None:
        print("\n[ 최종 산출된 이론적 기대 LI(Theoretical LI) 매트릭스 ]")
        print(li_matrix.head(15).to_string(index=False))

        filename = f"leverage_index_matrix_{target_year}.csv"
        li_matrix.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"{filename} 파일이 생성되었습니다.")