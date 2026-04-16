import pandas as pd
import mysql.connector
import logging
from config import DB_CONFIG, INV_BASE_MAPPING

# 🚨 [수정 완료] 변경된 파일명에서 함수를 가져옵니다.
from yearly_re_rv_matrix_generator import get_inning_run_distribution

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def calculate_cumulative_re_memo(start_year=2016, end_year=2025):
    """지정 기간의 데이터를 합쳐서 통합 RE 득점 분포(memo) 생성"""
    logging.info(f"{start_year} ~ {end_year} 누적 데이터 추출 시작...")
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT before_out_count, before_runner_state, 
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced,
               COUNT(*) AS freq
        FROM hiball_game_record_result
        WHERE game_type = 4201 
          AND season BETWEEN {start_year} AND {end_year}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    df = pd.read_sql(query, conn)
    conn.close()

    transition_data = {}
    for _, row in df.iterrows():
        curr_s = (int(row['before_out_count']), int(row['before_runner_state']))
        if curr_s not in transition_data: transition_data[curr_s] = []
        transition_data[curr_s].append({
            'next_o': int(row['next_out_count']), 'next_r': int(row['next_runner_state']),
            'runs': int(row['runs_produced']), 'freq': int(row['freq'])
        })

    state_totals = {s: sum(i['freq'] for i in items) for s, items in transition_data.items()}

    memo = {}
    for out in range(3):
        for runner in range(8):
            memo[(out, runner)] = get_inning_run_distribution(out, runner, transition_data, state_totals)
    return memo


def convolve_dists(dist_list):
    res = {0: 1.0}
    for d in dist_list:
        new_res = {}
        for r1, p1 in res.items():
            for r2, p2 in d.items():
                if p1 * p2 < 1e-12: continue
                new_res[r1 + r2] = new_res.get(r1 + r2, 0) + p1 * p2
        res = new_res
    return res


def generate_cumulative_we_table(start_yr, end_yr):
    # 1. 누적 득점 분포(memo) 생성
    memo = calculate_cumulative_re_memo(start_yr, end_yr)
    avg_inning_dist = memo[(0, 0)]
    results_dict = {}

    logging.info(f"누적 WE 시뮬레이션(0~2아웃) 계산 중...")
    for inning in range(1, 10):
        for tb in [0, 1]:
            for out in range(3):
                for b_mapped in [0, 1, 2, 12, 3, 13, 23, 123]:
                    b_raw = INV_BASE_MAPPING[b_mapped]
                    n_atk = (9 - inning) + 1
                    n_def = (9 - inning) + 1 if tb == 0 else (9 - inning)

                    atk_dist = convolve_dists([memo[(out, b_raw)]] + [avg_inning_dist] * (n_atk - 1))
                    def_dist = convolve_dists([avg_inning_dist] * n_def)

                    for diff in range(-5, 6):
                        win_prob = 0
                        for r_atk, p_atk in atk_dist.items():
                            threshold = diff + r_atk
                            p_lt = sum(p_def for r_def, p_def in def_dist.items() if r_def < threshold)
                            p_eq = def_dist.get(threshold, 0)
                            win_prob += p_atk * (p_lt + 0.5 * p_eq)
                        results_dict[(inning, tb, out, b_mapped, diff)] = round(win_prob, 3)

    # 2. 3아웃 보정 로직
    logging.info("3아웃 이닝 교대 상황 보정 중...")
    for inning in range(1, 10):
        for tb in [0, 1]:
            for d in range(-5, 6):
                if tb == 0:  # 원정팀 3아웃 -> 홈팀 무사 주자 없음
                    home_we = results_dict.get((inning, 1, 0, 0, -d), 0.5)
                    results_dict[(inning, 0, 3, 0, d)] = round(1 - home_we, 3)
                else:  # 홈팀 3아웃 -> 다음 이닝 원정팀 무사 주자 없음
                    if inning < 9:
                        away_we = results_dict.get((inning + 1, 0, 0, 0, -d), 0.5)
                        results_dict[(inning, 1, 3, 0, d)] = round(1 - away_we, 3)
                    else:  # 9회말 종료
                        if d > 0:
                            results_dict[(inning, 1, 3, 0, d)] = 1.0
                        elif d < 0:
                            results_dict[(inning, 1, 3, 0, d)] = 0.0
                        else:
                            results_dict[(inning, 1, 3, 0, d)] = 0.5

    # 3. 450행 최종 데이터프레임 빌드
    logging.info("최종 450행 테이블 구성 중...")
    final_rows = []
    for inning in range(1, 10):
        for tb in [0, 1]:
            for out in range(4):  # 0, 1, 2, 3아웃 포함
                current_states = [0, 1, 2, 12, 3, 13, 23, 123] if out < 3 else [0]
                for b in current_states:
                    row = {
                        '# HOME_AWAY': 'A' if tb == 0 else 'H',
                        'INNING': inning, 'INNING_TB': tb,
                        'OUT_COUNT': out, 'BASE_STATE': b
                    }
                    for d in range(-5, 6):
                        col_name = "TIE" if d == 0 else (f"UPPER_{d}" if d > 0 else f"UNDER_{abs(d)}")
                        row[col_name] = results_dict.get((inning, tb, out, b, d), 0.0)
                    final_rows.append(row)

    return pd.DataFrame(final_rows)


if __name__ == "__main__":
    # 🚨 [수정 완료] 여기서 연도를 지정하면 알아서 누적 범위를 설정하고 파일명을 만듭니다!
    target_year = 2025
    start_year = 2016  # KBO 트랙맨/PTS 데이터 등 유의미한 데이터 시작 연도

    logging.info(f"[{start_year} ~ {target_year}] 누적 데이터 기반 WE 테이블 생성을 시작합니다.")

    we_df = generate_cumulative_we_table(start_year, target_year)

    if not we_df.empty:
        # 연도가 직관적으로 박힌 파일명으로 자동 저장 (예: win_expectancy_table_2024.csv)
        filename = f"win_expectancy_table_{target_year}.csv"
        we_df.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"🎉 누적 WE 테이블 저장 완료: {filename}")