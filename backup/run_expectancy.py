import pandas as pd
import mysql.connector
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_inning_run_distribution(start_out, start_runner, transition_data, state_totals):
    state_probs = {0: {(start_out, start_runner): 1.0}}
    final_run_dist = {}
    # 최대 30번의 전이(충분한 이닝 길이)를 시뮬레이션
    for _ in range(30):
        new_state_probs = {}
        for runs, states in state_probs.items():
            for (curr_o, curr_r), p_state in states.items():
                if curr_o >= 3 or (curr_o, curr_r) not in transition_data:
                    final_run_dist[runs] = final_run_dist.get(runs, 0) + p_state
                    continue

                t_total = state_totals[(curr_o, curr_r)]
                for item in transition_data[(curr_o, curr_r)]:
                    p_trans = (item['freq'] / t_total) * p_state
                    nr, no, ns = runs + item['runs'], item['next_o'], item['next_r']
                    if nr not in new_state_probs: new_state_probs[nr] = {}
                    new_state_probs[nr][(no, ns)] = new_state_probs[nr].get((no, ns), 0) + p_trans
        state_probs = new_state_probs
        if not state_probs: break
    return final_run_dist


def calculate_season_re(season):
    conn = mysql.connector.connect(**DB_CONFIG)
    query = f"""
        SELECT before_out_count, before_runner_state, 
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced,
               COUNT(*) AS freq
        FROM hiball_game_record_result
        WHERE game_type = 4201 AND season = {season}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """
    df = pd.read_sql(query, conn)
    conn.close()

    if df.empty: return pd.DataFrame()

    transition_data = {}
    for _, row in df.iterrows():
        try:
            curr_s = (int(row['before_out_count']), int(row['before_runner_state']))
            if curr_s not in transition_data: transition_data[curr_s] = []
            transition_data[curr_s].append({
                'next_o': int(row['next_out_count']),
                'next_r': int(row['next_runner_state']),
                'runs': int(row['runs_produced']),
                'freq': int(row['freq'])
            })
        except:
            continue

    state_totals = {s: sum(i['freq'] for i in items) for s, items in transition_data.items()}
    re_results = []

    # 1. 0~2아웃 상황 계산
    for runner in range(8):
        for out in range(3):
            # 상황별 기대 득점 분포 계산
            dist = get_inning_run_distribution(out, runner, transition_data, state_totals)
            re_val = sum(runs * prob for runs, prob in dist.items())

            # 발생 빈도(count) 및 총 기대 득점(run) 산출
            count = state_totals.get((out, runner), 0)
            run = round(re_val * count) if count > 0 else 0

            re_results.append({
                'season': season,
                'situation': f"{runner}{out}",
                'value': round(re_val, 3),
                'runner_state': runner,
                'out_count': out,
                'run': run,
                'count': count
            })

        # 2. 3아웃 상황 추가 (요청하신대로 value 0.000 및 count 0으로 처리)
        re_results.append({
            'season': season,
            'situation': f"{runner}3",
            'value': 0.000,
            'runner_state': runner,
            'out_count': 3,
            'run': 0,
            'count': 0
        })

    return pd.DataFrame(re_results)


if __name__ == "__main__":
    # 2025년 타겟팅
    target_years = [2025]
    final_output_list = []

    for year in target_years:
        logging.info(f"{year} 시즌 RE Matrix 계산 중...")
        re_df = calculate_season_re(year)
        if not re_df.empty:
            final_output_list.append(re_df)

            # 콘솔 출력용 (요청하신 형식과 동일하게 보이기 위함)
            print(f"\n# season\tsituation\tvalue\trunner_state\tout_count\trun\tcount")
            for _, row in re_df.iterrows():
                print(
                    f"{row['season']}\t{row['situation']}\t{row['value']:.3f}\t{row['runner_state']}\t{row['out_count']}\t{row['run']}\t{row['count']}")

    if final_output_list:
        final_master = pd.concat(final_output_list)
        # CSV 파일로도 저장 (엑셀 작업 시 유용)
        final_master.to_csv("yearly_re_matrix.csv", index=False, encoding='utf-8-sig')
        logging.info("yearly_re_matrix.csv 저장 완료.")