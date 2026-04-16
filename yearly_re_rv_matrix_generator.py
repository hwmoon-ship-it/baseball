import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==============================================================================
# 1. 득점 기대치 (RE) 산출 모듈
# ==============================================================================
def get_inning_run_distribution(start_out, start_runner, transition_data, state_totals):
    state_probs = {0: {(start_out, start_runner): 1.0}}
    final_run_dist = {}
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


def generate_re_matrix(season, engine):
    logging.info(f"[{season}] RE Matrix 연산 중...")
    query = f"""
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
    df = pd.read_sql(query, engine)

    transition_data = {}
    for _, row in df.iterrows():
        try:
            curr_s = (int(row['before_out_count']), int(row['before_runner_state']))
            if curr_s not in transition_data: transition_data[curr_s] = []
            transition_data[curr_s].append({
                'next_o': int(row['next_out_count']), 'next_r': int(row['next_runner_state']),
                'runs': int(row['runs_produced']), 'freq': int(row['freq'])
            })
        except:
            continue

    state_totals = {s: sum(i['freq'] for i in items) for s, items in transition_data.items()}
    re_results = []
    re_dict = {}

    for runner in range(8):
        for out in range(3):
            dist = get_inning_run_distribution(out, runner, transition_data, state_totals)
            re_val = sum(runs * prob for runs, prob in dist.items())
            count = state_totals.get((out, runner), 0)
            run = round(re_val * count) if count > 0 else 0

            re_dict[(season, out, runner)] = re_val
            re_results.append({
                'season': season, 'situation': f"{runner}{out}", 'value': round(re_val, 3),
                'runner_state': runner, 'out_count': out, 'run': run, 'count': count
            })

        # 3아웃 처리
        re_dict[(season, 3, runner)] = 0.000
        re_results.append({
            'season': season, 'situation': f"{runner}3", 'value': 0.000,
            'runner_state': runner, 'out_count': 3, 'run': 0, 'count': 0
        })

    re_df = pd.DataFrame(re_results)
    re_df = re_df.sort_values(by=['runner_state', 'out_count']).reset_index(drop=True)
    return re_df, re_dict


# ==============================================================================
# 2. 타격 이벤트 RV 산출 및 매핑 모듈 (기존 DB 완벽 호환 커스텀판)
# ==============================================================================
def generate_rv_for_hit_result(season, engine, re_dict):
    logging.info(f"[{season}] 타격/주루 이벤트 RV 연산 중 (DB 스펙 호환)...")
    query = f"""
        SELECT season, hit_result_name, 
               before_out_count, before_runner_state,
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced,
               plate_appearance, at_bat, base_on_balls, hit_by_pitch, sacrifice_flies_out, intentional_walks
        FROM hiball_game_record
        WHERE game_type = 4201 AND season = {season}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
          AND hit_result_name IS NOT NULL
    """
    df_plays = pd.read_sql(query, engine)

    # Scale 계산
    total_h = len(df_plays[df_plays['hit_result_name'].isin(['1루타', '2루타', '3루타', '홈런'])])
    total_ab = df_plays['at_bat'].sum()
    total_bb = df_plays['base_on_balls'].sum()
    total_hbp = df_plays['hit_by_pitch'].sum()
    total_sf = df_plays['sacrifice_flies_out'].sum()

    obp_den = total_ab + total_bb + total_hbp + total_sf
    obp = (total_h + total_bb + total_hbp) / obp_den if obp_den > 0 else 0.330
    scale = 1.207

    # RV 연산
    def get_rv(row):
        try:
            s, b_o, b_s = int(row['season']), int(row['before_out_count']), int(row['before_runner_state'])
            n_o, n_s = int(row['next_out_count']), int(row['next_runner_state'])
            re_pre = re_dict.get((s, b_o, b_s), 0.0)
            re_post = 0.0 if n_o >= 3 else re_dict.get((s, n_o, n_s), 0.0)
            return re_post - re_pre + row['runs_produced']
        except:
            return 0.0

    df_plays['rv'] = df_plays.apply(get_rv, axis=1)

    # 각 결과별 평균 RV
    rv_avg = df_plays.groupby('hit_result_name')['rv'].mean()

    # 🚨 일반 아웃 평균 계산
    out_events = ['땅볼아웃', '플라이아웃', '직선타아웃']
    out_val = rv_avg[rv_avg.index.isin(out_events)].mean() if any(rv_avg.index.isin(out_events)) else -0.266

    # 🚨 병살 + 삼중살 통합 평균 계산 (DOUBLE_OUT으로 통합)
    dp_tp_events = df_plays[df_plays['hit_result_name'].str.contains('병살|삼중살', na=False)]
    double_out_val = dp_tp_events['rv'].mean() if not dp_tp_events.empty else -0.870

    # 🚨 낫아웃(아웃) 평균 계산
    not_out_events = df_plays[df_plays['hit_result_name'].str.contains('낫아웃터치아웃|낫아웃송구아웃', na=False)]
    not_out_val = not_out_events['rv'].mean() if not not_out_events.empty else -0.288  # 삼진과 비슷하게 세팅

    # 기존 DB 항목에 맞춘 최종 매핑
    hit_mapping = {
        'SO': rv_avg.get('삼진아웃', -0.288),
        '1H': rv_avg.get('1루타', 0.490),
        'BB': rv_avg.get('볼넷', 0.358),
        '2H': rv_avg.get('2루타', 0.819),
        'DOUBLE_OUT': double_out_val,  # 🚨 병살 + 삼중살 통합
        'HBP': rv_avg.get('사구(HP)', 0.385),
        '3H': rv_avg.get('3루타', 1.253),
        'HR': rv_avg.get('홈런', 1.429),
        'SF_OUT': rv_avg.get('희생플라이아웃', -0.100),
        'ERROR': rv_avg.get('실책', 0.523),
        'SB': rv_avg.get('도루', 0.200),
        'CS': rv_avg.get('도루자', rv_avg.get('도루실패', -0.420)),
        'OUT': out_val,  # 일반 아웃
        'NOT_OUT_OUT': not_out_val,  # 🚨 낫아웃 아웃 추가
        'IBB': rv_avg.get('고의사구', 0.193),
        'SCALE': scale
    }

    rv_hit_list = [{'season': season, 'hit_result_name': k, 'Run_Value': round(v, 3)} for k, v in hit_mapping.items()]
    return pd.DataFrame(rv_hit_list)


# ==============================================================================
# 3. 볼카운트별 RV (Pitch Value) 산출 모듈
# ==============================================================================
def generate_rv_for_count(season, engine, re_dict):
    logging.info(f"[{season}] 볼카운트별 RV(rv_for_count) 연산 중...")
    query = f"""
        SELECT season, b_stand AS ball_count, s_stand AS strike_count,
               before_out_count, before_runner_state,
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced,
               hit_result_name
        FROM hiball_game_record
        WHERE game_type = 4201 AND season = {season}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
    """
    df_pitch = pd.read_sql(query, engine)
    df_pitch = df_pitch[(df_pitch['ball_count'] <= 3) & (df_pitch['strike_count'] <= 2)]

    def get_rv(row):
        if pd.isna(row['hit_result_name']): return 0.0
        try:
            s, b_o, b_s = int(row['season']), int(row['before_out_count']), int(row['before_runner_state'])
            n_o, n_s = int(row['next_out_count']), int(row['next_runner_state'])
            re_pre = re_dict.get((s, b_o, b_s), 0.0)
            re_post = 0.0 if n_o >= 3 else re_dict.get((s, n_o, n_s), 0.0)
            return re_post - re_pre + row['runs_produced']
        except:
            return 0.0

    df_pitch['rv'] = df_pitch.apply(get_rv, axis=1)
    count_rv = df_pitch.groupby(['strike_count', 'ball_count'])['rv'].mean().reset_index()
    c_dict = {(r['strike_count'], r['ball_count']): r['rv'] for _, r in count_rv.iterrows()}

    rv_count_results = []
    for strike in range(3):
        for ball in range(4):
            base_rv = c_dict.get((strike, ball), 0.0)
            rv_ball = c_dict.get((strike, ball + 1), base_rv) - base_rv if ball < 3 else 0.0
            rv_strike = c_dict.get((strike + 1, ball), base_rv) - base_rv if strike < 2 else 0.0

            rv_count_results.append({
                'season': season,
                'count': f"{strike}-{ball}",
                'run_value': round(base_rv, 4),
                'ball': round(rv_ball, 4),
                'strike': round(rv_strike, 4),
                'ball_count': ball,
                'strike_count': strike
            })

    return pd.DataFrame(rv_count_results)


if __name__ == "__main__":
    target_year = 2025

    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    df_re, dict_re = generate_re_matrix(target_year, engine)

    if dict_re:
        df_rv_hit = generate_rv_for_hit_result(target_year, engine, dict_re)
        df_rv_count = generate_rv_for_count(target_year, engine, dict_re)

        print("\n=== [1] yearly_re_matrix ===")
        print(df_re.head(10).to_string(index=False))

        print("\n=== [2] rv_for_count ===")
        print(df_rv_count.to_string(index=False))

        print("\n=== [3] rv_for_hit_result (SB, CS 포함) ===")
        print(df_rv_hit.to_string(index=False))

        df_re.to_csv(f"yearly_re_matrix_{target_year}.csv", index=False, encoding='utf-8-sig')
        df_rv_hit.to_csv(f"rv_for_hit_result_{target_year}.csv", index=False, encoding='utf-8-sig')
        df_rv_count.to_csv(f"rv_for_count_{target_year}.csv", index=False, encoding='utf-8-sig')
        logging.info("🎉 SB, CS가 추가된 3종 매트릭스 CSV 생성 완료!")

    engine.dispose()