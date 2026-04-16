import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==============================================================================
# 1. 파일 로드
# ==============================================================================
def load_re_matrix(season):
    filename = f"yearly_re_matrix_{season}.csv"
    try:
        df = pd.read_csv(filename)
        re_dict = {}
        for _, row in df.iterrows():
            re_dict[(int(row['out_count']), int(row['runner_state']))] = float(row['value'])
        return re_dict
    except FileNotFoundError:
        fallback = f"../{filename}"
        try:
            df = pd.read_csv(fallback)
            re_dict = {}
            for _, row in df.iterrows():
                re_dict[(int(row['out_count']), int(row['runner_state']))] = float(row['value'])
            return re_dict
        except:
            return None


def load_league_constants(season):
    filename = f"league_constants_{season}_raw.csv"
    try:
        return pd.read_csv(filename).iloc[0].to_dict()
    except:
        fallback = f"../{filename}"
        try:
            return pd.read_csv(fallback).iloc[0].to_dict()
        except:
            return None


# ==============================================================================
# 2. 통합 주루/트랙맨 데이터 추출
# ==============================================================================
def get_integrated_baserunning_data(season, engine):
    query = f"""
        SELECT 
            rr.game_record_id, rr.runner_id, rr.runner_name,
            gr.hit_result_name, gr.before_out_count, gr.before_runner_state,  
            rr.base_move_type_name, rr.start_base, rr.end_base, rr.out_yn,               
            tr.exit_speed, tr.angle, tr.bearing               
        FROM hiball_runner_record rr
        INNER JOIN hiball_game_record gr ON rr.game_record_id = gr.game_record_id
        LEFT JOIN hiball_trackman_record tr ON rr.game_record_id = tr.pitch_uid
        WHERE rr.season = {season} AND gr.game_type = 4201 AND rr.start_base > 0
    """
    df = pd.read_sql(query, engine)
    df['exit_speed'] = pd.to_numeric(df['exit_speed'], errors='coerce').fillna(0)
    df['angle'] = pd.to_numeric(df['angle'], errors='coerce').fillna(0)
    df['bearing'] = pd.to_numeric(df['bearing'], errors='coerce').fillna(0)
    return df


# ==============================================================================
# 3. 주루 이벤트 세분화 및 세부 기록 추적기 (🚨 땅볼 추가 & 도실/주루사 분리)
# ==============================================================================
def classify_runner_events(df):
    def get_event_class(row):
        move_type = str(row['base_move_type_name'])
        hit_result = str(row['hit_result_name'])
        start_base = int(row['start_base'])

        # 1. 특수 주루 (분리)
        if move_type == '도루': return '도루_성공'
        if move_type == '도루실패(아웃)' or '도루실패' in move_type: return '도루_실패'
        if '주루사' in move_type or '견제사' in move_type: return '주루사_견제사'
        if move_type in ['폭투', '포일']: return '배터리실수_진루'

        # 2. 타격 기반 추가 진루 (UBR)
        if '1루타' in hit_result:
            if start_base == 1: return '1루타_1루주자'
            if start_base == 2: return '1루타_2루주자'
        elif '2루타' in hit_result:
            if start_base == 1: return '2루타_1루주자'
        elif '플라이아웃' in hit_result or '희생플라이' in hit_result:
            if start_base == 1: return '뜬공_1루주자_태그업'
            if start_base == 2: return '뜬공_2루주자_태그업'
            if start_base == 3: return '뜬공_3루주자_태그업'
        # 🚨 [신규] 땅볼 진루 추가
        elif '땅볼' in hit_result or move_type == '땅볼':
            if start_base == 1: return '땅볼_1루주자'
            if start_base == 2: return '땅볼_2루주자'
            if start_base == 3: return '땅볼_3루주자'

        return '기타_자동진루'

    df['event_class'] = df.apply(get_event_class, axis=1)

    # 기록 카운팅용 매핑
    def map_detail_stat(row):
        ev, end, out = row['event_class'], int(row['end_base']), row['out_yn'] == 'Y'

        if ev == '1루타_1루주자':
            return '1루타_1루_아웃' if out else ('1루타_1루_3루(성공)' if end >= 3 else '1루타_1루_2루(머뭄)')
        elif ev == '1루타_2루주자':
            return '1루타_2루_아웃' if out else ('1루타_2루_홈(성공)' if end >= 4 else '1루타_2루_3루(머뭄)')
        elif ev == '도루_성공':
            return '도루_성공'
        elif ev == '도루_실패':
            return '도루_실패'  # 🚨 순수 도실만 카운트
        elif ev == '주루사_견제사':
            return '주루사_견제사'  # 🚨 주루사/견제사 분리
        elif '땅볼' in ev:
            if out: return f'{ev}_아웃'
            # 땅볼 진루 판별: 1루주자->3루, 2루주자->3루/홈, 3루주자->홈 이면 진루 성공
            is_advance = False
            if '1루주자' in ev and end >= 3: is_advance = True
            if '2루주자' in ev and end >= 3: is_advance = True
            if '3루주자' in ev and end >= 4: is_advance = True
            return f'{ev}_진루' if is_advance else f'{ev}_머뭄'
        return None

    df['detail_stat'] = df.apply(map_detail_stat, axis=1)
    stat_df = df.dropna(subset=['detail_stat'])
    runner_stats = pd.crosstab(stat_df['runner_name'], stat_df['detail_stat']).reset_index()

    ubr_events = ['1루타_1루주자', '1루타_2루주자', '2루타_1루주자',
                  '뜬공_1루주자_태그업', '뜬공_2루주자_태그업', '뜬공_3루주자_태그업',
                  '땅볼_1루주자', '땅볼_2루주자', '땅볼_3루주자']
    ubr_df = df[df['event_class'].isin(ubr_events)].copy()

    return df, ubr_df, runner_stats


# ==============================================================================
# 4. 트랙맨 기반 상황별 진루 확률표 생성 (땅볼 알고리즘 추가)
# ==============================================================================
def build_integrated_probability_table(ubr_df):
    ubr_df['speed_group'] = pd.cut(ubr_df['exit_speed'], bins=[-1, 120, 140, 200],
                                   labels=['느림(<120)', '보통(120~140)', '빠름(>140)'])
    ubr_df['direction_group'] = pd.cut(ubr_df['bearing'], bins=[-180, -15, 15, 180], labels=['좌측', '중앙', '우측'])

    def get_outcome(row):
        if row['out_yn'] == 'Y': return 'Out'
        start, end = int(row['start_base']), int(row['end_base'])
        ev = row['event_class']

        if ev == '1루타_1루주자': return 'Advance' if end >= 3 else 'Hold'
        if ev == '1루타_2루주자' or ev == '2루타_1루주자': return 'Advance' if end >= 4 else 'Hold'
        if '태그업' in ev: return 'Advance' if end > start else 'Hold'

        # 🚨 [신규] 땅볼 진루 판별 로직
        if '땅볼' in ev:
            if start == 1: return 'Advance' if end >= 3 else 'Hold'
            if start == 2: return 'Advance' if end >= 3 else 'Hold'  # 2루 주자가 3루 가는 것도 큰 진루
            if start == 3: return 'Advance' if end >= 4 else 'Hold'

        return 'Hold'

    ubr_df['outcome'] = ubr_df.apply(get_outcome, axis=1)
    prob_table = ubr_df.groupby(['event_class', 'speed_group', 'direction_group', 'outcome'],
                                observed=False).size().unstack(fill_value=0)

    for col in ['Advance', 'Hold', 'Out']:
        if col not in prob_table.columns: prob_table[col] = 0

    prob_table['Total'] = prob_table['Advance'] + prob_table['Hold'] + prob_table['Out']
    prob_table['P_Advance'] = np.where(prob_table['Total'] > 0, prob_table['Advance'] / prob_table['Total'], 0)
    prob_table['P_Hold'] = np.where(prob_table['Total'] > 0, prob_table['Hold'] / prob_table['Total'], 1)
    prob_table['P_Out'] = np.where(prob_table['Total'] > 0, prob_table['Out'] / prob_table['Total'], 0)

    return prob_table, ubr_df


# ==============================================================================
# 5. 기대 득점(ERV) 대비 최종 rWAR 계산
# ==============================================================================
def calculate_final_rwar(df_all, ubr_df, prob_table, re_dict, constants):
    run_values = []

    # 1. UBR (타구 판단에 의한 추가 진루 연산)
    for _, row in ubr_df.iterrows():
        try:
            probs = prob_table.loc[(row['event_class'], row['speed_group'], row['direction_group'])]
            p_adv, p_hold, p_out = probs['P_Advance'], probs['P_Hold'], probs['P_Out']
            o = int(row['before_out_count'])
            ev = row['event_class']

            if ev == '1루타_1루주자':
                rv_hold = re_dict.get((o, 3), 0.90)
                rv_advance = re_dict.get((o, 5), 1.15)
                rv_out = re_dict.get((min(o + 1, 3), 1), 0.22) if o < 2 else 0
            elif ev == '1루타_2루주자':
                rv_hold = re_dict.get((o, 5), 1.15)
                rv_advance = re_dict.get((o, 1), 0.22) + 1.0
                rv_out = re_dict.get((min(o + 1, 3), 1), 0.22) if o < 2 else 0
            elif '태그업' in ev:
                rv_advance, rv_hold, rv_out = 0.35, 0.00, -0.60
            elif '땅볼' in ev:
                # 땅볼 진루는 병살타 등 상황이 복잡하므로 고정 가중치(Linear Weights) 적용
                rv_advance, rv_hold, rv_out = +0.15, -0.10, -0.45
            else:
                rv_advance, rv_hold, rv_out = +0.30, +0.00, -0.50

            erv = (p_adv * rv_advance) + (p_hold * rv_hold) + (p_out * rv_out)

            if row['outcome'] == 'Advance':
                actual_rv = rv_advance
            elif row['outcome'] == 'Hold':
                actual_rv = rv_hold
            else:
                actual_rv = rv_out

            run_values.append({'runner_name': row['runner_name'], 'event_type': 'UBR', 'run_value': actual_rv - erv})
        except KeyError:
            continue

    # 2. 순수 발야구 연산 (도루/도실/주루사)
    sb_value = constants.get('run_sb', 0.2)
    cs_value = constants.get('run_cs', -0.4)
    oob_value = -0.5  # 견제사/주루사는 도실보다 더 큰 팀 모멘텀 손실

    for _, row in df_all.iterrows():
        event = row['event_class']
        if event == '도루_성공':
            run_values.append({'runner_name': row['runner_name'], 'event_type': '도루성공', 'run_value': sb_value})
        elif event == '도루_실패':
            run_values.append({'runner_name': row['runner_name'], 'event_type': '도루실패', 'run_value': cs_value})
        elif event == '주루사_견제사':
            run_values.append({'runner_name': row['runner_name'], 'event_type': '주루사', 'run_value': oob_value})

    rv_df = pd.DataFrame(run_values)
    rwar_df = rv_df.groupby('runner_name')['run_value'].sum().reset_index()
    rwar_df['rWAR'] = rwar_df['run_value'] / constants.get('r_win', 10.0)

    return rwar_df.round(3)


# ==============================================================================
# 메인 실행부
# ==============================================================================
if __name__ == "__main__":
    target_year = 2025
    logging.info("땅볼 진루와 주루사 분리가 완벽히 적용된 rWAR 파이프라인 가동...")

    re_dict = load_re_matrix(target_year)
    constants = load_league_constants(target_year)

    if re_dict and constants:
        db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)
        df_raw = get_integrated_baserunning_data(target_year, engine)

        if not df_raw.empty:
            df_all, ubr_df, runner_stats = classify_runner_events(df_raw)
            prob_table, _ = build_integrated_probability_table(ubr_df)

            prob_table[['Total', 'P_Advance', 'P_Hold', 'P_Out']].round(3).to_csv(
                f"integrated_advancement_prob_{target_year}.csv", encoding='utf-8-sig')

            rwar_df = calculate_final_rwar(df_all, ubr_df, prob_table, re_dict, constants)
            final_result = pd.merge(rwar_df, runner_stats, on='runner_name', how='left').fillna(0)
            final_result = final_result.sort_values(by='rWAR', ascending=False)

            count_cols = [c for c in final_result.columns if c not in ['runner_name', 'run_value', 'rWAR']]
            final_result[count_cols] = final_result[count_cols].astype(int)

            print(f"\n=== [ {target_year}시즌 통합 rWAR Top 10 ] ===")
            display_cols = ['runner_name', 'rWAR', 'run_value', '도루_성공', '도루_실패', '주루사_견제사', '땅볼_2루주자_진루']
            display_cols = [c for c in display_cols if c in final_result.columns]
            print(final_result[display_cols].head(10).to_string(index=False))

            csv_filename = f"rwar_total_result_{target_year}.csv"
            try:
                final_result.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                logging.info(f"🎉 오류 수정 완료! 결과가 '{csv_filename}'에 저장되었습니다.")
            except PermissionError:
                logging.error(f"🚨 '{csv_filename}' 파일이 열려 있어 저장 실패! 엑셀을 닫고 다시 실행해주세요.")

        engine.dispose()