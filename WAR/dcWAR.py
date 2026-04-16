import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==============================================================================
# 1. 포수 전용 마스터 데이터 추출 (트랙맨 + 수비 + 게임기록 3단 조인)
# ==============================================================================
def get_catcher_master_data(season, engine):
    logging.info(f"[{season}] 포수의 마스터 데이터를 추출합니다 (한글 이름 매핑 포함)...")

    # 🚨 수정: fr.final_fielder_name (한글)을 포수 이름으로 우선 사용합니다!
    query = f"""
        SELECT 
            fr.final_fielder_name AS catcher_name,  -- 🚨 한글 이름 최우선 적용
            tr.catcher AS trackman_catcher,         -- 트랙맨 영문 이름 (백업)
            tr.plate_loc_height,              
            tr.rel_speed,                     
            tr.pop_time,                      
            gr.hit_result_name AS game_event, 
            fr.batted_ball_type_code,         
            fr.error_yn,                      
            tr.exit_speed,                    
            tr.angle,                         
            tr.bearing,                       
            fr.fielding_result_position_name  
        FROM hiball_trackman_record tr
        LEFT JOIN hiball_fielding_record fr
               ON CAST(tr.game_info_id AS UNSIGNED) = CAST(fr.game_info_id AS UNSIGNED)
              AND CAST(tr.inning_pitch_seq AS UNSIGNED) = CAST(fr.fr_inning_pitch_seq AS UNSIGNED)
              AND fr.fielding_record_seq = 1
        LEFT JOIN hiball_game_record gr
               ON fr.game_record_id = gr.game_record_id
        WHERE tr.season = {season}
          -- 포수 포지션인 경우만 명확히 필터링
          AND (fr.fielding_result_position_name = '포수' OR fr.fielding_result_position_name IS NULL)
    """
    df = pd.read_sql(query, engine)

    # 🚨 영문 이름만 있는 경우의 백업 처리 (하지만 대부분 fr.final_fielder_name이 있을 것입니다)
    df['catcher_name'] = df['catcher_name'].fillna(df['trackman_catcher'])

    # 이름이 없는 행은 제외
    df = df[df['catcher_name'].notna() & (df['catcher_name'] != '')]

    # 숫자형 변환
    num_cols = ['plate_loc_height', 'rel_speed', 'exit_speed', 'angle', 'bearing', 'pop_time']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    df['game_event'] = df['game_event'].fillna('')
    return df


# ==============================================================================
# 2. 포수 3대장 OAA (블로킹, 도루, 땅볼) 스케일 조정 산출
# ==============================================================================
def calculate_catcher_metrics(df):
    logging.info("블로킹, 도루 저지, 타구 처리 OAA를 내/외야수 밸런스에 맞춰 계산합니다...")

    catcher_stats = []
    grouped = df.groupby('catcher_name')

    for catcher_name, group in grouped:
        total_pitches = len(group)
        if total_pitches < 50:
            continue

        # ---------------------------------------------------------
        # [1] 블로킹 OAA (난이도 상향 패치)
        # ---------------------------------------------------------
        blocking_opps = group[(group['plate_loc_height'] > -2.0) & (group['plate_loc_height'] <= 0.5)]
        block_total = len(blocking_opps)
        block_fails = blocking_opps['game_event'].str.contains('폭투|포일', na=False).sum()
        block_success = block_total - block_fails

        # 🚨 KBO 1군 포수라면 블로킹은 기본적으로 98%는 성공해야 한다고 가정 (엄격한 기준)
        # 성공해도 +0.02점밖에 안 오르고, 한 번 흘리면 -0.98점의 치명타!
        expected_blocks = block_total * 0.98
        blocking_oaa = block_success - expected_blocks if block_total > 0 else 0

        # ---------------------------------------------------------
        # [2] 도루 저지 OAA (난이도 상향 패치)
        # ---------------------------------------------------------
        steal_opps = group[group['game_event'].str.contains('도루', na=False)]
        cs_total = len(steal_opps)
        cs_success = steal_opps['game_event'].str.contains('도루자|아웃', na=False).sum()

        # 🚨 KBO 평균 도루 저지율 약 30% 적용
        # 잡아내면 +0.7점, 내주면 -0.3점 수준의 밸런스
        expected_cs = cs_total * 0.30
        cs_oaa = cs_success - expected_cs if cs_total > 0 else 0

        # ---------------------------------------------------------
        # [3] 타구 처리 OAA
        # ---------------------------------------------------------
        fielding_opps = group[(group['batted_ball_type_code'].isin(['6701', 6701])) &
                              (group['fielding_result_position_name'] == '포수')]

        fielding_total = len(fielding_opps)
        f_oaa_sum = 0
        for _, row in fielding_opps.iterrows():
            is_caught = 0 if str(row['error_yn']).upper() == 'Y' else (1 if '아웃' in row['game_event'] else 0)
            speed = row['exit_speed']

            if speed > 130:
                prob = 0.60
            elif speed < 60:
                prob = 0.40
            else:
                prob = 0.85

            f_oaa_sum += (is_caught - prob)

        catcher_stats.append({
            '포수명': catcher_name,
            '총투구수': total_pitches,
            '블로킹기회': block_total,
            '블로킹OAA': round(blocking_oaa, 2),
            '도루시도': cs_total,
            '저지OAA': round(cs_oaa, 2),
            '타구기회': fielding_total,
            '타구OAA': round(f_oaa_sum, 2)
        })

    return pd.DataFrame(catcher_stats)


# ==============================================================================
# 3. 종합 수비 득점 가치(Run Value) 및 밸런스 패치된 dWAR 계산
# ==============================================================================
def calculate_catcher_dwar(stats_df):
    logging.info("3대장 지표를 내/외야수 스케일과 동일한 득점 가치로 환산합니다...")

    # 🚨 득점 가치 환산율 하향 (스케일 조정)
    # 1 도루저지 = 약 0.45 득점 (내/외야수 1아웃 처리와 비슷한 수준으로 하향)
    # 1 블로킹 = 약 0.25 득점
    stats_df['블로킹_득점'] = stats_df['블로킹OAA'] * 0.25
    stats_df['도루저지_득점'] = stats_df['저지OAA'] * 0.45
    stats_df['타구처리_득점'] = stats_df['타구OAA'] * 0.85

    stats_df['순수수비득점(Runs)'] = stats_df['블로킹_득점'] + stats_df['도루저지_득점'] + stats_df['타구처리_득점']

    # 🚨 포지션 보정 점수 밸런스 패치:
    # 내야수처럼 '타구 기회'를 기준으로 할지, 투구수를 기준으로 할지 고민되는 부분.
    # 포수는 투구 하나하나가 노동이므로 투구수를 쓰되, 최대치(+12.5점)의 도달 기준을 엄격하게!
    # 1시즌 풀타임 포수 투구수를 약 8,000구로 잡고 비율 적용
    stats_df['포지션보정'] = (stats_df['총투구수'] / 8000.0) * 12.5

    # 🚨 너무 높은 보정 점수 방지 (최대 12.5점)
    stats_df['포지션보정'] = np.where(stats_df['포지션보정'] > 12.5, 12.5, stats_df['포지션보정'])

    stats_df['최종_수비점수'] = stats_df['순수수비득점(Runs)'] + stats_df['포지션보정']

    # 🚨 WAR 변환 상수 (Runs per Win = 10.0 고정)
    stats_df['포수_dWAR'] = (stats_df['최종_수비점수'] / 10.0).round(3)

    final_df = stats_df.sort_values(by='포수_dWAR', ascending=False)

    display_cols = ['포수명', '총투구수', '블로킹OAA', '저지OAA', '타구OAA', '순수수비득점(Runs)', '포지션보정', '포수_dWAR']
    return final_df[display_cols]


# ==============================================================================
# 메인 실행부
# ==============================================================================
if __name__ == "__main__":
    target_year = 2025
    logging.info("ABS 시대 포수 전용 수비 평가(dWAR) 파이프라인 가동을 시작합니다...")

    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    df_raw = get_catcher_master_data(target_year, engine)

    if not df_raw.empty:
        metrics_df = calculate_catcher_metrics(df_raw)
        dwar_df = calculate_catcher_dwar(metrics_df)

        print(f"\n=== [ {target_year}시즌 포수 dWAR (밸런스 패치 완료) Top 10 ] ===")
        print(dwar_df.head(10).to_string(index=False))

        csv_filename = f"dwar_catcher_result_{target_year}.csv"
        try:
            dwar_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            logging.info(f"🎉 완벽합니다! 스케일 조정된 포수 dWAR이 '{csv_filename}'에 저장되었습니다.")
        except PermissionError:
            logging.error(f"🚨 '{csv_filename}' 파일이 열려 있어 저장 실패! 엑셀을 닫고 다시 실행해주세요.")
    else:
        logging.warning("포수 데이터를 DB에서 찾을 수 없습니다.")

    engine.dispose()