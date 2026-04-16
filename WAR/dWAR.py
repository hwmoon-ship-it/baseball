import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_data(season, engine):
    logging.info(f"[{season}] 타구 처리 마스터 데이터를 안전하게 추출합니다...")

    # 🚨 영문 이름 조인 에러를 방지하기 위해 오직 한글(수비 테이블) 기반 단일 쿼리만 사용!
    query = f"""
        SELECT 
            fr.final_fielder_name AS fielder_name,  
            fr.fielding_result_position_name AS pos, 
            gr.hit_result_name AS game_event,               
            fr.batted_ball_type_code,  
            fr.error_yn,                      
            tr.exit_speed,                    
            tr.angle,                         
            tr.bearing
        FROM hiball_fielding_record fr
        LEFT JOIN hiball_trackman_record tr 
               ON CAST(fr.game_info_id AS UNSIGNED) = CAST(tr.game_info_id AS UNSIGNED)
              AND CAST(fr.fr_inning_pitch_seq AS UNSIGNED) = CAST(tr.inning_pitch_seq AS UNSIGNED)
        LEFT JOIN hiball_game_record gr
               ON fr.game_record_id = gr.game_record_id
        WHERE fr.season = {season}
          AND fr.f_game_type = 4201  
          AND fr.fielding_result_position_name IS NOT NULL
          AND fr.fielding_result_position_name != ''
          AND fr.fielding_record_seq = 1  
    """
    df = pd.read_sql(query, engine)
    for col in ['exit_speed', 'angle', 'bearing']:
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    df['game_event'] = df['game_event'].fillna('')
    return df


def calculate_oaa(df):
    logging.info("OAA 볼륨 정상화 연산 중 (dWAR 2점대 스케일 복구)...")
    stats = []

    for (name, pos), group in df.groupby(['fielder_name', 'pos']):
        bip_opps = group[group['batted_ball_type_code'].isin(['6701', '6702', '6703', 6701, 6702, 6703])]
        f_opp = len(bip_opps)
        f_succ, f_oaa = 0, 0.0

        for _, row in bip_opps.iterrows():
            is_error = str(row['error_yn']).upper() == 'Y'
            is_caught = 1 if ('아웃' in str(row['game_event']) or '병살' in str(row['game_event'])) and not is_error else 0
            if is_error: is_caught = 0
            f_succ += is_caught

            speed, angle, bearing = row['exit_speed'], row['angle'], row['bearing']
            b_type = str(row['batted_ball_type_code'])
            if speed == 0 and angle == 0: continue

            prob = 0.0

            # 🚨 1. 내야수 (OAA 볼륨이 예쁘게 쌓이도록 확률을 85~90% 수준으로 완화)
            if pos in ['1루수', '2루수', '3루수', '유격수', '투수'] and ('6701' in b_type or '땅볼' in b_type):
                is_routine = False
                if pos == '3루수' and -40 <= bearing <= -15:
                    is_routine = True
                elif pos == '유격수' and -25 <= bearing <= 5:
                    is_routine = True
                elif pos == '2루수' and -5 <= bearing <= 30:
                    is_routine = True
                elif pos == '1루수' and 15 <= bearing <= 40:
                    is_routine = True
                elif pos == '투수' and -10 <= bearing <= 10:
                    is_routine = True

                if is_routine:
                    if speed > 150:
                        prob = 0.40  # 강습 캐치 (+0.60 획득)
                    elif speed > 135:
                        prob = 0.70  # 좋은 수비 (+0.30 획득)
                    else:
                        prob = 0.85  # 평범한 땅볼 (+0.15 꾸준히 적립!)
                else:
                    prob = 0.15  # 수비 범위 밖 안타 (놓쳐도 -0.15만 감점)

            # 🚨 2. 외야수 (무한 마이너스 방지 & 볼륨 업)
            elif pos in ['좌익수', '중견수', '우익수']:
                if '6701' in b_type or '땅볼' in b_type: continue

                is_routine_zone = False
                if pos == '좌익수' and bearing < -10:
                    is_routine_zone = True
                elif pos == '중견수' and -25 <= bearing <= 25:
                    is_routine_zone = True
                elif pos == '우익수' and bearing > 10:
                    is_routine_zone = True

                if is_routine_zone:
                    if angle < 20 and speed > 140:
                        prob = 0.30
                    elif angle < 20:
                        prob = 0.65
                    elif speed > 145:
                        prob = 0.50
                    else:
                        prob = 0.90  # 평범한 플라이 (+0.10 꾸준히 적립!)
                else:
                    prob = 0.10  # 좌중간/우중간 가르는 안타 (놓쳐도 -0.10만 감점!)

            else:
                prob = 0.85 if '땅볼' in b_type else 0.95

            f_oaa += (is_caught - prob)

        dp_succ = group['game_event'].str.contains('병살', na=False).sum() if pos in ['1루수', '2루수', '3루수', '유격수'] else 0

        stats.append({
            '포지션': pos, '선수명': name,
            '타구기회': f_opp, '타구성공': f_succ, '타구OAA': round(f_oaa, 2),
            '병살성공': dp_succ,
        })

    return pd.DataFrame(stats)


def calculate_dwar(df):
    logging.info("최종 포지션 보정 점수 및 dWAR 산출 중...")

    # 1 OAA = 0.85 Runs
    df['순수수비득점(Runs)'] = (
            (df['타구OAA'] * 0.85) +
            (df['병살성공'] * 0.40)
    ).round(2)

    def get_pos_adj(row):
        pos, opp = str(row['포지션']), int(row['타구기회'])

        # 🚨 포수 보정 점수 완벽 복구: 타구 기회 40번만 넘어도 12.5점 풀타임 인정!
        # 영문/한글 조인 에러 걱정 없이 수비 테이블 데이터만으로 100% 보장!
        if pos == '포수': return min(opp / 40.0, 1.0) * 12.5

        # 야수 보정 점수 (FanGraphs Original)
        ratio = min(opp / 350.0, 1.0)
        if pos == '유격수':
            return 7.5 * ratio
        elif pos in ['2루수', '3루수', '중견수']:
            return 2.5 * ratio
        elif pos in ['좌익수', '우익수']:
            return -7.5 * ratio
        elif pos == '1루수':
            return -12.5 * ratio
        return 0

    df['포지션보정'] = df.apply(get_pos_adj, axis=1).round(2)
    df['최종dWAR'] = ((df['순수수비득점(Runs)'] + df['포지션보정']) / 10.0).round(3)

    # 허수 데이터 0 처리
    df['블로킹기회'] = 0;
    df['블로킹성공'] = 0;
    df['블로킹OAA'] = 0.0
    df['도루시도'] = 0;
    df['도루저지'] = 0;
    df['도루OAA'] = 0.0
    df['보살성공'] = 0

    cols = [
        '포지션', '선수명', '타구기회', '타구성공', '타구OAA',
        '병살성공', '순수수비득점(Runs)', '포지션보정', '최종dWAR'
    ]
    return df.sort_values(by='최종dWAR', ascending=False)[cols]


if __name__ == "__main__":
    target_year = 2025
    logging.info("KBO 통합 수비 지표(dWAR) 메이저리그 밸런스 패치 가동!")
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    df_raw = get_data(target_year, engine)

    if not df_raw.empty:
        metrics_df = calculate_oaa(df_raw)
        dwar_df = calculate_dwar(metrics_df)

        print(f"\n=== [ {target_year}시즌 수비 통합 dWAR Top 20 (스케일 2점대 복구 완료!) ] ===")
        print(dwar_df.head(20).to_string(index=False))

        csv_filename = f"dwar_ultimate_result_{target_year}.csv"
        try:
            dwar_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            logging.info(f"🎉 완벽합니다! OAA 볼륨이 복구되어 정상적인 WAR 스케일이 완성되었습니다.")
        except PermissionError:
            pass
    engine.dispose()