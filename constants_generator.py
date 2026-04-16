import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os
import sys
import datetime
from config import DB_CONFIG

# 콘솔에 진행 상황을 알려주기 위한 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ==============================================================================
# 1. 득점 기대치(RE24) 매트릭스 로드 (컬럼명 자동 호환 패치)
# ==============================================================================
def load_re_matrix(season):
    filename = f"yearly_re_matrix_{season}.csv"
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            df = pd.read_csv(filename, encoding=enc)

            # 1) 우리가 새로 만든 파일 포맷인 경우 (자동 호환 변환)
            if all(col in df.columns for col in ['season', 'out_count', 'runner_state', 'value']):
                logging.info(f"RE Matrix 로드 성공: {filename} (인코딩: {enc})")
                # 기존 계산 엔진이 인식할 수 있도록 컬럼명 강제 변경
                df = df.rename(columns={
                    'season': 'Season',
                    'out_count': 'OUT',
                    'runner_state': 'STATE',
                    'value': 'RE'
                })
                return df

            # 2) 예전 기존 파일 포맷인 경우
            elif all(col in df.columns for col in ['Season', 'OUT', 'STATE', 'RE']):
                logging.info(f"RE Matrix 로드 성공: {filename} (인코딩: {enc})")
                return df

        except Exception:
            continue

    logging.error(f"{filename} 파일을 찾을 수 없거나 형식이 잘못되었습니다. (컬럼명을 확인해주세요)")
    return None


# ==============================================================================
# 2. 동적 타격 가치(Run Value) 산출 엔진
# ==============================================================================
def get_dynamic_linear_weights(season, engine, re_matrix):
    logging.info(f"[{season}] 타석 데이터 추출 및 Run Value 계산 중...")

    query = f"""
        SELECT season, hit_result_name, 
               before_out_count, before_runner_state,
               CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
               CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
               ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced
        FROM hiball_game_record
        WHERE game_type = 4201 AND season = {season}
          AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
          AND hit_result_name IS NOT NULL
    """
    df_plays = pd.read_sql(query, engine)

    re_matrix['Season'] = re_matrix['Season'].astype(int)
    re_matrix['OUT'] = re_matrix['OUT'].astype(int)
    re_matrix['STATE'] = re_matrix['STATE'].astype(int)
    re_dict = re_matrix.set_index(['Season', 'OUT', 'STATE'])['RE'].to_dict()

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
    rv_avg = df_plays.groupby('hit_result_name')['rv'].mean()

    out_events = ['땅볼아웃', '플라이아웃', '삼진아웃', '직선타아웃', '파울플라이아웃', '낫아웃터치아웃']
    available_outs = [e for e in out_events if e in rv_avg.index]
    rv_out = rv_avg[available_outs].mean() if available_outs else -0.28

    uw_weights = {
        'uw_1b': rv_avg.get('1루타', 0.505) - rv_out,
        'uw_2b': rv_avg.get('2루타', 0.760) - rv_out,
        'uw_3b': rv_avg.get('3루타', 1.050) - rv_out,
        'uw_hr': rv_avg.get('홈런', 1.420) - rv_out,
        'uw_bb': rv_avg.get('볼넷', 0.315) - rv_out,
        'uw_hbp': rv_avg.get('사구(HP)', 0.335) - rv_out
    }

    return uw_weights, abs(rv_out)


# ==============================================================================
# 3. 리그 누적 데이터 집계
# ==============================================================================
def get_league_totals(season, engine):
    logging.info(f"[{season}] 리그 누적 데이터(PA, AB, HR 등) 집계 중...")

    query = f"""
        SELECT 
            SUM(plate_appearance) as total_pa, SUM(at_bat) as total_ab,
            SUM(single) as total_1b, SUM(doubles) as total_2b,
            SUM(triples) as total_3b, SUM(home_runs) as total_hr,
            SUM(base_on_balls) as total_bb, SUM(hit_by_pitch) as total_hbp,
            SUM(intentional_walks) as total_ibb, SUM(strike_out) as total_so,
            SUM(sacrifice_flies_out) as total_sf, SUM(infield_fly_out) as total_iff,
            SUM(CASE WHEN memo_name LIKE '%도루성공%' THEN 1 ELSE 0 END) as total_sb,
            SUM(CASE WHEN memo_name LIKE '%도루실패%' THEN 1 ELSE 0 END) as total_cs,
            SUM(CASE WHEN out_count > before_out_count THEN (out_count - before_out_count) ELSE 0 END) / 3.0 as total_ip,
            SUM(home_score - before_home_score + away_score - before_away_score) as total_r
        FROM hiball_game_record 
        WHERE season = {season} AND game_type = 4201
    """
    df = pd.read_sql(query, engine)
    if df.empty or pd.isna(df.iloc[0]['total_pa']):
        return None
    return df.iloc[0].to_dict()


# ==============================================================================
# 4. 세이버메트릭스 상수 산출 로직
# ==============================================================================
def calculate_sabermetrics_constants(season):
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    re_matrix = load_re_matrix(season)
    if re_matrix is None:
        engine.dispose()
        return None

    lg = get_league_totals(season, engine)
    if not lg or lg['total_pa'] == 0:
        logging.warning(f"[{season}] 데이터가 부족하여 산출할 수 없습니다.")
        engine.dispose()
        return None

    uw_weights, run_out_abs = get_dynamic_linear_weights(season, engine, re_matrix)
    engine.dispose()

    logging.info(f"[{season}] 상수 수학적 계산 적용 중...")

    ra9 = (lg['total_r'] * 9) / lg['total_ip'] if lg['total_ip'] > 0 else 0
    era = ra9 * (4.31 / 4.80)
    adjustment = ra9 - era

    fip_num = (13 * lg['total_hr']) + (3 * (lg['total_bb'] + lg['total_hbp'] - lg['total_ibb'])) - (2 * lg['total_so'])
    raw_fip = fip_num / lg['total_ip'] if lg['total_ip'] > 0 else 0
    c_fip = era - raw_fip

    iffip_num = fip_num - (2 * lg['total_iff'])
    raw_iffip = iffip_num / lg['total_ip'] if lg['total_ip'] > 0 else 0
    c_iffip = era - raw_iffip

    wOBA_num = (uw_weights['uw_bb'] * (lg['total_bb'] - lg['total_ibb']) +
                uw_weights['uw_hbp'] * lg['total_hbp'] +
                uw_weights['uw_1b'] * lg['total_1b'] +
                uw_weights['uw_2b'] * lg['total_2b'] +
                uw_weights['uw_3b'] * lg['total_3b'] +
                uw_weights['uw_hr'] * lg['total_hr'])
    wOBA_den = lg['total_pa'] - lg['total_ibb']
    wOBA_unscaled = wOBA_num / wOBA_den if wOBA_den > 0 else 0

    total_h = lg['total_1b'] + lg['total_2b'] + lg['total_3b'] + lg['total_hr']
    obp_num = total_h + lg['total_bb'] + lg['total_hbp']
    obp_den = lg['total_ab'] + lg['total_bb'] + lg['total_hbp'] + lg['total_sf']
    obp = obp_num / obp_den if obp_den > 0 else 0

    scale = obp / wOBA_unscaled if wOBA_unscaled > 0 else 1.2
    league_woba = wOBA_unscaled * scale

    r_win = (ra9 * 1.5) + 3
    run_sb = 0.200
    run_cs = -(2 * run_out_abs + 0.075)

    lgwSB_den = lg['total_1b'] + lg['total_bb'] + lg['total_hbp'] - lg['total_ibb']
    lgwSB = ((lg['total_sb'] * run_sb) + (lg['total_cs'] * run_cs)) / lgwSB_den if lgwSB_den > 0 else 0

    result = {
        'season': season,
        'w_oba': round(league_woba, 3),
        'scale': round(scale, 3),
        'obp': round(obp, 3),
        'c_fip': round(c_fip, 2),
        'c_iffip': round(c_iffip, 2),
        'ra9': round(ra9, 2),
        'era': round(era, 2),
        'adjustment': round(adjustment, 3),
        'r_pa': round(lg['total_r'] / lg['total_pa'], 3) if lg['total_pa'] > 0 else 0,
        'r_win': round(r_win, 2),
        'run_cs': round(run_cs, 3),
        'run_sb': round(run_sb, 3),
        'lgwSB': round(lgwSB, 5),
        'w_b1b': round(uw_weights['uw_1b'] * scale, 3),
        'w_b2b': round(uw_weights['uw_2b'] * scale, 3),
        'w_b3b': round(uw_weights['uw_3b'] * scale, 3),
        'w_hr': round(uw_weights['uw_hr'] * scale, 3),
        'w_bb': round(uw_weights['uw_bb'] * scale, 3),
        'w_hbp': round(uw_weights['uw_hbp'] * scale, 3),
        'fip': round(era, 2),
        'iffip': round(era, 2),
        'fipr9': round(era + adjustment, 3),
        'iffipr9': round(era + adjustment, 3)
    }

    return result


# ==============================================================================
# 5. DB 자동 업데이트 함수
# ==============================================================================
def update_constants_to_db(constants_dict, table_name="league_constants"):
    """산출된 리그 상수를 DB에 UPSERT(Insert or Update) 합니다."""
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    columns = ', '.join(constants_dict.keys())
    placeholders = ', '.join([f":{key}" for key in constants_dict.keys()])
    updates = ', '.join([f"{key} = VALUES({key})" for key in constants_dict.keys() if key != 'season'])

    query = text(f"""
        INSERT INTO {table_name} ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query, constants_dict)
        logging.info(f"[{constants_dict['season']}] 시즌 리그 상수가 DB({table_name})에 성공적으로 반영되었습니다.")
    except Exception as e:
        logging.error(f"DB 업데이트 중 오류 발생: {e}")
    finally:
        engine.dispose()


# ==============================================================================
# 메인 실행부 (배치 스케줄러용)
# ==============================================================================
if __name__ == "__main__":
    target_year = 2026  # 산출하고자 하는 타겟 연도
    prev_year = target_year - 1

    today = datetime.date.today()
    cutoff_date = datetime.date(target_year, 6, 1)  # 6월 1일 기준

    logging.info(f"실행일자: {today} / 당해 연도 로직 분기 기준일: {cutoff_date}")

    # ==========================================================================
    # [트랙 1] 전년도(prev_year) 데이터는 무조건 계산해서 CSV 저장
    # ==========================================================================
    logging.info(f"전년도({prev_year}년) 데이터를 계산하여 CSV를 추출합니다.")
    prev_constants = calculate_sabermetrics_constants(prev_year)

    if prev_constants:
        df_prev = pd.DataFrame([prev_constants])
        csv_filename_prev = f"league_constants_{prev_year}_raw.csv"
        try:
            df_prev.to_csv(csv_filename_prev, index=False, encoding='utf-8-sig')
            logging.info(f"🎉 [CSV 추출 성공] 전년도 데이터가 '{csv_filename_prev}'에 저장되었습니다.")
        except PermissionError:
            df_prev.to_csv(f"league_constants_{prev_year}_raw_new.csv", index=False, encoding='utf-8-sig')

    # ==========================================================================
    # [트랙 2] 날짜 분기 처리 (당해 연도 CSV 추출 여부 & DB 반영 로직)
    # ==========================================================================
    final_constants_for_db = None

    if today < cutoff_date:
        logging.info(f"아직 {cutoff_date} 이전이므로, 당해 연도({target_year}년) CSV는 아직 생성하지 않습니다.")
        logging.info(f"대신, DB 업데이트는 안전하게 전년도({prev_year}년) 상수를 당해 연도로 복사하여 덮어씁니다.")

        if prev_constants:
            final_constants_for_db = prev_constants.copy()
            final_constants_for_db['season'] = target_year  # 연도만 2026으로 업데이트하여 DB에 넣음

    else:
        logging.info(f"{cutoff_date}이 지났으므로 당해 연도({target_year}년) 데이터를 추가로 계산하여 CSV를 생성합니다.")
        current_constants = calculate_sabermetrics_constants(target_year)

        if current_constants:
            df_current = pd.DataFrame([current_constants])
            csv_filename_curr = f"league_constants_{target_year}_raw.csv"
            try:
                df_current.to_csv(csv_filename_curr, index=False, encoding='utf-8-sig')
                logging.info(f"🎉 [CSV 추출 성공] 당해 연도 데이터가 '{csv_filename_curr}'에 저장되었습니다.")
            except PermissionError:
                df_current.to_csv(f"league_constants_{target_year}_raw_new.csv", index=False, encoding='utf-8-sig')

        # 6월 1일 이후에는 진짜 당해 연도 데이터를 DB에 반영
        final_constants_for_db = current_constants

    # ==========================================================================
    # [트랙 3] DB 업데이트 전 Sanity Check (유효성 검사)
    # ==========================================================================
    if final_constants_for_db:
        is_safe = True

        if not (1.05 <= final_constants_for_db['scale'] <= 1.30):
            logging.critical(f"[경고] Scale 값이 비정상입니다: {final_constants_for_db['scale']}")
            is_safe = False

        if not (2.80 <= final_constants_for_db['c_fip'] <= 4.20):
            logging.critical(f"[경고] c_fip 값이 비정상입니다: {final_constants_for_db['c_fip']}")
            is_safe = False

        if not (8.0 <= final_constants_for_db['r_win'] <= 12.0):
            logging.critical(f"[경고] r_win(승리 득점)이 비정상입니다: {final_constants_for_db['r_win']}")
            is_safe = False

        if is_safe:
            logging.info("데이터 유효성 검사 통과. 수치가 안정적이므로 DB 업데이트를 진행합니다.")
            update_constants_to_db(final_constants_for_db, table_name="league_constants")
            logging.info("오늘의 세이버메트릭스 상수 배치 작업이 성공적으로 완료되었습니다.")
        else:
            logging.critical("[업데이트 차단] 상수가 정상 범위를 벗어났습니다. 사이트 데이터 오염을 막기 위해 오늘 배치는 DB에 반영하지 않습니다!")
            sys.exit(1)
    else:
        logging.error("DB 업데이트용 상수 산출에 실패하여 배치를 종료합니다.")