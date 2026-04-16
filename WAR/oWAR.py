import pandas as pd
from sqlalchemy import create_engine
import logging
import os
from config import DB_CONFIG

# 콘솔 진행 상황 로깅
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ==============================================================================
# 1. KBO 144경기(1296이닝) 기준 포지션 조정 득점 상수 (FanGraphs 기준)
# ==============================================================================
POSITION_ADJUSTMENT_CONSTANTS = {
    '포수': 12.5,
    '유격수': 7.5,
    '2루수': 2.5,
    '중견수': 2.5,
    '3루수': 2.5,
    '좌익수': -7.5,
    '우익수': -7.5,
    '1루수': -12.5,
    '지명타자': -17.5
}


# ==============================================================================
# 2. 리그 상수 로드 (CSV 파일 우선 읽기)
# ==============================================================================
def load_league_constants(season):
    """테스트를 위해 미리 생성해 둔 CSV 파일에서 리그 상수를 가져옵니다."""
    filename = f"league_constants_{season}_raw.csv"

    try:
        df = pd.read_csv(filename)
        logging.info(f"[{season}] 리그 상수 CSV({filename}) 로드 성공!")
        return df.iloc[0].to_dict()
    except FileNotFoundError:
        # 상위 디렉토리 탐색용 방어 코드
        fallback = f"../{filename}"
        try:
            df = pd.read_csv(fallback)
            logging.info(f"[{season}] 리그 상수 CSV({fallback}) 로드 성공!")
            return df.iloc[0].to_dict()
        except:
            logging.error(f"상수 파일({filename})을 찾을 수 없습니다. 상수 생성기를 먼저 실행해주세요.")
            return None


# ==============================================================================
# 3. DB에서 선수별 타격 스탯 추출 (제시해주신 최적화 쿼리 적용)
# ==============================================================================

def get_batter_stats(season, engine):
    logging.info(f"[{season}] DB에서 타자들의 타격 및 포지션 데이터를 추출합니다...")

    query = f"""
        SELECT 
            r.batter_id, 
            r.batter_name,

            -- [수정 1] COUNT 대신 전임자가 만든 플래그 컬럼(0 또는 1)의 SUM을 사용하여 정확한 타석과 안타를 구합니다.
            SUM(r.plate_appearance) AS PA,
            SUM(r.at_bat) AS AB,
            SUM(r.single) AS 1B,
            SUM(r.doubles) AS 2B,
            SUM(r.triples) AS 3B,
            SUM(r.home_runs) AS HR,
            SUM(r.base_on_balls) AS BB,
            SUM(r.hit_by_pitch) AS HBP,
            SUM(r.intentional_walks) AS IBB,
            SUM(r.sacrifice_flies_out) AS SF,

            -- [수정 2] 조인된 로스터 서브쿼리에서 가장 많이 뛴 '진짜 주 포지션'을 가져옵니다.
            MAX(ros.position) AS main_position

        FROM hiball_game_record r

        -- [핵심] 해당 시즌에 각 선수가 가장 많이 출전한 포지션을 1순위(rn=1)로 뽑아오는 완벽한 서브쿼리
        LEFT JOIN (
            SELECT player_id, position
            FROM (
                SELECT player_id, position, 
                       ROW_NUMBER() OVER(PARTITION BY player_id ORDER BY COUNT(*) DESC) as rn
                FROM hiball_game_roster
                WHERE season = {season} AND position IS NOT NULL
                GROUP BY player_id, position
            ) tmp
            WHERE rn = 1
        ) ros ON r.batter_id = ros.player_id

        WHERE r.season = {season} 
          AND r.game_type = 4201 
        GROUP BY 
            r.batter_id, 
            r.batter_name
        HAVING PA > 0
    """

    df = pd.read_sql(query, engine)
    return df


# ==============================================================================
# 4. 순수 타격 기여도 (oWAR) 계산 핵심 로직 - oWAR은 순수 공력력만 계산하므로 포지션 조정 빠짐
# ==============================================================================
def calculate_pure_owar(batter_df, constants):
    logging.info("선수별 wOBA, wRAA 및 최종 oWAR 계산을 시작합니다...")

    # 1. 선수 개인의 wOBA 계산 (고의사구 IBB는 제외)
    # 분모: AB + BB - IBB + SF + HBP
    batter_df['wOBA_den'] = batter_df['AB'] + batter_df['BB'] - batter_df['IBB'] + batter_df['SF'] + batter_df['HBP']

    # 분자: (볼넷가치*볼넷) + (사구가치*사구) + (단타가치*단타) + ...
    batter_df['wOBA_num'] = (
            (constants['w_bb'] * (batter_df['BB'] - batter_df['IBB'])) +
            (constants['w_hbp'] * batter_df['HBP']) +
            (constants['w_b1b'] * batter_df['1B']) +
            (constants['w_b2b'] * batter_df['2B']) +
            (constants['w_b3b'] * batter_df['3B']) +
            (constants['w_hr'] * batter_df['HR'])
    )

    # wOBA 산출
    batter_df['wOBA'] = batter_df.apply(
        lambda x: x['wOBA_num'] / x['wOBA_den'] if x['wOBA_den'] > 0 else 0, axis=1
    )

    # 2. 타격 득점 기여도 (wRAA)
    # 계산식: ((선수 wOBA - 리그평균 wOBA) / wOBA 스케일) * 타석(PA)
    batter_df['wRAA'] = ((batter_df['wOBA'] - constants['w_oba']) / constants['scale']) * batter_df['PA']

    # 포지션 보정 (Pos_Adj)은 계산해서 컬럼으로 남겨두되, oWAR에는 섞지 않습니다! (나중에 Total WAR에서 합산)
    def get_pos_adj(row):
        pos_val = POSITION_ADJUSTMENT_CONSTANTS.get(row['main_position'], -17.5)
        return pos_val * (row['PA'] / 600.0)

    batter_df['Pos_Adj'] = batter_df.apply(get_pos_adj, axis=1)
    batter_df['Rep_Runs'] = batter_df['PA'] * 0.0333

    # 🚨 [핵심 수정] oWAR 공식에서 Pos_Adj를 과감하게 제거합니다!
    # oWAR = (타격 득점 + 대체선수 득점) / 1승당 득점
    batter_df['oWAR'] = (batter_df['wRAA'] + batter_df['Rep_Runs']) / constants['r_win']

    result_cols = ['batter_name', 'main_position', 'PA', 'wOBA', 'wRAA', 'Pos_Adj', 'Rep_Runs', 'oWAR']
    batter_df = batter_df[result_cols].round(3)

    return batter_df.sort_values(by='oWAR', ascending=False).reset_index(drop=True)


# ==============================================================================
# 메인 실행부
# ==============================================================================
if __name__ == "__main__":
    target_year = 2025  # 테스트할 연도

    # 1. csv 파일에서 당해 연도 리그 상수 읽어오기
    constants = load_league_constants(target_year)

    if constants:
        # DB 연결 엔진 생성
        db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)

        try:
            # 2. 쿼리 실행 및 데이터 추출
            df_batters = get_batter_stats(target_year, engine)

            if not df_batters.empty:
                # 3. oWAR 계산 엔진 가동
                df_owar = calculate_pure_owar(df_batters, constants)

                print(f"\n=== [ {target_year}시즌 타격 oWAR (공격 승리기여도) Top 20 ] ===")
                print(df_owar.head(20).to_string(index=False))

                # 결과를 CSV 파일로 추출
                csv_filename = f"owar_result_{target_year}.csv"
                df_owar.to_csv(csv_filename, index=False, encoding='utf-8-sig')
                logging.info(f"🎉 순수 oWAR 산출 완료! 결과가 '{csv_filename}'에 저장되었습니다.")
            else:
                logging.warning(f"[{target_year}] 해당 연도의 타격 데이터가 조회되지 않았습니다.")

        except Exception as e:
            logging.error(f"데이터베이스 쿼리 실행 중 오류가 발생했습니다: {e}")

        finally:
            engine.dispose()