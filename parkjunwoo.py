import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 1. 사용자님이 제공해주신 경기 코드 매핑
GAME_TYPE_MAP = {
    4201: '페넌트레이스',
    4208: '연습경기(1군)',
    4206: '퓨쳐스리그',
    4207: '시범경기',
    4205: '와일드카드결정',
    4204: '준플레이오프',
    4203: '플레이오프',
    4202: '한국시리즈',
    4212: '육성군',
    4213: '연습경기(2군)',
    4214: '교육리그'
}

# 🚨 2. 추출할 선수들의 이름을 리스트에 넣어주세요! (이미지에 있는 이름들)
# 예시로 몇 명만 넣었습니다. 필요하신 선수 이름을 모두 따옴표 안에 쉼표로 구분해 넣어주세요.
PLAYER_LIST = [
    '박준우', '김진욱', '나균안', '김강현', '박정민', '윤성빈', '정철원', '최이준',
    '현도훈', '박세웅', '로드리게스', '김원중', '비슬리', '쿄야마'
]


def get_multi_player_stats(season, engine, players):
    logging.info(f"[{season}] 여러 선수의 구종별 데이터를 단일 쿼리로 초고속 추출 중...")

    # 파이썬 리스트를 SQL의 IN ('A', 'B', 'C') 형태로 변환
    player_tuple = tuple(players)
    if len(players) == 1:
        player_tuple = f"('{players[0]}')"

    # 🚨 단일 테이블(hiball_game_record)만 사용하여 에러 원천 차단
    query = f"""
        SELECT 
            pitcher_name,
            game_type,
            ball_code_name AS pitch_type,
            COUNT(*) AS total_pitches,
            SUM(CASE WHEN strike_yn = 'Y' THEN 1 ELSE 0 END) AS strikes,
            SUM(hit) AS hits,
            SUM(at_bat) AS at_bats
        FROM hiball_game_record
        WHERE season = {season}
          AND pitcher_name IN {player_tuple}
          AND ball_code_name IS NOT NULL 
          AND ball_code_name != ''
        GROUP BY pitcher_name, game_type, ball_code_name
    """

    try:
        df = pd.read_sql(query, engine)
    except Exception as e:
        logging.error(f"🚨 SQL 쿼리 에러 발생: {e}")
        return pd.DataFrame()

    if df.empty:
        logging.warning("조건에 맞는 등판 데이터가 없습니다.")
        return df

    # 3. 경기종류 한글 매핑 (딕셔너리 활용)
    df['경기종류'] = df['game_type'].map(GAME_TYPE_MAP).fillna('기타')

    # 4. 비율 스탯 (스트라이크율, 피안타율) 안전하게 계산
    df['스트라이크율'] = df.apply(
        lambda x: f"{x['strikes'] / x['total_pitches']:.1%}" if x['total_pitches'] > 0 else "0.0%", axis=1
    )
    df['피안타율'] = df.apply(
        lambda x: x['hits'] / x['at_bats'] if x['at_bats'] > 0 else 0.0, axis=1
    )
    df['피안타율'] = df['피안타율'].apply(lambda x: f"{x:.3f}".replace('0.', '.'))

    df = df.rename(columns={
        'pitcher_name': '선수명',
        'pitch_type': '구종',
        'total_pitches': '총투구수',
        'strikes': '스트라이크',
        'hits': '피안타',
        'at_bats': '타수'
    })

    cols = ['선수명', '경기종류', '구종', '총투구수', '스트라이크', '스트라이크율', '타수', '피안타', '피안타율']

    # 선수별 -> 경기종류별 -> 총투구수 내림차순 정렬
    return df[cols].sort_values(by=['선수명', '경기종류', '총투구수'], ascending=[True, True, False])


if __name__ == "__main__":
    target_year = 2026

    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    result_df = get_multi_player_stats(target_year, engine, PLAYER_LIST)

    if not result_df.empty:
        # 🚨 여기서 선수별로 엑셀 시트(탭)를 분리합니다!
        output_filename = f"pitcher_stats_multi_tabs_{target_year}.xlsx"

        # openpyxl 엔진을 사용하여 엑셀 파일 생성
        with pd.ExcelWriter(output_filename, engine='openpyxl') as writer:
            for player in PLAYER_LIST:
                # 해당 선수의 데이터만 필터링 (선수명 컬럼은 시트 이름이 되므로 삭제)
                player_df = result_df[result_df['선수명'] == player].drop(columns=['선수명'])

                # 등판 기록이 있는 선수만 시트를 생성합니다.
                if not player_df.empty:
                    # 엑셀 탭 이름은 최대 31자까지만 가능하므로 안전하게 자르기
                    sheet_name = player[:31]
                    player_df.to_excel(writer, sheet_name=sheet_name, index=False)

        logging.info(f"🎉 성공! 선수별로 탭이 깔끔하게 나뉜 '{output_filename}' 파일이 생성되었습니다.")
    else:
        logging.warning("추출할 데이터가 없습니다.")

    engine.dispose()