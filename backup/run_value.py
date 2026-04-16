# run_value.py
import pandas as pd
from sqlalchemy import create_engine
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_re_matrix():
    for enc in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            df = pd.read_csv("yearly_re_matrix_2024.csv", encoding=enc)
            if all(col in df.columns for col in ['Season', 'OUT', 'STATE', 'RE']):
                logging.info(f"RE Matrix 로드 성공 (인코딩: {enc})")
                return df
        except:
            continue
    return None


def calculate_yearly_run_values():
    logging.info("1. DB에서 타석 데이터 추출...")
    db_url = f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(db_url)

    # [수정 완료] JOIN을 아예 없애고, 이미 테이블에 있는 hit_result_name 컬럼을 바로 씁니다!
    query = """
            SELECT season, hit_result_name, 
                   before_out_count, before_runner_state,
                   CASE WHEN out_count >= 3 THEN 3 ELSE out_count END AS next_out_count,
                   CASE WHEN out_count >= 3 THEN 0 ELSE runner_state END AS next_runner_state,
                   ((home_score + away_score) - (before_home_score + before_away_score)) AS runs_produced
            FROM hiball_game_record
            WHERE game_type = 4201 AND season = 2024
              AND before_out_count IS NOT NULL AND before_runner_state IS NOT NULL
              AND hit_result_name IS NOT NULL
        """
    df_plays = pd.read_sql(query, engine)

    # 연결 종료 (SQLAlchemy는 커넥션 풀을 쓰므로 engine.dispose()를 쓸 수도 있습니다)
    engine.dispose()

    re_matrix = load_re_matrix()
    if re_matrix is None:
        logging.error("yearly_re_matrix_2024.csv 로드 실패.")
        return

    re_matrix['Season'] = re_matrix['Season'].astype(int)
    re_matrix['OUT'] = re_matrix['OUT'].astype(int)
    re_matrix['STATE'] = re_matrix['STATE'].astype(int)
    re_dict = re_matrix.set_index(['Season', 'OUT', 'STATE'])['RE'].to_dict()

    logging.info("2. Run Value 계산 시작...")

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

    logging.info("3. 가중치 집계 및 저장...")
    summary = df_plays.groupby(['season', 'hit_result_name'])['rv'].agg(['mean', 'count']).reset_index()
    summary.columns = ['Season', 'hit_result_name', 'Run_Value', 'Freq']

    # index를 이름으로, columns를 연도로 하는 보기 좋은 매트릭스 생성
    pivot_rv = summary.pivot(index='hit_result_name', columns='Season', values='Run_Value')
    pivot_rv.to_csv("yearly_rv_matrix_2024.csv", encoding='utf-8-sig')

    logging.info("yearly_re_matrix_2024.csv 생성 완료. (이름 변환 적용)")
    return pivot_rv


if __name__ == "__main__":
    calculate_yearly_run_values()