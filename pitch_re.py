# pitch_re_matrix_288.py
import pandas as pd
import mysql.connector
import logging
from config import DB_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def generate_288_matrix():
    # 1. RE24 (상황별 기준값) 로드
    try:
        re_df = pd.read_csv('yearly_re_matrix_2025.csv', encoding='utf-8-sig')
        # 2016-2025 전체 평균을 기준으로 사용 (필요 시 특정 연도 필터링 가능)
        re24 = re_df.groupby(['OUT', 'STATE'])['RE'].mean().to_dict()
        logging.info("RE24 참조 테이블 로드 완료.")
    except Exception as e:
        logging.error(f"yearly_re_matrix_2025.csv 로드 실패: {e}")
        return

    # 2. 2025년 투구 데이터 추출 (PBP)
    logging.info("DB에서 2025년 투구 데이터 추출 중...")
    conn = mysql.connector.connect(**DB_CONFIG)
    query = """
    SELECT inning, inning_tb, before_out_count, before_ball_count, before_strike_count, before_runner_state, before_away_score, before_home_score,
           out_count, ball_count, strike_count, runner_state, away_score, home_score, hit_result_code
    FROM hiball_game_record
    WHERE season = 2025 AND game_type = 4201
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # 필수 데이터 결측치 제거
    df = df.dropna(subset=['before_out_count', 'before_runner_state', 'before_ball_count', 'before_strike_count'])

    # 3. 타석(PA) 식별 및 결과 가치 산출
    logging.info("타석별 최종 가치(PA Outcome) 계산 중...")
    df['is_terminal'] = df['hit_result_code'] != 0
    df['pa_id'] = (df['is_terminal'].shift(1).fillna(False)).cumsum()

    def get_pa_end_value(group):
        # 타석의 마지막 결과
        last = group.iloc[-1]
        first = group.iloc[0]

        # 1) 타석 종료 후 상황의 RE24 가치
        e_out, e_state = int(last['out_count']), int(last['runner_state'])
        re_post = 0.0 if e_out >= 3 else re24.get((e_out, e_state), 0.0)

        # 2) 타석 내 발생한 총 득점
        runs = (last['away_score'] + last['home_score']) - (first['before_away_score'] + first['before_home_score'])

        # 타석의 최종 가치 (종료 상황 RE + 득점)
        group['pa_final_val'] = re_post + runs
        return group

    # 모든 투구행에 해당 타석의 최종 결과 가치를 매칭
    df = df.groupby('pa_id', group_keys=False).apply(get_pa_end_value)

    # 4. 288개 상태별 절대 평균 RE 계산
    logging.info("288개 상태별 평균값 집계 중...")
    matrix_abs = df.groupby([
        'before_out_count', 'before_runner_state', 'before_ball_count', 'before_strike_count'
    ])['pa_final_val'].mean().reset_index()

    matrix_abs.columns = ['OUT', 'STATE', 'BALL', 'STRIKE', 'ABS_RE']

    # 5. [핵심] 0-0 기준 정규화 (사용자님 요청사항 반영)
    # 각 상황(24개)별로 0-0 카운트의 값을 0으로 맞춤
    def normalize_count(group):
        # 해당 상황의 0볼 0스트라이크 시점의 평균 기대값 추출
        zero_zero_row = group[(group['BALL'] == 0) & (group['STRIKE'] == 0)]

        if not zero_zero_row.empty:
            baseline = zero_zero_row['ABS_RE'].values[0]
        else:
            # 데이터가 부족해 0-0이 없는 경우 RE24의 상황 기본값 사용
            baseline = re24.get((group.name[0], group.name[1]), 0.0)

        group['COUNT_VALUE'] = group['ABS_RE'] - baseline
        return group

    logging.info("0-0 카운트 기준 정규화 적용 중...")
    matrix_288 = matrix_abs.groupby(['OUT', 'STATE'], group_keys=False).apply(normalize_count)

    # 6. 최종 파일 저장
    matrix_288.to_csv("pitch_re_matrix_288.csv", index=False, encoding='utf-8-sig')
    logging.info("성공! 288 Matrix(0-0 Baseline)가 생성되었습니다: pitch_re_matrix_288.csv")

    # 샘플 출력 (무사 무주자)
    print("\n[ 무사 무주자 상황 볼카운트별 가치 (0-0 = 0.0) ]")
    sample = matrix_288[(matrix_288['OUT'] == 0) & (matrix_288['STATE'] == 0)]
    print(sample.pivot(index='BALL', columns='STRIKE', values='COUNT_VALUE').round(4))


if __name__ == "__main__":
    generate_288_matrix()