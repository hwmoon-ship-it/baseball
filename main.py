# main.py
import run_expectancy
import win_expectancy_table_generator
import logging


def run_all_analysis(season):
    # 1. RE 계산
    re_df, _ = run_expectancy.calculate_season_re(season)
    re_df.to_csv(f"re_matrix_{season}.csv", index=False)

    # 2. WE 계산
    we_df = we.generate_we_table(season)
    we_df.to_csv(f"we_table_{season}_450.csv", index=False)


if __name__ == "__main__":
    run_all_analysis(2024)