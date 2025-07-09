import sys
import subprocess

# pip install requests to /tmp for AWS Lambda
subprocess.call(
    "pip install pandas -t /tmp/ --no-cache-dir".split(),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
sys.path.insert(1, "/tmp/")

subprocess.call(
    "pip install pyarrow -t /tmp/ --no-cache-dir".split(),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
sys.path.insert(1, "/tmp/")

import boto3
import pandas as pd
import numpy as np
import io
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

# AWS setup
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("mlb-elo-team-ratings")

SCHEDULE_BUCKET = "mlb-schedule-2025"
SCHEDULE_KEY = "schedule_2025_full.csv"
OUTPUT_BUCKET = "mlb-predictions-2025"
SIMULATIONS = 10000


def fetch_latest_elo_ratings():
    response = table.scan()
    items = response["Items"]
    latest_ratings = {}
    for item in items:
        team = item["team"]
        date = item["last_updated"]
        elo = float(item["elo"])
        if team not in latest_ratings or date > latest_ratings[team]["last_updated"]:
            latest_ratings[team] = {"elo": elo, "date": date}
    return {team: val["elo"] for team, val in latest_ratings.items()}


def load_schedule():
    obj = s3.get_object(Bucket=SCHEDULE_BUCKET, Key=SCHEDULE_KEY)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    df["date"] = pd.to_datetime(df["date"])
    return df


def win_prob(elo_a, elo_b):
    return 1 / (1 + 10 ** ((elo_b - elo_a) / 400))


def generate_next_game_predictions(schedule_df, elo_ratings):
    today = pd.Timestamp.today().normalize()
    future_games = schedule_df[schedule_df["date"] >= today].copy()
    future_games.sort_values("date", inplace=True)

    next_games = []
    seen_teams = set()
    for _, row in future_games.iterrows():
        home, away, date = row["homeTeam"], row["awayTeam"], row["date"]
        if home not in seen_teams:
            seen_teams.add(home)
            wp = win_prob(elo_ratings[home], elo_ratings[away])
            next_games.append(
                [
                    home,
                    away,
                    date.date(),
                    "home",
                    wp,
                    elo_ratings[home],
                    elo_ratings[away],
                ]
            )
        if away not in seen_teams:
            seen_teams.add(away)
            wp = win_prob(elo_ratings[away], elo_ratings[home])
            next_games.append(
                [
                    away,
                    home,
                    date.date(),
                    "away",
                    wp,
                    elo_ratings[away],
                    elo_ratings[home],
                ]
            )

    df = pd.DataFrame(
        next_games,
        columns=[
            "team",
            "opponent",
            "date",
            "home_away",
            "win_probability",
            "elo_team",
            "elo_opp",
        ],
    )
    s3.put_object(
        Body=df.to_csv(index=False),
        Bucket=OUTPUT_BUCKET,
        Key="next_game_win_expectancy.csv",
    )


def simulate_season(schedule_df, elo_ratings):
    completed = schedule_df.dropna(subset=["homeScore", "awayScore"])
    remaining = schedule_df[
        schedule_df["homeScore"].isna() | schedule_df["awayScore"].isna()
    ]

    teams = sorted(set(schedule_df["homeTeam"]) | set(schedule_df["awayTeam"]))
    team_idx = {team: i for i, team in enumerate(teams)}
    sim_matrix = np.zeros((SIMULATIONS, len(teams)))

    # Seed actual wins
    actual_wins = defaultdict(int)
    for _, row in completed.iterrows():
        if row["homeScore"] > row["awayScore"]:
            actual_wins[row["homeTeam"]] += 1
        else:
            actual_wins[row["awayTeam"]] += 1
    for team, idx in team_idx.items():
        sim_matrix[:, idx] = actual_wins[team]

    # Simulate remaining
    for _, row in remaining.iterrows():
        home, away = row["homeTeam"], row["awayTeam"]
        p_home_win = win_prob(elo_ratings[home], elo_ratings[away])
        draws = np.random.rand(SIMULATIONS)
        home_wins = draws < p_home_win
        sim_matrix[:, team_idx[home]] += home_wins.astype(int)
        sim_matrix[:, team_idx[away]] += (~home_wins).astype(int)

    # Save full simulation matrix
    df_sim = pd.DataFrame(sim_matrix, columns=teams)
    df_sim.insert(0, "sim_id", range(SIMULATIONS))
    table = pa.Table.from_pandas(df_sim)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3.put_object(
        Body=buf.getvalue(), Bucket=OUTPUT_BUCKET, Key="season_win_simulations.parquet"
    )

    # Summary stats
    summary = []
    for team in teams:
        values = df_sim[team].values
        summary.append(
            [
                team,
                round(values.mean(), 2),
                int(np.median(values)),
                round(values.std(), 2),
                int(np.percentile(values, 10)),
                int(np.percentile(values, 25)),
                int(np.percentile(values, 75)),
                int(np.percentile(values, 90)),
            ]
        )
    df_summary = pd.DataFrame(
        summary,
        columns=[
            "team",
            "avg_wins",
            "median_wins",
            "std_dev",
            "p10",
            "p25",
            "p75",
            "p90",
        ],
    )
    s3.put_object(
        Body=df_summary.to_csv(index=False),
        Bucket=OUTPUT_BUCKET,
        Key="season_win_projection_summary.csv",
    )


def lambda_handler(event=None, context=None):
    elo_ratings = fetch_latest_elo_ratings()
    schedule_df = load_schedule()
    generate_next_game_predictions(schedule_df, elo_ratings)
    simulate_season(schedule_df, elo_ratings)
