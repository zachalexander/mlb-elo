import sys
import subprocess

# pip install requests to /tmp for AWS Lambda
subprocess.call(
    "pip install pandas -t /tmp/ --no-cache-dir".split(),
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
sys.path.insert(1, "/tmp/")

import boto3
import pandas as pd
import io
import math
from datetime import datetime
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clients
s3 = boto3.client("s3")
dynamodb = boto3.client("dynamodb")

# Constants
ELO_K = 20
HOME_ADVANTAGE = 55
ELO_INIT = 1500
MOV_MULTIPLIER = 2.2

# Team abbreviation mapping from 2024 to 2025 format
TEAM_ABBREV_MAP = {
    "WSN": "WSH",
    "KCR": "KC",
    "SFG": "SF",
    "SDP": "SD",
    "CHW": "CWS",
    "ARI": "AZ",
    "OAK": "ATH",
    "TBR": "TB",
}

# S3 config
ELO_2024_BUCKET = "mlb-elo-ratings-output"
ELO_2024_KEY = "elo_rating_end_of_2024.csv"
SCHEDULE_BUCKET = "mlb-schedule-2025"
SCHEDULE_KEY = "schedule_2025_full.csv"
OUTPUT_KEY = "elo-ratings-2025.csv"

# DynamoDB table
DDB_TABLE = "mlb-elo-team-ratings"


def download_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def upload_csv(df, bucket, key):
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())


def calculate_elo_prob(team_elo, opp_elo):
    return 1 / (1 + 10 ** ((opp_elo - team_elo) / 400))


def margin_of_victory_multiplier(score_diff, elo_diff):
    return math.log(abs(score_diff) + 1) * (
        MOV_MULTIPLIER / (0.001 * abs(elo_diff) + MOV_MULTIPLIER)
    )


def update_dynamodb(team, elo, gamePk):
    dynamodb.put_item(
        TableName=DDB_TABLE,
        Item={
            "team": {"S": team},
            "elo": {"N": str(round(elo, 2))},
            "last_updated": {"S": datetime.utcnow().isoformat()},
            "last_gamePk": {"N": str(gamePk)},
        },
    )


def standardize_team_abbreviation(team):
    return TEAM_ABBREV_MAP.get(team, team)


def lambda_handler(event, context):
    elo_df = download_csv(ELO_2024_BUCKET, ELO_2024_KEY)
    # Apply abbreviation conversion to 2024 Elo data
    elo_df["team"] = elo_df["team"].apply(standardize_team_abbreviation)

    schedule_df = download_csv(SCHEDULE_BUCKET, SCHEDULE_KEY)

    team_elo = dict(zip(elo_df["team"], elo_df["elo"]))
    elo_rows = []

    for _, row in schedule_df.iterrows():
        home = standardize_team_abbreviation(row["homeTeam"])
        away = standardize_team_abbreviation(row["awayTeam"])
        home_score = row["homeScore"]
        away_score = row["awayScore"]
        date = row["date"]
        gamePk = row["gamePk"]

        if pd.isna(home_score) or pd.isna(away_score):
            continue  # Unplayed

        # Look up or warn
        if home not in team_elo:
            logger.warning(
                f"Home team {home} not found in 2024 Elo. Using default {ELO_INIT}."
            )
        if away not in team_elo:
            logger.warning(
                f"Away team {away} not found in 2024 Elo. Using default {ELO_INIT}."
            )

        elo_home = team_elo.get(home, ELO_INIT)
        elo_away = team_elo.get(away, ELO_INIT)

        # Adjust for home field
        elo_home_adj = elo_home + HOME_ADVANTAGE
        elo_away_adj = elo_away

        prob_home = calculate_elo_prob(elo_home_adj, elo_away_adj)
        result_home = 1 if home_score > away_score else 0

        score_diff = abs(home_score - away_score)
        mov = margin_of_victory_multiplier(score_diff, elo_home - elo_away)
        elo_shift = ELO_K * mov * (result_home - prob_home)

        # Update ratings
        team_elo[home] = elo_home + elo_shift
        team_elo[away] = elo_away - elo_shift

        # DynamoDB update
        update_dynamodb(home, team_elo[home], gamePk)
        update_dynamodb(away, team_elo[away], gamePk)

        # Track game
        elo_rows.append(
            {
                "date": date,
                "gamePk": gamePk,
                "homeTeam": home,
                "awayTeam": away,
                "homeScore": home_score,
                "awayScore": away_score,
                "homeElo_pre": elo_home,
                "awayElo_pre": elo_away,
                "homeElo_post": team_elo[home],
                "awayElo_post": team_elo[away],
                "elo_shift": elo_shift,
            }
        )

    # Save full file
    output_df = pd.DataFrame(elo_rows)
    upload_csv(output_df, ELO_2024_BUCKET, OUTPUT_KEY)

    return {
        "statusCode": 200,
        "body": f"Successfully updated Elo ratings for {len(output_df)} games and updated DynamoDB.",
    }
