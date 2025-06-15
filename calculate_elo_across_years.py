import boto3
import pandas as pd
import io
import zipfile

# S3 settings
bucket = "mlb-game-log-data-retrosheet"
zip_key = "gamelogs/gl1871_2024.zip"

s3 = boto3.client("s3")
columns = [
    "date",
    "game_num",
    "day",
    "visiting_team",
    "visiting_league",
    "visiting_game_num",
    "home_team",
    "home_league",
    "home_game_num",
    "visiting_score",
    "home_score",
]

# Download ZIP from S3
obj = s3.get_object(Bucket=bucket, Key=zip_key)
with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as z:
    all_dfs = []
    for file_name in z.namelist():
        if file_name.lower().endswith(".txt"):
            with z.open(file_name) as f:
                df = pd.read_csv(f, names=columns, usecols=range(len(columns)))
                df["date"] = pd.to_datetime(
                    df["date"], format="%Y%m%d", errors="coerce"
                )
                all_dfs.append(df)

# Combine and clean
game_logs_df = pd.concat(all_dfs).dropna(subset=["date"])

# Elo setup
teams = pd.concat([game_logs_df["home_team"], game_logs_df["visiting_team"]]).unique()
elo_ratings = {team: 1500 for team in teams}


def expected_score(r1, r2):
    return 1 / (1 + 10 ** ((r2 - r1) / 400))


def update_elo(winner, loser, k=20):
    exp = expected_score(winner, loser)
    return winner + k * (1 - exp), loser - k * (1 - exp)


elo_history = []
for _, row in game_logs_df.sort_values("date").iterrows():
    home, away = row["home_team"], row["visiting_team"]
    hs, vs = row["home_score"], row["visiting_score"]
    if home not in elo_ratings or away not in elo_ratings:
        continue
    if not (isinstance(hs, (int, float)) and isinstance(vs, (int, float))):
        continue
    home_elo, away_elo = elo_ratings[home], elo_ratings[away]
    if hs > vs:
        home_elo, away_elo = update_elo(home_elo, away_elo)
    elif vs > hs:
        away_elo, home_elo = update_elo(away_elo, home_elo)
    elo_ratings[home], elo_ratings[away] = home_elo, away_elo
    elo_history.append(
        {
            "date": row["date"],
            "home_team": home,
            "away_team": away,
            "home_score": hs,
            "away_score": vs,
            "home_elo_post": home_elo,
            "away_elo_post": away_elo,
        }
    )

# Save to CSV
pd.DataFrame(elo_history).to_csv("elo_ratings_by_game.csv", index=False)
print("Elo ratings saved to elo_ratings_by_game.csv")
