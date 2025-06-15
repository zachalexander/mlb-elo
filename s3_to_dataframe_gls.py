import boto3
import pandas as pd
import io
import zipfile

# AWS S3 settings
bucket = "mlb-game-log-data-retrosheet"
zip_key = "gamelogs/gl1871_2024.zip"  # full path in S3

# Set up S3 client
s3 = boto3.client("s3")

# Retrosheet game log columns (first 11 are usually enough for Elo models)
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

# Download and extract ZIP file from S3
print("Fetching {} from S3...".format(zip_key))
obj = s3.get_object(Bucket=bucket, Key=zip_key)
with zipfile.ZipFile(io.BytesIO(obj["Body"].read())) as z:
    all_dfs = []
    for file_name in z.namelist():
        if file_name.lower().endswith(".txt"):
            print("Processing file:", file_name)
            with z.open(file_name) as f:
                df = pd.read_csv(f, names=columns, usecols=range(len(columns)))
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
                df["source_file"] = file_name
                all_dfs.append(df)

# Combine all TXT files into a single DataFrame
game_logs_df = pd.concat(all_dfs, ignore_index=True)

# Example: preview the result
print(game_logs_df.head())
print(
    "Loaded {} games from {} file(s) in the zip.".format(
        len(game_logs_df), len(all_dfs)
    )
)
