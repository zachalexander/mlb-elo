# =============================================
# Elo Rating Calculation Pipeline for MLB Teams
# =============================================
#
# This script processes Retrosheet historical MLB game logs (1871–2024) to:
# 1. Extract game logs from a zip file in S3
# 2. Preprocess and map team codes to modern franchise IDs
# 3. Calculate Elo ratings for each game
# 4. Output the following files to S3:
#    - elo-ratings-1871-to-2023.csv : Elo ratings for games before 2024
#    - elo-ratings-2024.csv         : Elo ratings for 2024 season
#    - elo_rating_end_of_2024.csv   : Final Elo ratings for each team at the end of 2024
#    - excluded_games.csv           : List of excluded games due to unmapped teams
#
# Includes logging and error handling for transparency and robustness.
#
# Elo enhancements:
# - Home Field Advantage (+35 Elo boost for home team)
# - Margin of Victory Scaling (larger Elo shifts for blowout wins)
# - All enhancements are toggleable using USE_HOME_FIELD_ADVANTAGE and USE_MARGIN_OF_VICTORY

import boto3
import zipfile
import io
import pandas as pd
from datetime import datetime
import logging
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

s3 = boto3.client('s3')
BUCKET_NAME = 'mlb-game-log-data-retrosheet'
OUTPUT_BUCKET = 'mlb-elo-ratings-output'
GAMELOG_KEY = 'gamelogs/gl1871_2024.zip'

TEAM_NAME_URL = 'https://www.retrosheet.org/CurrentNames.csv'

def load_team_mappings():
    try:
        response = requests.get(TEAM_NAME_URL)
        response.raise_for_status()
        df = pd.read_csv(io.StringIO(response.text), header=None)
        df.columns = ['retro_id', 'mlb_id', 'league', 'unused1', 'city', 'nickname', 'unused2', 'start_date', 'end_date', 'alt_city', 'alt_abbrev']
        mapping = dict(zip(df['retro_id'], df['mlb_id']))
        current_teams = set(mapping.values())
        logging.info(f"Loaded {len(mapping)} team mappings from Retrosheet.")
        return mapping, current_teams
    except Exception as e:
        logging.error(f"Failed to load team mapping file: {e}")
        raise

TEAM_ABBREV_MAP, CURRENT_TEAMS = load_team_mappings()

def extract_game_logs():
    try:
        zip_obj = s3.get_object(Bucket=BUCKET_NAME, Key=GAMELOG_KEY)['Body'].read()
        all_games = []
        with zipfile.ZipFile(io.BytesIO(zip_obj)) as z:
            txt_files = [name for name in z.namelist() if name.endswith('.txt')]
            if not txt_files:
                raise ValueError("No .txt files found inside the zip archive.")
            for txt_file in txt_files:
                df = pd.read_csv(z.open(txt_file), header=None, dtype=str)
                if df.shape[1] >= 77:
                    df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 71, 72, 73, 74, 75, 76]]
                    df.columns = [
                        'date', 'game_number', 'day_of_week', 'visiting_team', 'visiting_league',
                        'visiting_game_number', 'home_team', 'home_league', 'home_game_number',
                        'visitor_score', 'home_score', 'length_outs', 'day_night', 'completion_info',
                        'forfeit_info', 'protest_info', 'park_id', 'attendance', 'time_of_game',
                        'visiting_line_scores', 'home_line_scores', 'visitor_pitchers',
                        'home_pitchers', 'umpires', 'other', 'source_file']
                    df['source_file'] = txt_file
                    all_games.append(df)
                else:
                    logging.warning(f"Skipping file {txt_file} due to unexpected number of columns: {df.shape[1]}")
        if all_games:
            combined_df = pd.concat(all_games, ignore_index=True)
            logging.info(f"Extracted and combined {len(all_games)} game logs from {GAMELOG_KEY}.")
            return combined_df
        else:
            raise ValueError("No valid game logs extracted from the archive.")
    except Exception as e:
        logging.error(f"Failed to extract game logs from {GAMELOG_KEY}: {e}")
        raise

def preprocess(df):
    try:
        df['date'] = pd.to_datetime(df['date'].str.strip(), format='%Y%m%d', errors='coerce')

        original_len = len(df)
        df['home_team_mapped'] = df['home_team'].map(TEAM_ABBREV_MAP)
        df['visiting_team_mapped'] = df['visiting_team'].map(TEAM_ABBREV_MAP)

        df['home_team'] = df['home_team_mapped'].combine_first(df['home_team'])
        df['visiting_team'] = df['visiting_team_mapped'].combine_first(df['visiting_team'])

        excluded_df = df[df['home_team'].isna() | df['visiting_team'].isna()]
        if not excluded_df.empty:
            excluded_teams = pd.concat([
                excluded_df[['home_team']],
                excluded_df[['visiting_team']]
            ]).dropna().drop_duplicates().values.flatten().tolist()
            logging.warning(f"Excluding {len(excluded_df)} games due to unmapped teams.")
            logging.warning(f"Unique excluded team codes: {sorted(set(excluded_teams))}")

            excluded_df.to_csv('/tmp/excluded_games.csv', index=False)
            with open('/tmp/excluded_games.csv', 'rb') as f:
                s3.put_object(Bucket=OUTPUT_BUCKET, Key='excluded_games.csv', Body=f.read())

        df = df[df['home_team'].notna() & df['visiting_team'].notna()]
        df = df.dropna(subset=['date'])
        logging.info(f"Preprocessed dataframe with {len(df)} games (excluded {original_len - len(df)} games).")
        logging.info(f"Date range of games: {df['date'].min().date()} to {df['date'].max().date()}")
        return df.sort_values(by='date')
    except Exception as e:
        logging.error(f"Error calculating Elo ratings: {e}")
        raise

# Elo enhancement toggles
USE_HOME_FIELD_ADVANTAGE = True
USE_MARGIN_OF_VICTORY = True

def calculate_elo(df):
    try:
        base_elo = 1500
        k = 20
        team_elos = {}
        results = []

        for _, row in df.iterrows():
            date = row['date']
            home = row['home_team']
            away = row['visiting_team']
            home_score = int(row['home_score'])
            away_score = int(row['visitor_score'])

            home_elo = team_elos.get(home, base_elo)
            away_elo = team_elos.get(away, base_elo)

            if USE_HOME_FIELD_ADVANTAGE:
                home_field_advantage = 35
            else:
                home_field_advantage = 0

            expected_home = 1 / (1 + 10 ** ((away_elo - (home_elo + home_field_advantage)) / 400))
            expected_away = 1 - expected_home

            if home_score > away_score:
                actual_home, actual_away = 1, 0
            elif home_score < away_score:
                actual_home, actual_away = 0, 1
            else:
                actual_home = actual_away = 0.5

            if USE_MARGIN_OF_VICTORY:
                margin = abs(home_score - away_score)
                denom = max(7.5 + 0.006 * abs(home_elo - away_elo), 1.0)
                multiplier = ((margin + 1) ** 0.8) / denom
                multiplier = min(multiplier, 3.0)
            else:
                multiplier = 1.0

            home_elo_new = home_elo + k * multiplier * (actual_home - expected_home)
            away_elo_new = away_elo + k * multiplier * (actual_away - expected_away)

            team_elos[home] = home_elo_new
            team_elos[away] = away_elo_new

            results.append({
                'date': date,
                'home_team': home,
                'away_team': away,
                'home_score': home_score,
                'away_score': away_score,
                'home_elo_before': home_elo,
                'away_elo_before': away_elo,
                'home_elo_after': home_elo_new,
                'away_elo_after': away_elo_new
            })

        logging.info(f"Calculated Elo ratings for {len(results)} games.")
        elo_df = pd.DataFrame(results)
        return elo_df
    except Exception as e:
        logging.error(f"Error calculating Elo ratings: {e}")
        raise

def main():
    try:
        df = extract_game_logs()
        df = preprocess(df)
        elo_df = calculate_elo(df)

        df_pre_2024 = elo_df[elo_df['date'].dt.year < 2024]
        all_output = df_pre_2024.to_csv(index=False)
        s3.put_object(Bucket=OUTPUT_BUCKET, Key='elo-ratings-1871-to-2023.csv', Body=all_output)

        df_2024 = elo_df[elo_df['date'].dt.year == 2024]
        ratings_2024 = df_2024.to_csv(index=False)
        s3.put_object(Bucket=OUTPUT_BUCKET, Key='elo-ratings-2024.csv', Body=ratings_2024)

        # Save final Elo at end of 2024
        final_elo_home = df_2024[['date', 'home_team', 'home_elo_after']].rename(columns={'home_team': 'team', 'home_elo_after': 'elo'})
        final_elo_away = df_2024[['date', 'away_team', 'away_elo_after']].rename(columns={'away_team': 'team', 'away_elo_after': 'elo'})
        final_elo_all = pd.concat([final_elo_home, final_elo_away])

        final_elo_2024 = final_elo_all.sort_values(by='date').groupby('team').tail(1).sort_values(by='team')
        end_2024_output = final_elo_2024.to_csv(index=False)
        s3.put_object(Bucket=OUTPUT_BUCKET, Key='elo_rating_end_of_2024.csv', Body=end_2024_output)

        logging.info("Elo ratings saved to S3 successfully.")
    except Exception as e:
        logging.critical(f"Pipeline failed: {e}")

if __name__ == '__main__':
    main()
