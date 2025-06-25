import boto3
import pandas as pd
import os
import configparser
import logging
from io import StringIO, BytesIO
import zipfile
from botocore.exceptions import ClientError
import statsapi

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants for Elo calculations
INITIAL_ELO = 1500
K_FACTOR = 20
HOME_FIELD_ADVANTAGE = 100

# Mapping of Retrosheet codes (including variants) to full MLB team names
TEAM_FULL_NAMES = {
    'ARI': 'Arizona Diamondbacks', 'ATL': 'Atlanta Braves', 'BAL': 'Baltimore Orioles',
    'BOS': 'Boston Red Sox', 'CHC': 'Chicago Cubs', 'CIN': 'Cincinnati Reds',
    'CLE': 'Cleveland Guardians', 'COL': 'Colorado Rockies', 'CWS': 'Chicago White Sox',
    'DET': 'Detroit Tigers', 'HOU': 'Houston Astros', 'KCR': 'Kansas City Royals', 'KC': 'Kansas City Royals',
    'LAA': 'Los Angeles Angels', 'ANA': 'Los Angeles Angels', 'CAL': 'Los Angeles Angels',
    'LAD': 'Los Angeles Dodgers', 'LAN': 'Los Angeles Dodgers',
    'MIA': 'Miami Marlins', 'FLA': 'Miami Marlins', 'MIL': 'Milwaukee Brewers',
    'MIN': 'Minnesota Twins', 'NYM': 'New York Mets', 'NYY': 'New York Yankees', 'NYA': 'New York Yankees',
    'OAK': 'Oakland Athletics', 'PHI': 'Philadelphia Phillies', 'PIT': 'Pittsburgh Pirates',
    'SDP': 'San Diego Padres', 'SDN': 'San Diego Padres', 'SEA': 'Seattle Mariners',
    'SFG': 'San Francisco Giants', 'SFN': 'San Francisco Giants', 'STL': 'St. Louis Cardinals',
    'TBR': 'Tampa Bay Rays', 'TBA': 'Tampa Bay Rays', 'TEX': 'Texas Rangers',
    'TOR': 'Toronto Blue Jays', 'WSN': 'Washington Nationals', 'WSH': 'Washington Nationals'
}
VALID_CODES = set(TEAM_FULL_NAMES.keys())

# Reverse map for standardized codes (first occurrence)
STANDARD_CODE = {
    code: next(k for k,v in TEAM_FULL_NAMES.items() if v==TEAM_FULL_NAMES[code])
    for code in VALID_CODES
}


def get_s3_client():
    creds = configparser.ConfigParser()
    path = os.path.expanduser('~/.aws/credentials')
    creds.read(path)
    profile = creds['default']
    session = boto3.Session(
        aws_access_key_id=profile.get('aws_access_key_id'),
        aws_secret_access_key=profile.get('aws_secret_access_key'),
        region_name=profile.get('region')
    )
    return session.client('s3', verify=False)


def refresh_2025_cache(bucket: str, prefix: str) -> str:
    """
    Fetches current 2025 game data from mlbstatsapi and writes a CSV cache to S3.
    Returns the S3 key of the cached file.
    """
    s3 = get_s3_client()
    # Fetch schedule for 2025 Regular season
    schedule = statsapi.schedule(sportId=1, season='2025-regular')
    rows = []
    for date_entry in schedule.get('dates', []):
        game_date = date_entry['date']
        for g in date_entry.get('games', []):
            rows.append({
                'date': game_date,
                'home_team_raw': g['teams']['home']['team']['abbreviation'],
                'away_team_raw': g['teams']['away']['team']['abbreviation'],
                'home_score': g.get('teams', {}).get('home', {}).get('score'),
                'away_score': g.get('teams', {}).get('away', {}).get('score')
            })
    df = pd.DataFrame(rows)
    # Write to S3
    key = f"{prefix}games_2025.csv"
    buf = StringIO()
    df.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    logger.info(f"Wrote 2025 cache to s3://{bucket}/{key}")
    return key


def read_historical_logs(bucket, key):
    s3 = get_s3_client()
    resp = s3.get_object(Bucket=bucket, Key=key)
    data = resp['Body'].read()
    frames = []
    with zipfile.ZipFile(BytesIO(data)) as z:
        for f in z.namelist():
            if f.lower().endswith('.txt'):
                df = pd.read_csv(z.open(f), header=None,
                                 usecols=[0,3,6,7,10],
                                 names=['date','away_team_raw','home_team_raw','away_score','home_score'],
                                 dtype=str)
                frames.append(df)
    return pd.concat(frames, ignore_index=True)


def read_2025_cache(bucket, prefix):
    s3 = get_s3_client()
    key = refresh_2025_cache(bucket, prefix)
    resp = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(resp['Body'])
    return df


def clean_and_combine(historical, cache2025):
    # Rename and standardize codes
    df = pd.concat([historical, cache2025], ignore_index=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date','away_team_raw','home_team_raw'])
    # Map raw codes to standardized
    df['away_team'] = df['away_team_raw'].map(STANDARD_CODE)
    df['home_team'] = df['home_team_raw'].map(STANDARD_CODE)
    # Filter and convert scores
    df = df[df['away_team'].isin(VALID_CODES) & df['home_team'].isin(VALID_CODES)]
    df['away_score'] = pd.to_numeric(df['away_score'], errors='coerce')
    df['home_score'] = pd.to_numeric(df['home_score'], errors='coerce')
    # Add full names
    df['away_team_full'] = df['away_team'].map(TEAM_FULL_NAMES)
    df['home_team_full'] = df['home_team'].map(TEAM_FULL_NAMES)
    return df[['date','home_team','home_team_full','away_team','away_team_full','home_score','away_score']]


def expected_score(rating, opponent_rating):
    return 1.0 / (1 + 10 ** ((opponent_rating - rating) / 400))


def update_ratings(home_rating, away_rating, home_score, away_score):
    home_adj = home_rating + HOME_FIELD_ADVANTAGE
    away_adj = away_rating
    exp_home = expected_score(home_adj, away_adj)
    exp_away = expected_score(away_adj, home_adj)
    if home_score > away_score:
        s_home, s_away = 1.0, 0.0
    elif home_score < away_score:
        s_home, s_away = 0.0, 1.0
    else:
        s_home = s_away = 0.5
    new_h = home_rating + K_FACTOR*(s_home-exp_home)
    new_a = away_rating + K_FACTOR*(s_away-exp_away)
    return new_h, new_a


def calculate_elo(df):
    elo = {code: INITIAL_ELO for code in VALID_CODES}
    records = []
    for _,row in df.sort_values('date').iterrows():
        h,a = row['home_team'], row['away_team']
        nh, na = update_ratings(elo[h], elo[a], int(row['home_score']), int(row['away_score']))
        record = row.to_dict()
        record.update({
            'home_elo_before': elo[h], 'away_elo_before': elo[a],
            'home_elo_after': nh, 'away_elo_after': na
        })
        records.append(record)
        elo[h], elo[a] = nh, na
    return pd.DataFrame(records)


def write_results(df, bucket, key):
    s3 = get_s3_client()
    try:
        exist = pd.read_csv(s3.get_object(Bucket=bucket, Key=key)['Body'])
    except Exception:
        exist = pd.DataFrame()
    all_df = pd.concat([exist, df], ignore_index=True)
    all_df.drop_duplicates(subset=['date','home_team','away_team'], inplace=True)
    all_df.sort_values('date', inplace=True)
    buf = StringIO()
    all_df.to_csv(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def main():
    bucket = 'mlb-game-log-data-retrosheet'
    hist_key = 'gamelogs/gl1871_2024.zip'
    cache_prefix = '2025_cache/'
    out_key = 'elo/ratings_history.csv'

    hist = read_historical_logs(bucket, hist_key)
    cache25 = read_2025_cache(bucket, cache_prefix)
    combined = clean_and_combine(hist, cache25)
    elo_df = calculate_elo(combined)
    write_results(elo_df, bucket, out_key)

if __name__ == '__main__':
    main()
