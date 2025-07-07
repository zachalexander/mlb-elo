# =============================================
# AWS Lambda Elo Rating & Backfill Function
# =============================================
#
# This AWS Lambda script supports two operations:
# 1. **Daily Update:** Fetches the most recent completed games from the MLB Stats API
#    (attempts today, then yesterday if none), calculates Elo updates,
#    and writes initial and post-game ratings to DynamoDB.
# 2. **Backfill 2025 Season:** Loads end-of-2024 Elo ratings from S3, iterates through all
#    completed 2025 games thus far, computes initial and post-game Elo for each,
#    and writes both values to DynamoDB, ensuring no duplicate entries.
#
import os
import sys
import subprocess
# pip install requests to /tmp for AWS Lambda
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

import csv
import json
import logging
import decimal
import boto3
import requests
from io import StringIO
from datetime import datetime, timezone, timedelta

# Team abbreviation mapping from full names
TEAM_ABBREV_MAP = {
    "Arizona Diamondbacks": "ARI",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago White Sox": "CHW",
    "Chicago Cubs": "CHC",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KCR",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Yankees": "NYY",
    "New York Mets": "NYM",
    "Athletics": "OAK",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SDP",
    "San Francisco Giants": "SFG",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TBR",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSN"
}

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Config from environment
TABLE_NAME    = os.getenv('ELO_TABLE_NAME')
OUTPUT_BUCKET = os.getenv('OUTPUT_BUCKET', 'mlb-elo-ratings-output')
END_2024_KEY  = os.getenv('END_2024_KEY', 'elo_rating_end_of_2024.csv')
SEASON_YEAR   = int(os.getenv('SEASON_YEAR', '2025'))

# DynamoDB table
table = dynamodb.Table(TABLE_NAME)

# Elo parameters
BASE_ELO                 = 1500
K_FACTOR                 = 20
HOME_FIELD_ADVANTAGE     = 35
USE_HOME_FIELD_ADVANTAGE = True
USE_MARGIN_OF_VICTORY    = True
USE_PLAYOFF_WEIGHTING    = True

# MLB Stats API URL
MLB_SCHEDULE_URL = 'https://statsapi.mlb.com/api/v1/schedule'


def _extract_team_abbrev(team_obj):
    """
    Use full team name mapping first, then fall back to API abbreviations.
    """
    full_name = team_obj.get('name')
    if full_name in TEAM_ABBREV_MAP:
        return TEAM_ABBREV_MAP[full_name]
    # Fallbacks
    return (team_obj.get('abbreviation')
            or TEAM_ABBREV_MAP.get(team_obj.get('teamName'),
                                   team_obj.get('teamName'))
            or str(team_obj.get('id')))


def _extract_score(team_side: dict):
    try:
        return int(team_side.get('score', 0))
    except:
        return 0


def fetch_schedule_for_date(game_date: datetime):
    params = {'sportId': 1, 'date': game_date.strftime('%Y-%m-%d'), 'hydrate': 'decisions,linescore'}
    resp = requests.get(MLB_SCHEDULE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    games = []
    for date_block in data.get('dates', []):
        for g in date_block.get('games', []):
            if g.get('status', {}).get('abstractGameState') == 'Final':
                home = _extract_team_abbrev(g['teams']['home']['team'])
                away = _extract_team_abbrev(g['teams']['away']['team'])
                home_score = _extract_score(g['teams']['home'])
                away_score = _extract_score(g['teams']['away'])
                game_id = f"{home}{away}-{g.get('gamePk')}"
                games.append({
                    'gamePk': game_id,
                    'homeTeam': home,
                    'awayTeam': away,
                    'homeScore': home_score,
                    'awayScore': away_score,
                    'date': date_block.get('date')
                })
    logger.info(f"Fetched {len(games)} games for {game_date.strftime('%Y-%m-%d')}.")
    return games


def load_previous_elos():
    resp = table.scan()
    return {item['team']: float(item['post_elo']) for item in resp.get('Items', [])}


def load_existing_keys():
    resp = table.scan(ProjectionExpression='team,gamePk')
    return set((item['team'], item['gamePk']) for item in resp.get('Items', []))


def calculate_single_game_elo(home, away, home_score, away_score, date_str, prev_elos):
    home_elo = prev_elos.get(home, BASE_ELO)
    away_elo = prev_elos.get(away, BASE_ELO)
    hfa = HOME_FIELD_ADVANTAGE if USE_HOME_FIELD_ADVANTAGE else 0
    exp_home = 1 / (1 + 10 ** ((away_elo - (home_elo + hfa)) / 400))
    act_home = 1 if home_score > away_score else 0 if home_score < away_score else 0.5
    act_away = 1 - act_home
    if USE_MARGIN_OF_VICTORY:
        m = abs(home_score - away_score)
        denom = max(7.5 + 0.006 * abs(home_elo - away_elo), 1)
        mult = min(((m + 1) ** 0.8) / denom, 3)
    else:
        mult = 1
    k = K_FACTOR
    gd = datetime.strptime(date_str, '%Y-%m-%d')
    if USE_PLAYOFF_WEIGHTING and gd.month >= 10:
        k *= 2
    elif USE_PLAYOFF_WEIGHTING and gd.month >= 9:
        k *= 1.5
    new_home = home_elo + k * mult * (act_home - exp_home)
    new_away = away_elo + k * mult * (act_away - (1 - exp_home))
    return ((home_elo, new_home), (away_elo, new_away))


def write_elo_records(records: list):
    existing = load_existing_keys()
    seen = set()
    to_write = []
    for r in records:
        key = (r['team'], r['gamePk'])
        if key in seen or key in existing:
            continue
        seen.add(key)
        r['initial_elo'] = decimal.Decimal(str(r['initial_elo']))
        r['post_elo'] = decimal.Decimal(str(r['post_elo']))
        to_write.append(r)
    with table.batch_writer() as batch:
        for r in to_write:
            batch.put_item(Item=r)
    logger.info(f"Wrote {len(to_write)} new Elo records.")


def backfill_season():
    obj = s3_client.get_object(Bucket=OUTPUT_BUCKET, Key=END_2024_KEY)
    csv_content = obj['Body'].read().decode('utf-8')
    reader = csv.DictReader(StringIO(csv_content))
    elos = {row['team']: float(row['elo']) for row in reader}
    logger.info("Loaded starting Elo from 2024 end-of-season CSV.")
    params = {'sportId': 1, 'season': SEASON_YEAR, 'hydrate': 'decisions,linescore'}
    resp = requests.get(MLB_SCHEDULE_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    seen_games = set()
    recs = []
    for d in data.get('dates', []):
        ds = d.get('date')
        if ds > datetime.now(timezone.utc).strftime('%Y-%m-%d'):
            continue
        for g in d.get('games', []):
            if g.get('status', {}).get('abstractGameState') != 'Final' or g.get('gameType') != 'R':
                continue
            home = _extract_team_abbrev(g['teams']['home']['team'])
            away = _extract_team_abbrev(g['teams']['away']['team'])
            gid_home = f"{home}-{g.get('gamePk')}"
            gid_away = f"{away}-{g.get('gamePk')}"
            if gid_home and gid_away in seen_games:
                continue
            seen_games.add(gid_home)
            seen_games.add(gid_away)
            hs = _extract_score(g['teams']['home'])
            as_ = _extract_score(g['teams']['away'])
            (hb, ha), (ab, aa) = calculate_single_game_elo(home, away, hs, as_, ds, elos)
            elos[home], elos[away] = ha, aa
            recs.append({'team': home, 'gamePk': gid_home, 'date': ds, 'initial_elo': hb, 'post_elo': ha})
            recs.append({'team': away, 'gamePk': gid_away, 'date': ds, 'initial_elo': ab, 'post_elo': aa})
    print(recs)
    write_elo_records(recs)
    logger.info("Backfill complete.")


def lambda_handler(event, context):
    try:
        if event.get('backfill'):
            backfill_season()
            return {'statusCode': 200, 'body': json.dumps('Backfill complete')}
        now = datetime.now(timezone.utc)
        for offset in (0, 1):
            pd = now - timedelta(days=offset)
            games = fetch_schedule_for_date(pd)
            if games:
                logger.info(f"Processing games for {pd.strftime('%Y-%m-%d')}")
                break
        else:
            logger.info("No games to process.")
            return {'statusCode': 200, 'body': json.dumps('No games to process')}
        prev_elos = load_previous_elos()
        updated = []
        for g in games:
            (hb, ha), (ab, aa) = calculate_single_game_elo(g['homeTeam'], g['awayTeam'], g['homeScore'], g['awayScore'], g['date'], prev_elos)
            prev_elos[g['homeTeam']], prev_elos[g['awayTeam']] = ha, aa
            updated.append({'team': g['homeTeam'], 'gamePk': g['gamePk'], 'date': g['date'], 'initial_elo': hb, 'post_elo': ha})
            updated.append({'team': g['awayTeam'], 'gamePk': g['gamePk'], 'date': g['date'], 'initial_elo': ab, 'post_elo': aa})
        write_elo_records(updated)
        return {'statusCode': 200, 'body': json.dumps('Daily update complete')}
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        return {'statusCode': 500, 'body': json.dumps(str(e))}
