# =============================================
# AWS Lambda: Fetch & Save Full 2025 Regular-Season Schedule with Score Backfill
# =============================================
#
# This AWS Lambda function:
#  - Fetches the entire 2025 MLB regular-season schedule from the Stats API
#  - Filters to gameType 'R' only
#  - For games marked "Final" with missing scores, backfills using the live game feed
#  - Ensures each gamePk is output only once
#  - Flattens each game into a CSV row and writes the full schedule CSV to S3

import sys
import subprocess
# pip install requests to /tmp for AWS Lambda
subprocess.call('pip install requests -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

import os
import csv
import io
import json
import logging
import boto3
import requests
from datetime import datetime

# Logging setup
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
SEASON_YEAR = os.getenv('SEASON_YEAR', '2025')
S3_BUCKET   = os.getenv('SCHEDULE_BUCKET')           # e.g. 'my-schedule-bucket'
S3_KEY      = os.getenv('SCHEDULE_KEY', f'schedule_{SEASON_YEAR}_full.csv')

# MLB Stats API endpoints
SCHEDULE_URL   = 'https://statsapi.mlb.com/api/v1/schedule'
GAME_FEED_URL  = 'https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live'

# Reuse HTTP session
session = requests.Session()

def fetch_full_schedule():
    params = {
        'sportId': 1,
        'season': SEASON_YEAR,
        'gameTypes': 'R',
        'hydrate': 'decisions,linescore,team',
        'limit': 5000
    }
    resp = session.get(SCHEDULE_URL, params=params)
    resp.raise_for_status()
    return resp.json().get('dates', [])

def backfill_score(game_pk):
    try:
        url = GAME_FEED_URL.format(gamePk=game_pk)
        r = session.get(url)
        r.raise_for_status()
        teams = r.json().get('liveData', {}).get('linescore', {}).get('teams', {})
        return teams.get('home', {}).get('runs'), teams.get('away', {}).get('runs')
    except Exception as e:
        logger.warning(f"Backfill failed for {game_pk}: {e}")
        return None, None

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        schedule_blocks = fetch_full_schedule()
        dates = len(schedule_blocks)
        games_cnt = sum(len(block.get('games', [])) for block in schedule_blocks)
        logger.info(f"Fetched {dates} days and {games_cnt} games for {SEASON_YEAR}")

        buffer = io.StringIO()
        writer = csv.writer(buffer)
        writer.writerow([
            'date','gamePk','status','homeTeam','awayTeam',
            'homeScore','awayScore','venueName',
            'homeStartingPitcherId','homeStartingPitcherName',
            'awayStartingPitcherId','awayStartingPitcherName'
        ])

        seen = set()
        for block in schedule_blocks:
            date_str = block.get('date')
            for g in block.get('games', []):
                if g.get('gameType') != 'R':
                    continue
                pk = str(g.get('gamePk'))
                if pk in seen:
                    continue
                seen.add(pk)

                status = g.get('status', {}).get('abstractGameState')
                home = g['teams']['home']['team'].get('abbreviation')
                away = g['teams']['away']['team'].get('abbreviation')
                hs = g['teams']['home'].get('score')
                as_ = g['teams']['away'].get('score')

                if status == 'Final' and (hs is None or as_ is None):
                    bhs, bas = backfill_score(pk)
                    hs = bhs if bhs is not None else hs
                    as_ = bas if bas is not None else as_
                if status == 'Final' and (hs is None or as_ is None):
                    logger.warning(f"Skipping final game {pk} on {date_str} due to missing score")
                    continue

                try:
                    url = GAME_FEED_URL.format(gamePk=pk)
                    r = session.get(url)
                    r.raise_for_status()
                    data = r.json()
                    venue_info = data.get('gameData', {}).get('venue', {})
                    venue_name = venue_info.get('name')

                    probables = data.get('gameData', {}).get('probablePitchers', {})
                    home_pitcher_id = probables.get('home', {}).get('id')
                    home_pitcher_name = probables.get('home', {}).get('fullName')
                    away_pitcher_id = probables.get('away', {}).get('id')
                    away_pitcher_name = probables.get('away', {}).get('fullName')
                except Exception as e:
                    logger.warning(f"Failed to fetch venue/pitchers for game {pk}: {e}")
                    venue_name = ''
                    home_pitcher_id = home_pitcher_name = away_pitcher_id = away_pitcher_name = ''

                writer.writerow([
                    date_str,
                    pk,
                    status,
                    home,
                    away,
                    hs if hs is not None else '',
                    as_ if as_ is not None else '',
                    venue_name,
                    home_pitcher_id,
                    home_pitcher_name,
                    away_pitcher_id,
                    away_pitcher_name
                ])

        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY, Body=buffer.getvalue())
        logger.info(f"Saved deduped schedule CSV to s3://{S3_BUCKET}/{S3_KEY}")

        return {'statusCode': 200, 'body': json.dumps({'days': dates, 'games': games_cnt})}

    except Exception as e:
        logger.error(f"Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
