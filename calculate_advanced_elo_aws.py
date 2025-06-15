import boto3
import pandas as pd
import io
import statsapi
from datetime import datetime
from decimal import Decimal

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
    "Oakland Athletics": "OAK",
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

S3_BUCKET = 'mlb-game-log-data-retrosheet'
ELO_S3_KEY = 'elo_ratings_by_game.csv'
DYNAMODB_TABLE = 'Elo-Ratings-Table'

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(DYNAMODB_TABLE)

# Elo parameters
base_k = 20
home_field_advantage = 35

def expected_score(r1, r2):
    return 1 / (1 + 10 ** ((r2 - r1) / 400))

def get_injury_penalty(team_abbr):
    try:
        full_team_name = [k for k, v in TEAM_ABBREV_MAP.items() if v == team_abbr]
        if not full_team_name:
            print(f"Could not resolve full team name for abbreviation: {team_abbr}")
            return 0
        team_id = statsapi.lookup_team(full_team_name[0])[0]['id']
        roster = statsapi.get('team_roster', {'teamId': team_id, 'rosterType': 'injured'})
        if isinstance(roster, str):
            import json
            roster = json.loads(roster)
        if 'roster' in roster and isinstance(roster['roster'], list):
            injured_players = roster['roster']
            print(injured_players)
        else:
            injured_players = []
        il_count = len(injured_players)
        print(f"Team: {team_abbr}, Injured List Count: {il_count}")
        return -10 if il_count > 3 else 0
    except Exception as e:
        print(f"Error retrieving injury list for team {team_abbr}: {e}")
        return 0

def update_elo(r1, r2, score_diff):
    mov_multiplier = (abs(score_diff) + 1) ** 0.8 / (7.5 + 0.006 * abs(r1 - r2))
    k = base_k * mov_multiplier
    exp = expected_score(r1, r2)
    return r1 + k * (1 - exp), r2 - k * (1 - exp)

def load_gamelogs_from_s3():
    obj = s3.get_object(Bucket='mlb-game-log-data-retrosheet', Key='gamelogs/gl1871_2024.zip')
    zip_bytes = io.BytesIO(obj['Body'].read())
    import zipfile
    with zipfile.ZipFile(zip_bytes, 'r') as z:
        all_files = [f for f in z.namelist() if f.endswith('.txt')]
        dfs = [pd.read_csv(z.open(f), header=None) for f in all_files]
    df = pd.concat(dfs, ignore_index=True)
    df = df[[0, 3, 6, 9, 10]]
    df.columns = ['date', 'away_team', 'home_team', 'away_score', 'home_score']
    return df

def load_all_games():
    historical_df = load_gamelogs_from_s3()
    historical_df['date'] = pd.to_datetime(historical_df['date'], format='%Y%m%d')
    historical_df = historical_df[['date', 'home_team', 'away_team', 'home_score', 'away_score']]

    current_year = datetime.now(tz=pd.Timestamp.utcnow().tz).year
    try:
        cached_obj = s3.get_object(Bucket=S3_BUCKET, Key='cache/schedule_2025.csv')
        df_2025 = pd.read_csv(io.BytesIO(cached_obj['Body'].read()))
        df_2025['date'] = pd.to_datetime(df_2025['date'], errors='coerce')
        print(f"Loaded cached schedule with {len(df_2025)} games.")
    except s3.exceptions.NoSuchKey:
        all_games = []
        seen_games = set()
        from datetime import timedelta
        start = datetime(current_year, 3, 1)  # MLB season typically starts in March
        end = datetime.utcnow()
        delta = timedelta(days=7)
        week_counter = 0
        total_fetched = 0
        while start < end:
            chunk_end = min(start + delta, end)
            week_counter += 1
            weekly_games = 0
            games_chunk = statsapi.schedule(start_date=start.strftime('%Y-%m-%d'), end_date=chunk_end.strftime('%Y-%m-%d'))
            for g in games_chunk:
                if g['status'] == 'Final' and g.get('game_type') == 'R':
                  home_name = g['home_name']
                  away_name = g['away_name']
                  home_abbr = TEAM_ABBREV_MAP.get(home_name)
                  away_abbr = TEAM_ABBREV_MAP.get(away_name)
                  game_key = (g['game_date'], home_abbr, away_abbr)
                  if home_abbr and away_abbr and game_key not in seen_games:
                      seen_games.add(game_key)
                      weekly_games += 1
                      all_games.append({
                          'date': pd.to_datetime(g['game_date']),
                          'home_team': home_abbr,
                          'away_team': away_abbr,
                          'home_score': g['home_score'],
                          'away_score': g['away_score'],
                          'pitchers': g.get('home_probable_pitcher', '') + ' vs ' + g.get('away_probable_pitcher', ''),
                          'injury_note': g.get('note', '')
                      })
        df_2025 = pd.DataFrame(all_games)
        print(f"Week {week_counter}: Retrieved {weekly_games} games from {start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}")
        total_fetched += weekly_games
        start += delta
        print(f"Fetched and cached {len(df_2025)} new games.")
        print(f"Total regular season games fetched for 2025: {total_fetched}")
        buf = io.BytesIO()
        df_2025['date'] = pd.to_datetime(df_2025['date'], errors='coerce')
        df_2025 = df_2025.sort_values(by='date')
        df_2025.to_csv(buf, index=False)
        buf.seek(0)
        s3.put_object(Bucket=S3_BUCKET, Key='cache/schedule_2025.csv', Body=buf.getvalue())

    df_2025 = df_2025[df_2025['home_team'].isin(TEAM_ABBREV_MAP.values()) & df_2025['away_team'].isin(TEAM_ABBREV_MAP.values())]
    df_combined = pd.concat([historical_df, df_2025], ignore_index=True)
    df_combined = df_combined.dropna(subset=['date', 'home_team', 'away_team'])

    # Optional enhancement: Check for missing days in 2025 season
    all_dates = pd.date_range(start='2025-03-01', end=datetime.utcnow().date())
    missing_dates = [d for d in all_dates if d not in df_combined[df_combined['date'].dt.year == 2025]['date'].dt.date.unique()]
    if missing_dates:
        print(f"Warning: Missing dates in 2025 schedule cache: {missing_dates}")

    return df_combined.sort_values(by='date')

# Cache dictionary for pitcher ERA lookups
pitcher_era_cache = {}

def calculate_elo():

    df = load_all_games()
    ratings = {team: 1500 for team in TEAM_ABBREV_MAP.values()}
    updated_rows = []

    for _, game in df.iterrows():
        if pd.isnull(game['date']):
            print(f"Skipping game with missing date: {game}")
            continue
        try:
            game_date = pd.to_datetime(game['date'], errors='raise')
        except Exception as e:
            print(f"Invalid date encountered: {game['date']} in game: {game}, error: {e}")
            continue
        year = game_date.year
        apply_adjustments = year == 2025
        home = game['home_team']
        away = game['away_team']
        hs = game['home_score']
        vs = game['away_score']
        if pd.isnull(game['date']):
            print(f"Skipping game with missing or invalid date: {game}")
            continue
        date = game_date.strftime('%Y-%m-%d')

        # Adjust Elo based on pitcher info and injury note
        home_pitcher = away_pitcher = ''
        if apply_adjustments:
            pitchers = game.get('pitchers', '')
            home_pitcher = pitchers.split(' vs ')[0].strip() if ' vs ' in pitchers else ''
            away_pitcher = pitchers.split(' vs ')[1].strip() if ' vs ' in pitchers else ''

        # Fetch pitcher ERA-based Elo adjustments
        def get_pitcher_era_adjustment(name):
            if name in pitcher_era_cache:
                return pitcher_era_cache[name]
            try:
                search = statsapi.lookup_player(name.title())
                if search:
                    pid = search[0]['id']
                    stats = statsapi.player_stat_data(pid, group='pitching', type='season')
                    era = float(stats['stats'][0]['stats'].get('era', 4.5))
                    print(f"Pitcher: {name.title()}, ERA: {era}")
                    if era <= 3.0:
                        adj = 15
                    elif era <= 4.0:
                        adj = 5
                    elif era >= 5.0:
                        adj = -10
                    else:
                        adj = 0
                    pitcher_era_cache[name] = adj
                    return adj
                pitcher_era_cache[name] = 0
                return 0
            except Exception as e:
                print(f"Error retrieving ERA for pitcher {name}: {e}")
                pitcher_era_cache[name] = 0
                return 0
            except Exception as e:
                print(f"Error retrieving ERA for pitcher {name}: {e}")
                return 0
            except:
                return 0

        home_pitcher_adj = get_pitcher_era_adjustment(home_pitcher) if apply_adjustments else 0
        away_pitcher_adj = get_pitcher_era_adjustment(away_pitcher) if apply_adjustments else 0

        # Injury adjustment using team IL count
        injury_note = str(game.get('injury_note', '')).lower()
        home_injury_adj = get_injury_penalty(home) if apply_adjustments else 0
        away_injury_adj = get_injury_penalty(away) - 10 if apply_adjustments and away.lower() in injury_note else (get_injury_penalty(away) if apply_adjustments else 0)

        home_elo = ratings.get(home, 1500) + home_pitcher_adj + home_injury_adj
        away_elo = ratings.get(away, 1500) + away_pitcher_adj + away_injury_adj

        home_adj = home_elo + home_field_advantage
        away_adj = away_elo

        if hs > vs:
            home_new, away_new = update_elo(home_adj, away_adj, hs - vs)
            home_elo = home_new - home_field_advantage
            away_elo = away_new
        elif vs > hs:
            away_new, home_new = update_elo(away_adj, home_adj, vs - hs)
            home_elo = home_new - home_field_advantage
            away_elo = away_new

        ratings[home] = home_elo
        ratings[away] = away_elo

        updated_rows.append({
            'date': date,
            'home_team': home,
            'away_team': away,
            'home_score': hs,
            'away_score': vs,
            'home_elo_post_raw': round(ratings[home] - home_pitcher_adj - home_injury_adj, 2),
            'away_elo_post_raw': round(ratings[away] - away_pitcher_adj - away_injury_adj, 2),
            'home_elo_post': round(home_elo, 2),
            'away_elo_post': round(away_elo, 2)
        })

        if year == 2025:
            raw_home = ratings.get(home, 1500)
            raw_away = ratings.get(away, 1500)
            
            table.put_item(Item={
                'team_date': f"{home}#{date}",
                'team': home,
                'date': date,
                'elo': Decimal(str(round(home_elo, 2))),
                'elo_raw': Decimal(str(round(raw_home, 2)))
            })
            table.put_item(Item={
                'team_date': f"{away}#{date}",
                'team': away,
                'date': date,
                'elo': Decimal(str(round(away_elo, 2))),
                'elo_raw': Decimal(str(round(raw_away, 2)))
            })

    result_df = pd.DataFrame(updated_rows)
    result_df = result_df[result_df['home_team'].isin(TEAM_ABBREV_MAP.values()) & result_df['away_team'].isin(TEAM_ABBREV_MAP.values())]

    # ensure date column is datetime
    result_df['date'] = pd.to_datetime(result_df['date'], errors='raise')

    # merge updated elo back into full dataset
    try:
        old_obj = s3.get_object(Bucket=S3_BUCKET, Key=ELO_S3_KEY)
        df_all = pd.read_csv(io.BytesIO(old_obj['Body'].read()))
        df_all['date'] = pd.to_datetime(df_all['date'], errors='coerce')
        df_all = df_all.dropna(subset=['date'])
        df_all = df_all[df_all['date'].dt.year != 2025]
    except s3.exceptions.NoSuchKey:
        df_all = pd.DataFrame()

    result_df = pd.DataFrame(updated_rows)
    result_df['date'] = pd.to_datetime(result_df['date'], errors='coerce')
    result_df = result_df.sort_values(by='date')
    buf = io.BytesIO()
    result_df.to_csv(buf, index=False)
    buf.seek(0)

    timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    backup_key = f"backup/elo_ratings_by_game_{timestamp}.csv"
    s3.copy_object(
        Bucket=S3_BUCKET,
        CopySource={"Bucket": S3_BUCKET, "Key": ELO_S3_KEY},
        Key=backup_key
    )

    # overwrite with updated Elo
    final_df = pd.concat([df_all, result_df], ignore_index=True).sort_values(by='date')
    final_buf = io.BytesIO()
    final_df.to_csv(final_buf, index=False)
    final_buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=ELO_S3_KEY, Body=final_buf.getvalue())

    # Save Elo changes and log to S3 in CSV format
    summary_rows = []
    for team, final_elo in ratings.items():
        initial_elo = 1500  # Default unless overwritten
        for row in updated_rows:
            if row['home_team'] == team:
                initial_elo = row['home_elo_post']
            elif row['away_team'] == team:
                initial_elo = row['away_elo_post']
        summary_rows.append({
            'team': team,
            'initial_elo': round(initial_elo, 2),
            'final_elo': round(final_elo, 2),
            'change': round(final_elo - initial_elo, 2),
            'date': datetime.utcnow().strftime('%Y-%m-%d')
        })

    summary_df = pd.DataFrame(summary_rows)

    # Log raw vs adjusted Elo difference per team
    raw_adjusted_log = []
    for row in updated_rows:
        for side in ['home', 'away']:
            raw = row[f'{side}_elo_post_raw']
            adjusted = row[f'{side}_elo_post']
            raw_adjusted_log.append({
                'date': row['date'],
                'team': row[f'{side}_team'],
                'elo_raw': raw,
                'elo_adjusted': adjusted,
                'difference': round(adjusted - raw, 2)
            })
    elo_diff_df = pd.DataFrame(raw_adjusted_log)
    if not elo_diff_df.empty:
        diff_buf = io.StringIO()
        elo_diff_df.to_csv(diff_buf, index=False)
        diff_buf.seek(0)
        diff_key = f"logs/elo_adjustment_diff_{datetime.utcnow().strftime('%Y%m%d')}.csv"
        s3.put_object(Bucket=S3_BUCKET, Key=diff_key, Body=diff_buf.getvalue())
    summary_df = summary_df[summary_df['team'].isin(TEAM_ABBREV_MAP.values())]
    csv_buf = io.StringIO()
    summary_df.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    log_key = f"logs/elo_summary_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=log_key, Body=csv_buf.getvalue())

    # Append to centralized summary log
    try:
        central_obj = s3.get_object(Bucket=S3_BUCKET, Key='logs/elo_daily_summary.csv')
        central_df = pd.read_csv(io.BytesIO(central_obj['Body'].read()))
        combined_df = pd.concat([central_df, summary_df], ignore_index=True)
    except s3.exceptions.NoSuchKey:
        combined_df = summary_df

    full_buf = io.StringIO()
    combined_df.to_csv(full_buf, index=False)
    full_buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key='logs/elo_daily_summary.csv', Body=full_buf.getvalue())

    return {
        "statusCode": 200,
        "body": f"{len(updated_rows)} games reprocessed and Elo ratings updated. Summary saved to {log_key}."
    }

calculate_elo()