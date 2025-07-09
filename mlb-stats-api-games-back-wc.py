import json
import sys
import subprocess
# pip install custom package to /tmp/ and add to path
subprocess.call('pip install MLB-StatsAPI -t /tmp/ --no-cache-dir'.split(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
sys.path.insert(1, '/tmp/')

import statsapi
import boto3
from datetime import date, datetime, timedelta
import requests

dynamodb = boto3.resource('dynamodb')

def build_games_back_series(start: date, end: date):
    """
    Iterate from start to end (inclusive), fetch games back each day,
    and either print or save as CSV.
    """
    table_id = 1
    current = start
    rows = []
    while current <= end:
        url = "https://statsapi.mlb.com/api/v1/standings"
        params = {
            "leagueId": 103,           # 103 = AL
            "season": current.year,
            "date": current.isoformat(),
            "sportId": 1               # MLB
        }
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        # print(data)

        # The JSON has records grouped by type; one of them is 'wildCard' standings.
        for record in data.get("records", []):
            for teamRec in record["teamRecords"]:
                # print(teamRec)
                if teamRec["team"]["id"] == 110:   # 110 = Baltimore Orioles
                    print(str("mlb-day-"+ current.isoformat()), f"{current.isoformat()}, {teamRec["wildCardGamesBack"]}")
                    if(teamRec["wildCardGamesBack"] == "-"):
                        teamRec["wildCardGamesBack"] = 0

                    if(teamRec["gamesBack"] == "-"):
                        teamRec["gamesBack"] = 0

                    data = {
                        'id': str("mlb-day-"+ current.isoformat()),
                        'date': f"{current.isoformat()}",
                        'wildcard_gb': teamRec["wildCardGamesBack"],
                        'division_gb': teamRec["gamesBack"]
                    }
                    table = dynamodb.Table('Orioles-Games_Back')   
                    table.put_item(Item=data)

                    current += timedelta(days=1)
                    table_id += 1

def lambda_handler(event, context):

    # To just print to console:
    # start_date = date(2025, 3, 27)
    # end_date = date(2025, 5, 29)

    current_date = date.today()
    build_games_back_series(current_date, current_date)
    

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
