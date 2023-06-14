import base64
import requests
import os
import json
import datetime
from enum import Enum
from getpass import getpass
import pandas as pd
from google.cloud import bigquery


# API ENDPOINT
API_ENDPOINT = 'https://api.ps3838.com'


# Available Request Methods
class HttpMethod(Enum):
    GET = 'GET'
    POST = 'POST'


# Constants to fill by each user
PS3838_USERNAME = str(os.environ.get('PS3838_USERNAME'))
PS3838_PASSWORD = str(os.environ.get('PS3838_PASSWORD'))


LEAGUES = {'premierleague': '1980', 'ligue1': '2036', 'laliga': '2196', 'bundesliga': '1842', 
           'seriea': '2436', 'eredvisie': '1928', 'liga1': '2386'}

def get_headers(request_method: HttpMethod) -> dict:
    headers = {}
    headers.update({'Accept': 'application/json'})
    if request_method is HttpMethod.POST:
        headers.update({'Content-Type': 'application/json'})

    headers.update({'Authorization': 'Basic {}'.format(
        base64.b64encode((bytes("{}:{}".format(PS3838_USERNAME, PS3838_PASSWORD), 'utf-8'))).decode())
    })

    return headers

def get_operation_endpoint(operation: str) -> str:
    return '{}{}'.format(API_ENDPOINT, operation)

def test_api():
    response = requests.get(get_operation_endpoint('/v3/odds'),
                            headers=get_headers(HttpMethod.GET),
                            params={'sportId': '29', 'leagueIds': '2196', 'oddsFormat': 'Decimal'}
                            )
    print(response.content)
    print(response.text)

def get_football_fixtures(league_id):
    operation = '/v3/fixtures'
    req = requests.get(
        get_operation_endpoint(operation),
        headers=get_headers(HttpMethod.GET),
        params={'sportId': '29', 'leagueIds': str(league_id), 'since': '200', 'isLive': '0'}
    )
    return req.json()

def get_football_odds(league_id):
    operation = '/v3/odds'
    req = requests.get(
        get_operation_endpoint(operation),
        headers=get_headers(HttpMethod.GET),
        params={'sportId': '29', 'leagueIds': str(league_id), 'oddsFormat': 'Decimal', 'isLive': '0'}
    )
    print(req.status_code)
    return req.json()

def extract_league_data(league_name):
    l_id = LEAGUES[league_name]
    try:
        odds = get_football_odds(l_id)
        events = get_football_fixtures(l_id)
        return pd.DataFrame(odds["leagues"][0]['events']), pd.DataFrame(events['league'][0]['events'])
    except:
        print(league_name + ' NO DATA')
        return pd.DataFrame(), pd.DataFrame()
    
def preprocess_league_data(league_odds, league_events):
    ct = datetime.datetime.now()
    event_ids = league_events.id.tolist()
    odds = pd.DataFrame([], columns=['id', 'PSH', 'PSD', 'PSA'])
    for event_id in event_ids:
        try:
            moneyline = league_odds[league_odds['id'] == event_id].iloc[0]['periods'][0]['moneyline']
            odds = odds.append({'id': event_id, 
                                'PSH': moneyline['home'], 
                                'PSD': moneyline['draw'], 
                                'PSA': moneyline['away']}, ignore_index=True)
        except:
            'exception'
    event_res = league_events.merge(odds, on='id').rename(columns={'starts': 'Date', 'home': 'HomeTeam', 'away': 'AwayTeam'})
    event_res = event_res[event_res.Date > ct.strftime("%Y-%m-%d %H:%M:%S")]
    event_res['Date'] = event_res['Date'].str[:10]
    event_melt = pd.melt(event_res, id_vars=['Date', 'PSH', 'PSD', 'PSA'], value_vars=['HomeTeam', 'AwayTeam'], var_name='Kind', value_name='Team').sort_values(by='Date')
    event_melt = event_melt.drop_duplicates(subset='Team')
    event_res = event_melt.set_index(['Date', 'PSH', 'PSD', 'PSA', 'Kind']).Team.unstack('Kind').reset_index().dropna()
    event_res = event_res.sort_values(by='Date').drop_duplicates(subset=['Date', 'HomeTeam', 'AwayTeam'])
    event_res['SnapshotDatetime'] = ct
    return event_res[['SnapshotDatetime', 'Date', 'HomeTeam', 'AwayTeam', 'PSH', 'PSD', 'PSA']]        

def get_league_data(league_name):
    odds, event = extract_league_data(league_name)
    if odds.size == event.size == 0:
        return pd.DataFrame()
    return preprocess_league_data(odds, event)

def create_league_table(client, league_name):
    table_id = f'peaceful-access-388710.footballdataset.{league_name}_odds'
    schema = [
                bigquery.SchemaField('SnapshotDatetime', 'DATETIME', mode='NULLABLE'),
                bigquery.SchemaField('Date', 'DATE', mode='NULLABLE'),
                bigquery.SchemaField('HomeTeam', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('AwayTeam', 'STRING', mode='NULLABLE'),
                bigquery.SchemaField('PSH', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('PSD', 'FLOAT64', mode='NULLABLE'),
                bigquery.SchemaField('PSA', 'FLOAT64', mode='NULLABLE')
                ]
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    return table

def load_league_table(client, league_table, league_df):
    errors = client.insert_rows_from_dataframe(league_table, league_df)
    if errors == []:
        print('Data loaded into BigQuery table successfully.')
    else:
        print(f'Error inserting rows: {errors}')