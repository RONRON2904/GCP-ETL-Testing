from ps3838_utils import *
from google.cloud import bigquery

def handler(request):
    client = bigquery.Client() 
    for league_name in LEAGUES.keys():
        table = create_league_table(client, league_name)
        league_data = get_league_data(league_name)
        if league_data.size > 0:
            load_league_table(client, table, league_data)