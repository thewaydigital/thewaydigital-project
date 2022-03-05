#libraries
import json
import pandas as pd
import time
import sys
import os
sys.path.insert(0, os.getcwd())
import yaml
import requests
# Bigquery library
from google.cloud import bigquery
pd.options.display.float_format = '{:,.2f}'.format

# load BQ credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
client_bigquery = bigquery.Client()

#creating a Dataframe
df = pd.DataFrame()


#Load Smartsheets credentials
config_file = open(os.getcwd()+'/config/credentials_smartsheets.yaml','r')
config = yaml.safe_load(config_file)

api_key = config.get('smartsheets').get('query').get('api_key')


headers = {'Content-Type': 'application/json;','Authorization':'Bearer '+api_key}
DatafromApi = requests.get("https://api.smartsheet.com/2.0/sheets/", headers=headers)

for row in DatafromApi.json()['data']:

    try:
        sheet_id = row.get('id')
        sheet_name = row.get('name')
        access_level = row.get('accessLevel')
        link = row.get('permalink')
        created_at = row.get('createdAt')
        modified_at = row.get('modifiedAt')
        dict_temp = {
                'sheet_id':[sheet_id],
                'sheet_name':[sheet_name],
                'access_level':[access_level],
                'link':[link],
                'created_at':[created_at],
                'modified_at':[modified_at]
        }
        df_temp = pd.DataFrame(dict_temp)
        df = pd.concat([df, df_temp])
        time.sleep(1.5)
    except:
        print('error while trying to get data.')


###########################

df.info()

if not df.empty:
    # Bigquery processing
    table_id = 'spartan-cedar-337400.smartsheets.smartsheets-list'

    # Prepare queries
    print('Prepare queries: ')
    print('- Delete queries')

    delete_query_general = "DELETE FROM `{0}` WHERE true"
    delete_query = delete_query_general.format(table_id)

    # Run delete queries
    print(delete_query)
    query_job = client_bigquery.query(delete_query)
    i = 0
    while True:
        print("Checking if query is done. Attempt: ", i)
        time.sleep(5)
        i += 1
        if query_job.done():
            print('Query is done!')
            break

    # Insert queries
    print('- Insert queries')

    # Transform dataframe into json object
    df_json = df.to_json(orient='records')
    df_json_object = json.loads(df_json)

    # Upload to BQ
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'NEWLINE_DELIMITED_JSON'
    job = client_bigquery.load_table_from_json(
        df_json_object, table_id, job_config=job_config)
    print('Done!')

    # clear workspace
    del df_json_object
    del df
