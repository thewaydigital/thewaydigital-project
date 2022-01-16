#libraries
import json
import pandas as pd
import time
import sys
import os
sys.path.insert(0, os.getcwd())
import requests
# Bigquery library
from google.cloud import bigquery
pd.options.display.float_format = '{:,.2f}'.format

# load BQ credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
client_bigquery = bigquery.Client()
general_query = "SELECT * FROM `spartan-cedar-337400.google_sheets.lasinoh_subscribers_view`"
general_query = general_query.format()  
print(general_query)
query_job = client_bigquery.query(general_query)
# Checking if query is done
i = 0
while True:
    print
    "Checking if query is done. Attempt: ", i
    time.sleep(3)
    i += 1
    if query_job.done():
        print
        'Query is done!'
        break
print('Creating dataframe...')
df_manual_list = query_job.to_dataframe()
print('Done!')
df_manual_list.info()

#creating a Dataframe
df = pd.DataFrame()

for index,row in df_manual_list.iterrows():

    try:
        headers = {'Content-Type': 'application/json;'}
        response = requests.post(url='http://ip-api.com/json/'+row.get('ip_address'), headers=headers)

        print('post response code: ', response.status_code)
        city = response.json()['city']
        region = response.json()['regionName']
        country_name = response.json()['country']
        country_code = response.json()['countryCode']
        date_subscribed = row.get('date_subscribed')
        email = row.get('email')
        utm_source = row.get('utm_source')
        utm_medium = row.get('utm_medium')
        utm_campaign = row.get('utm_campaign')
        utm_content = row.get('utm_content')
        dict_temp = {
                'city':[city],
                'region':[region],
                'country_name':[country_name],
                'country_code':[country_code],
                'date_subscribed':[date_subscribed],
                'email':[email],
                'utm_source':[utm_source],
                'utm_medium':[utm_medium],
                'utm_campaign':[utm_campaign],
                'utm_content':[utm_content]
        }
        df_temp = pd.DataFrame(dict_temp)
        df = pd.concat([df, df_temp])
        time.sleep(1.5)
    except:
        print('error while trying to post.')
        country_name = row.get('ip_address')
        date_subscribed = row.get('date_subscribed')
        email = row.get('email')
        utm_source = row.get('utm_source')
        utm_medium = row.get('utm_medium')
        utm_campaign = row.get('utm_campaign')
        utm_content = row.get('utm_content')
        dict_temp = {
                'city':None,
                'region':None,
                'country_name':[country_name],
                'country_code':None,
                'date_subscribed':[date_subscribed],
                'email':[email],
                'utm_source':[utm_source],
                'utm_medium':[utm_medium],
                'utm_campaign':[utm_campaign],
                'utm_content':[utm_content]
        }
        df_temp = pd.DataFrame(dict_temp)
        df = pd.concat([df, df_temp])



###########################

df.info()

if not df.empty:
    # Bigquery processing
    table_id = 'spartan-cedar-337400.google_sheets.lasinoh_subscribers_complete_table'

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
