#libraries
import json
import pandas as pd
import time
import datetime
import sys
import os
sys.path.insert(0, os.getcwd())
import pygsheets
# Bigquery library
from google.cloud import bigquery
pd.options.display.float_format = '{:,.2f}'.format

# load BQ credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
client_bigquery = bigquery.Client()
general_query = "SELECT * FROM `spartan-cedar-337400.google_sheets.lasinoh_subscribers_complete_table` WHERE country_code = 'EC'"
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
        city = row.get('city')
        region = row.get('region')
        country_name = row.get('country_name')
        country_code = row.get('country_code')
        date_subscribed = datetime.datetime.fromtimestamp(row.get('date_subscribed') / 1000.0, tz=datetime.timezone.utc)
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
    except:
        print('error while trying to process the script.')



###########################

if not df.empty:
    # converting timestamp to string
    df['date_subscribed'] = df['date_subscribed'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # reorder columns
    df = df[['date_subscribed','email','city','region','country_name','country_code','utm_source','utm_medium','utm_campaign','utm_content']]
    # Connection to Google Sheets
    gc = pygsheets.authorize(service_file=os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
    sh = gc.open_by_key('14qWAsAyCdFwe5bb-Qb5_MztvBM2-ID0gJIBi-hI_OVc')
    wks = sh.worksheet_by_title("Leads - EC")

    # Convert Dataframe to List
    df_list = df.values.tolist()

    # clear rows before insert
    wks.clear(start='A2', end=None, fields='userEnteredValue')
    # Insert new rows
    wks.append_table(df_list,start='A2')
    print('-- Rows created!')
    del df, df_temp
else:
    print('-- Accounts Synced')
