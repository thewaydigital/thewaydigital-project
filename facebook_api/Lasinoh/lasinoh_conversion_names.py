#libraries
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
import time
#github referecen libraries
import sys
import os
sys.path.insert(0,os.getcwd())
from datetime import date
import json
import pandas as pd
import requests
from tools import *
import yaml
from config.credentials_facebook_ads import *

#Bigquery library
from google.cloud import bigquery
pd.options.display.float_format = '{:,.2f}'.format


accounts_list = {'Lasinoh_Argentina':'act_167239824878114','Lasinoh_Bolivia':'act_115849660425891','Lasinoh_Chile':'act_176237457634349',
'Lasinoh_Colombia':'act_1553612921694258','Lasinoh_Costa_Rica':'act_2632858513596871','Lasinoh_Ecuador':'act_2749994491981998',
'Lasinoh_Mexico':'act_410286810085364','Lasinoh_Panama':'act_253359839678017','Lasinoh_Peru':'act_694121314617272'}
# Facebook API Tokens
my_access_token = FB_ACCESS_TOKEN
my_app_id = FB_APP_ID
my_app_secret = FB_APP_SECRET


config_file = open(os.getcwd()+'/config/layout_facebook_api.yaml', 'r')
config = yaml.safe_load(config_file)

fields = config.get('facebook').get('query').get('fields')

# Retrieve data each day in date-list
for account_name in accounts_list.keys():

    print('---- Account Name: ', account_name)
    account_id = accounts_list[account_name]
    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
    my_account = AdAccount(account_id)


    df = pd.DataFrame()

    params = {
        'date_preset':'last_30d'
    }

    async_job = my_account.get_insights_async(fields=fields, params=params)
    async_job.api_get()

    report_id = str(async_job[AdReportRun.Field.id])

    print('Attempting to retrieve Facebook API data from last 30 days. ')
    print('-- Report ID: ', report_id)

    while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[AdReportRun.Field.async_percent_completion] < 100:
        time.sleep(5)
        async_job.api_get()
    time.sleep(1)

    print(async_job.keys())

    if 'error' in async_job.keys():
        print(async_job)
    if "Job Completed" in async_job[AdReportRun.Field.async_status]:
        dataFromAPI = async_job.get_result()
        print('Async job completed successfully! Date: ', str(date))
    else:
        print('No data or API unavailable')

    print('Creating Dataframe...')
    print('------------------------')
    for item in dataFromAPI:
        account_id = item.get('account_id')
        ########################################################
        for conv in item.get('conversions'):
            conversion_name = conv.get('action_type')

            dict_temp = {
                'account_id': [account_id],
                'conversion_name': [conversion_name],
            }
            df_temp = pd.DataFrame(dict_temp)
            df = pd.concat([df, df_temp])

        #########################

    # info
    df.info()

    if not df.empty:
        #################################################################
        # Bigquery processing
        # load BQ credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
        client = bigquery.Client()
        table_id = 'spartan-cedar-337400.facebook_insights.fb-custom-conversions-names'

        # Prepare queries
        print('Prepare queries: ')
        print('- Delete queries')

        delete_query_general = "DELETE FROM `{0}` WHERE account_id ={1}"
        delete_query = delete_query_general.format(
            table_id, account_id)

        # Run delete queries
        print(delete_query)
        query_job = client.query(delete_query)
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
        job = client.load_table_from_json(
            df_json_object, table_id, job_config=job_config)
        print('Done!')

        # clear workspace
        del df_json_object

    # Delete Facebook Report
    del df
    print('--Deleting Facebook Report')
    request_url = 'https://graph.facebook.com/v13.0/{}'.format(report_id)
    request_params = {'access_token': my_access_token}
    del_report = requests.delete(request_url, data=request_params)
    print(del_report)
