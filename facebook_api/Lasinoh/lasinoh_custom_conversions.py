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
'Lasinoh_Mexico':'act_410286810085364','Lasinoh_Panama':'act_253359839678017','Lasinoh_Peru':'act_694121314617272','Lasinoh_Latam':'act_881115419238511'}
#Facebook API Tokens
my_access_token = FB_ACCESS_TOKEN
my_app_id = FB_APP_ID
my_app_secret = FB_APP_SECRET

# Date parameters
start_date, end_date = set_start_end_date(1)
date_list = set_date_list(start_date,end_date)

config_file = open(os.getcwd()+'/config/layout_facebook_api.yaml','r')
config = yaml.safe_load(config_file)

fields = config.get('facebook').get('query').get('fields')
breakdowns = config.get('facebook').get('query').get('breakdowns_platform_position')
level = config.get('facebook').get('query').get('level')

# load BQ credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
client = bigquery.Client()

# Retrieve data each day in date-list
for date in date_list:
    for account_name in accounts_list.keys():
        # Obtener ID's de tabla en bigquery
        # Definir query
        print('Getting accounts from BigQuery Table')
        managers_query = "SELECT conversion_name, IF(REPLACE(conversion_name,'offsite_conversion.fb_pixel_custom.','') = '','fb_pixel_custom',REPLACE(conversion_name,'offsite_conversion.fb_pixel_custom.','')) AS name, account_id FROM `spartan-cedar-337400.facebook_insights.fb-custom-conversions-names` WHERE account_id ={0}"
        managers_query = managers_query.format(accounts_list[account_name][4:])
        print(managers_query)
        query_job = client.query(managers_query)
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
        df_managers_list = query_job.to_dataframe()
        print('Done!')
        df_managers_list.info()

        print('---- Account Name: ', account_name)
        account_id = accounts_list[account_name]
        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
        my_account = AdAccount(account_id)
    
        df = pd.DataFrame()

        params = {
            'level': level,
            'breakdowns': breakdowns,
            'time_range': {'since':f"{date}",'until':f"{date}"},
            'use_unified_attribution_setting': True

        }
        
        
        async_job = my_account.get_insights_async( fields=fields, params=params) 
        async_job.api_get()

        report_id = str(async_job[AdReportRun.Field.id])

        print('Attempting to retrieve Facebook API data from: ', str(date))
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
            print('Async job completed successfully! Date: ',str(date))
        else: 
            print('No data or API unavailable')

        print('Creating Dataframe...')
        print('------------------------')
        #for index,convnames in df_managers_list.iterrows():
        for item in dataFromAPI:
                account_id = item.get('account_id')
                campaign_id = item.get('campaign_id')
                adset_id = item.get('adset_id')
                ad_id = item.get('ad_id')
                date_start = item.get('date_start')
                date_stop = item.get('date_stop')
                placement = item.get('publisher_platform') + "|" +  item.get('platform_position') + "|" + item.get('device_platform')
                for index,convnames in df_managers_list.iterrows():
                    custom_conversion_name = convnames.get('name')
                    custom_conversion_id = 0
                    ########################################################
                    # custom conversion values
                    try:
                        # Set action dict
                        actions_dict = dict()
                        #Get each custom conversion value for the provide ID
                        for action in item.get('conversions'):
                            name = action.get('action_type')
                            dict_s = {
                                name: action.get('value')
                            }
                            actions_dict.update(dict_s)
                            # Assign Value to Each Action Variable
                        try:
                            custom_conversion_value = actions_dict.get(convnames.get('conversion_name'), 0)
                        except:
                            custom_conversion_value = 0    
                    except:
                        custom_conversion_value = 0

                        ########################################################
                    if custom_conversion_value != 0:
                        dict_temp = {
                                'account_id': [account_id],
                                'campaign_id': [campaign_id],
                                'adset_id': [adset_id],
                                'ad_id': [ad_id],
                                'date_start': [date_start],
                                'date_stop': [date_stop],
                                'placement': [placement],
                                'custom_conversion_id':[custom_conversion_id],
                                'custom_conversion_name': [custom_conversion_name],
                                'custom_conversion_value':[custom_conversion_value],
                                'custom_conv_type': 'Pixel'
                        }
                        df_temp = pd.DataFrame(dict_temp)
                        df = pd.concat([df, df_temp])

                    #########################

        #info
        df.info()

        if not df.empty:
            # set date column
            df['date'] = df['date_start']
            # remove date extra columns
            del df['date_start']
            del df['date_stop']
            #################################################################
            # Bigquery processing
            table_id = 'spartan-cedar-337400.facebook_insights.lasinoh-custom-conversions-v2'

            # Prepare queries
            print('Prepare queries: ')
            print('- Delete queries')

            delete_query_general = "DELETE FROM `{0}` WHERE date between '{1}' and '{2}' AND account_id ={3} AND custom_conv_type='Pixel'"
            delete_query = delete_query_general.format(table_id, date, date,account_id)

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

            #Insert queries
            print('- Insert queries')

            # Transform dataframe into json object
            df_json = df.to_json(orient='records')
            df_json_object = json.loads(df_json)

            #Upload to BQ
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'NEWLINE_DELIMITED_JSON'
            job = client.load_table_from_json(df_json_object, table_id, job_config = job_config)
            print('Done!')

            #clear workspace
            del df_json_object

        #Delete Facebook Report
        del df
        print('--Deleting Facebook Report')
        request_url = 'https://graph.facebook.com/v13.0/{}'.format(report_id)
        request_params = {'access_token': my_access_token}
        del_report = requests.delete(request_url,data=request_params)
        print(del_report)
