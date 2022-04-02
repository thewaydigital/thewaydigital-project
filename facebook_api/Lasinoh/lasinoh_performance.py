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

# Date parameters
start_date, end_date = set_start_end_date(1)
date_list = set_date_list(start_date,end_date)

accounts_list = {'Lasinoh_Argentina':'act_167239824878114','Lasinoh_Bolivia':'act_115849660425891','Lasinoh_Chile':'act_176237457634349',
'Lasinoh_Colombia':'act_1553612921694258','Lasinoh_Costa_Rica':'act_2632858513596871','Lasinoh_Ecuador':'act_2749994491981998',
'Lasinoh_Mexico':'act_410286810085364','Lasinoh_Panama':'act_253359839678017','Lasinoh_Peru':'act_694121314617272'}

#Facebook API Tokens
my_access_token = FB_ACCESS_TOKEN
my_app_id = FB_APP_ID
my_app_secret = FB_APP_SECRET

config_file = open(os.getcwd()+'/config/layout_facebook_api.yaml','r')
config = yaml.safe_load(config_file)

fields = config.get('facebook').get('query').get('fields_videos_plays')
breakdowns = config.get('facebook').get('query').get('breakdowns_platform_position')
level = config.get('facebook').get('query').get('level')

# Retrieve data each day in date-list
for date in date_list:
    for account_name in accounts_list.keys():
        print('---- Account Name: ', account_name)
        account_id = accounts_list[account_name]
        FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)
        my_account = AdAccount(account_id)

        df = pd.DataFrame()

        params = {
            'level': level,
            #'filtering': [],
            'breakdowns': breakdowns,
            'time_range': {'since':f"{date}",'until':f"{date}"},
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
            print('Async job completed successfully! Date: ', str(date))
        else: 
            print('No data or API unavailable')

        print('Creating Dataframe...')
        print('------------------------')

        for item in dataFromAPI:
            account_id = item.get('account_id')
            account_name = item.get('account_name')
            campaign_id = item.get('campaign_id')
            campaign_name = item.get('campaign_name')
            adset_id = item.get('adset_id')
            adset_name = item.get('adset_name')
            ad_id = item.get('ad_id')
            ad_name = item.get('ad_name')
            impressions = item.get('impressions')
            clicks = item.get('clicks')
            spend = item.get('spend')
            inline_link_clicks = item.get('inline_link_clicks')
            date_start = item.get('date_start')
            date_stop = item.get('date_stop')
            device_platform = item.get('device_platform')
            platform_position = item.get('platform_position')
            publisher_platform = item.get('publisher_platform')
            #video_views_25_watched = item.get('video_p25_watched_actions') 
            #video_views_50_watched = item.get('video_p50_watched_actions') 
            #video_views_100_watched = item.get('video_p100_watched_actions') 

            ########################################################
            # Actions
            # Set actions dict
            actions_dict = dict()
            thruplays_dict = dict()
            try:
                for action in item.get('actions'):
                    name = action.get('action_type')
                    dict_s = {
                        name: action.get('value')
                    }
                    actions_dict.update(dict_s)
                """
                for thruplays in item.get('video_play_actions'):
                    name = thruplays.get('action_type')
                    dict_x ={
                        name: thruplays.get('value')
                    }
                    thruplays_dict.update(dict_x)
                """
                # Assign Value to Each Action Variable
                try:
                    post_save = actions_dict.get('onsite_conversion.post_save', 0)
                except:
                    post_save = 0
              
                try:
                    link_click = actions_dict.get('link_click', 0)
                except:
                    link_click = 0
                
                try:
                    post_reaction = actions_dict.get('post_reaction', 0)
                except:
                    post_reaction = 0

                try:
                    post_engagement = actions_dict.get('post_engagement', 0)
                except:
                    post_engagement = 0

                try:
                    page_engagement = actions_dict.get('page_engagement', 0)
                except:
                    page_engagement = 0

                try:
                    leads = actions_dict.get('offsite_conversion.fb_pixel_lead', 0)
                except :
                    leads = 0

                try:
                    fb_mobile_activate_app = actions_dict.get('app_custom_event.fb_mobile_activate_app', 0)
                except :
                    fb_mobile_activate_app = 0

                try:
                    mobile_app_install = actions_dict.get('mobile_app_install', 0)
                except :
                    mobile_app_install = 0

                try:
                    video_view = actions_dict.get('video_view', 0)
                except :
                    video_view = 0
                """ 
                try:
                    video_play_actions = thruplays_dict.get('video_view', 0)
                except :
                    video_play_actions = 0
                """
                try:
                    custom_conversion_fb_pixel = actions_dict.get('offsite_conversion.fb_pixel_custom',0)
                except:
                    custom_conversion_fb_pixel = 0 

            except :
                # No conversions
                post_save = 0
                link_click = 0
                post_reaction = 0
                post_engagement = 0
                page_engagement = 0
                leads = 0
                fb_mobile_activate_app = 0
                mobile_app_install = 0
                video_view = 0
                video_play_actions = 0 
                custom_conversion_fb_pixel = 0 

            ########################################################

            dict_temp = {
                'account_id':[account_id],
                'account_name':[account_name],
                'campaign_id': [campaign_id],
                'campaign_name': [campaign_name],
                'adset_id': [adset_id],
                'adset_name': [adset_name],
                'ad_id': [ad_id],
                'ad_name': [ad_name],
                'impressions': [impressions],
                'clicks': [clicks],
                'spend': [spend],
                'inline_link_clicks': [inline_link_clicks],
                'date_start': [date_start],
                'date_stop': [date_stop],
                'device_platform': [device_platform],
                'platform_position': [platform_position],
                'publisher_platform': [publisher_platform],
                'post_save': [post_save],
                'link_click': [link_click],
                'post_reaction': [post_reaction],
                'post_engagement': [post_engagement],
                'page_engagement': [page_engagement],
                'leads': [leads],
                'fb_mobile_activate_app': [fb_mobile_activate_app],
                'mobile_app_install': [mobile_app_install],
                'video_view': [video_view],
                'video_play_actions':[video_play_actions],
                'custom_conversion_fb_pixel':[custom_conversion_fb_pixel]
                #'video_views_25_watched':[video_views_25_watched],
                #'video_views_50_watched':[video_views_50_watched],
                #'video_views_100_watched':[video_views_100_watched]
            }
            df_temp = pd.DataFrame(dict_temp)
            df = pd.concat([df, df_temp])

        #########################

        #info
        df.info()
        #df.to_csv('lasinoh_argentina.csv')
         
        if not df.empty:
            # set date column
            df['date'] = df['date_start']
            # remove date extra columns
            del df['date_start']
            del df['date_stop']
            #################################################################
            # Bigquery processing
            # load BQ credentials
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (os.getcwd()+'/config/'+'spartan-cedar-337400-178c9ede4da3.json')
            client = bigquery.Client()
            table_id = 'spartan-cedar-337400.facebook_insights.lasinoh_performance'

            # Prepare queries
            print('Prepare queries: ')
            print('- Delete queries')

            delete_query_general = "DELETE FROM `{0}` WHERE account_id ={3} AND date between '{1}' and '{2}'"
            delete_query = delete_query_general.format(table_id, date,date,account_id)

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
        request_url = 'https://graph.facebook.com/v12.0/{}'.format(report_id)
        request_params = {'access_token': my_access_token}
        del_report = requests.delete(request_url,data=request_params)
        print(del_report)
        