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
general_query = "SELECT * FROM `spartan-cedar-337400.smartsheets.smartsheets-list` WHERE sheet_id = 3900867691734916"
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

#Load Smartsheets credentials
config_file = open(os.getcwd()+'/config/credentials_smartsheets.yaml','r')
config = yaml.safe_load(config_file)

api_key = config.get('smartsheets').get('query').get('api_key')

for index,row in df_manual_list.iterrows():

    try:
        headers = {'Content-Type': 'application/json;','Authorization':'Bearer '+api_key}
        response = requests.get("https://api.smartsheet.com/2.0/sheets/"+str(row.get('sheet_id')), headers=headers)
        print('post response code: ', response.status_code)
        for item in response.json()['rows']:
            created_at = item.get('createdAt')
            modified_at = item.get('modifiedAt')
            for content in item.get('cells'):
                try:
                    if content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('date_of_enquiry'):
                        date_of_enquiry = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('name'):
                        name = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('las_name'):
                        las_name = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('email'):
                        email = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('phone'):
                        phone = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('questions'):
                        questions = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('zip_code'):
                        zip_code = content.get('displayValue')   
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('extra_comments'):
                        extra_comments = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('actioned_by'):
                        actioned_by = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('date'):
                        date = content.get('value')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('action_comments'):
                        action_comments = content.get('displayValue')
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('nationality'):
                        nationality = content.get('displayValue')    
                    elif content.get('columnId') == config.get('smartsheets').get('contact_from_columns').get('country'):
                        country = content.get('displayValue')                       
                except:
                    print('Error trying to get columnId and displayValue.')
            dict_temp = {
                'created_at':[created_at],
                'modified_at':[modified_at],
                'date_of_enquiry':[date_of_enquiry],
                'name':[name],
                'las_name':[las_name],
                'email':[email],
                'phone':[phone],
                'questions':[questions],
                'zip_code':[zip_code],
                'extra_comments':[extra_comments],
                'actioned_by':[actioned_by],
                'date':[date],
                'action_comments':[action_comments],
                'nationality':[nationality],
                'country':[country]
            }
            df_temp = pd.DataFrame(dict_temp)
            df = pd.concat([df, df_temp])
            #time.sleep(0.5)
    except:
        print("error in Get request.")


###########################

df.info()

if not df.empty:
    # Bigquery processing
    table_id = 'spartan-cedar-337400.smartsheets.smartsheets-contact-form'
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
