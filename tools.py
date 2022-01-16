#libraries
import datetime
import pandas as pd
import os

#functions
# get a date range. Default: end_date = today; start_date = two days before today
def set_start_end_date(days_range=2):
    if os.getenv('end_date') == None:
        end_date = (datetime.datetime.today()).strftime('%Y-%m-%d')
    else:
        end_date = os.getenv('end_date')

    if os.getenv('start_date') == None:
        start_date = (datetime.datetime.today() - datetime.timedelta(days=days_range)).strftime('%Y-%m-%d')
    else:
        start_date = os.getenv('start_date')
    print (start_date)
    print (end_date)
    return start_date , end_date

def set_date_list(start_date, end_date):
    sd = str(start_date).split("-")
    ed = str(end_date).split("-")
    start_date = datetime.date(int(sd[0]), int(sd[1]), int(sd[2]))
    end_date = datetime.date(int(ed[0]), int(ed[1]), int(ed[2])) + datetime.timedelta(days=1)
    date_list = []
    for n in range(int((end_date - start_date).days)):
        date_list.append(start_date + datetime.timedelta(n))
    return date_list


def ga_response_to_df(response):
    list = []
    # get report data
    for report in response.get('reports', []):
        #print("Report header: ", report.keys())
        # set column headers
        columnHeader = report.get('columnHeader', {})
        #print("Column header: ", columnHeader.keys())

        dimensionHeaders = columnHeader.get('dimensions', [])
        #print("Dimension header: ", dimensionHeaders)

        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
        rows = report.get('data', {}).get('rows', [])
        #print("Rows: ", rows)

    for row in rows:
        # create dict for each row
        dict = {}
        dimensions = row.get('dimensions', [])
        dateRangeValues = row.get('metrics', [])

        # fill dict with dimension header (key) and dimension value (value)
        # print(zip(dimensionHeaders, dimensions))
        for header, dimension in zip(dimensionHeaders, dimensions):
            # print("header, dimension: ", header, dimension)
            dict[header] = dimension

        # fill dict with metric header (key) and metric value (value)
        for i, values in enumerate(dateRangeValues):
            for metric, value in zip(metricHeaders, values.get('values')):
                # set int as int, float a float
                if ',' in value or '.' in value:
                    dict[metric.get('name')] = float(value)
                else:
                    dict[metric.get('name')] = int(value)
        #print(dict)
        list.append(dict)

    df = pd.DataFrame(list)
    return df