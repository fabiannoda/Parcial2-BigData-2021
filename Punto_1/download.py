from datetime import datetime, timedelta
import time
import pandas as pd
import urllib
import boto3
def format_date(date_datetime):
    date_timetuple = date_datetime.timetuple()
    date_mktime = time.mktime(date_timetuple)
    date_int = int(date_mktime)
    return date_int

def handler(event, context):
    s3 = boto3.resource('s3')
    dt_start = datetime.today() - timedelta(days=2)
    dt_end = datetime.today() - timedelta(days=1)
    if dt_start.weekday() in [0,1,2,3,6] and dt_end.weekday() in [0,1,2,3,4]:
    
        start = format_date(dt_start)
        end = format_date(dt_end)
        names = ['AVHOQ', 'EC', 'AVAL', 'CMTOY']

        for i in range(len(names)):
            try:
                df = pd.read_csv(f'https://query1.finance.yahoo.com/v7/finance/download/{names[i]}?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true')
                df.to_csv(f'/tmp/{names[i]}.csv', index=False)
            except urllib.error.HTTPError as e:
                filesito = open(f'/tmp/{names[i]}.csv','w+')
                filesito.write(f'Date,Open,High,Low,Close,Adj Close,Volume \n{dt_end.strftime("%Y-%m-%d")},0,0,0,0,0,0')
                filesito.close()

            s3.meta.client.upload_file(f'/tmp/{names[i]}.csv', 'parcial2-bigdata-2021', f'stocks/company={names[i]}/year={dt_end.year}/month={dt_end.month}/day={dt_end.day}/{names[i]}.csv')
    else:
        print('fechas no validas')


def trigger(event, context):
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='msck repair table stocks',
        QueryExecutionContext={
            'Database': 'stocks',
        }
    )