from datetime import datetime, timedelta
import time
import requests
import boto3
def format_date(date_datetime):
    date_timetuple = date_datetime.timetuple()
    date_mktime = time.mktime(date_timetuple)
    date_int = int(date_mktime)
    date_str = str(date_int)
    return date_str

def handler(event, context):
    s3 = boto3.resource('s3')
    names = ['EC']
    dt_start = datetime(2021,10,13)
    dt_end = datetime(2021,10,14)
    
    start = format_date(dt_start)
    end = format_date(dt_end)

    for i in range(len(names)):
        url = f'https://query1.finance.yahoo.com/v7/finance/download/{names[i]}?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true'
        r = requests.get(url)
        #open(f'{names[i]}.csv').write(r.content)
        #s3.meta.client.upload_file(f'{names[i]}.csv', 's3://parcial2-bigdata-2021/stocks/', f'{names[i]}.csv')


def trigger(event, context):
    print('funcione con un disparador')