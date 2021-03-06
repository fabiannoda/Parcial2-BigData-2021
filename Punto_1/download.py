from datetime import datetime, timedelta
import time
import pandas as pd
import urllib
import boto3
def format_date(date_datetime):
    """
    Hace un formateo de las fechas que vienen en datetime para convertirlo en tiempo en segundos transcurridos desde epoch en la hora local

    Args:
        date_datetime: Parámetro a formatear de tipo datetime

    Returns: 
        date_int: Objeto time formateado y casteado a entero
    """
    date_timetuple = date_datetime.timetuple()
    date_mktime = time.mktime(date_timetuple)
    date_int = int(date_mktime)
    return date_int

def handler(event, context):
    """
    Descarga un archivo .csv haciendo una petición a un 'end point' para distintos codigos de acciones, ésto a partir de la fecha del dia anterior, validando que sea un dia laboral y manejando la excepción para cuando no encuentre el archivo de la petición. Al finalizar se suben los resultados a un bucket de S3

    Args:
        event: Parámetro por defecto para una función lambda
        context: Parámetro por defecto para una función lambda
    
    Exceptions: 
        urllib.error.HTTPError: Si el archivo pedido no aparece se crea uno con las mismas columnas pero todos los valor en 0
    """
    s3 = boto3.resource('s3')
    dt_start = datetime(2021,10,21) - timedelta(days=1)
    dt_start = dt_start.replace(hour=23)
    if dt_start.weekday() < 5:
    
        start = format_date(dt_start)
        names = ['AVHOQ', 'EC', 'AVAL', 'CMTOY']

        for i in range(len(names)):
            try:
                df = pd.read_csv(f'https://query1.finance.yahoo.com/v7/finance/download/{names[i]}?period1={start}&period2={start}&interval=1d&events=history&includeAdjustedClose=true')
                df.to_csv(f'/tmp/{names[i]}.csv', index=False)
            except urllib.error.HTTPError as e:
                filesito = open(f'/tmp/{names[i]}.csv','w+')
                filesito.write(f'Date,Open,High,Low,Close,Adj Close,Volume \n{dt_start.strftime("%Y-%m-%d")},0,0,0,0,0,0')
                filesito.close()

            s3.meta.client.upload_file(f'/tmp/{names[i]}.csv', 'parcial2-bigdata-2021', f'stocks/company={names[i]}/year={dt_start.year}/month={dt_start.month}/day={dt_start.day}/{names[i]}.csv')
    else:
        print('fechas no validas')


def trigger(event, context):
    """
    Ejecuta un Athena query para actualizar las particiones de la tabla luego de creado un nuevo archivo en S3

    Args:
        event: Parámetro por defecto para una función lambda
        context: Parámetro por defecto para una función lambda
    """
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='msck repair table stocks',
        QueryExecutionContext={
            'Database': 'stocks',
        },
        ResultConfiguration={
            'OutputLocation': 's3://grades-bigdata-2021/paper/'
        },
        WorkGroup='primary'

    )