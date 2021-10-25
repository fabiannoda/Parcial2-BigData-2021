import requests
import os
from lxml import html
from datetime import datetime
import boto3
import pandas as pd

"""
Variable global hace referencia a la fecha en el momento de ejecución.
"""
date = datetime.now()


def uploadNewsHtmlS3(url):
    """
    La siguiente función sube un archivo HTML con destino bucket en S3.

    Args:
        url: Parametro que hace referencia a la página web de la cual se desea 
        extraer la información.
    """
    s3 = boto3.resource('s3')
    url = url
    newspeaper = url.split(".")[1]
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers=headers)
    localPath = f'/tmp/{newspeaper}_raw.html'
    myfile = open(localPath,'w', encoding='utf-16')
    myfile.write(str(response.text))
    myfile.close()
    s3Path =  f'headlines/raw/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.html'
    s3.meta.client.upload_file(localPath, 'parcial-2do-corte', s3Path)


def downloadNewsHtmlS3(newspeaper):
    """
    La siguiente función descarga un archivo HTML desde un bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo alojado en S3 y el que se guarda en el lambda.
    """
    s3 = boto3.client('s3')
    s3Path = f'headlines/raw/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.html'
    localPath = f'/tmp/{newspeaper}.html'
    s3.download_file('parcial-2do-corte',s3Path,localPath)


def uploadNewscsvS3(newspeaper):
    """
    La siguiente función sube un archivo CSV con destino bucket en S3.

    Args:
        newspeaper: Parametro que hace referencia al nombre del periorico, que a 
        su vez es el nombre del archivo que será alojado en S3.
    """
    s3 = boto3.resource('s3')
    s3Path = f'headlines/final/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.csv'
    localPath = f'/tmp/{newspeaper}.csv'
    s3.meta.client.upload_file(localPath, 'parcial-2do-corte', s3Path)


def getEltiempoNews():
    """
    La siguiente función procesa el archivo eltiempo.HTML que se encuentra en el direcctorio 
    temporal de Lambda, para ello utiliza los XPATH de la categoria, el título y 
    extrae el texto, url.

    Exceptions: 
        ValueError: Cuando el tamaño de las listas no coincide después de procesar los titulos,
        se llenan los espacios vacios con 0.
    """
    print("Procesando el arcivho eltiempo.html".center(50,'='))
    tree = html.parse('/tmp/eltiempo.html')

    categories = tree.xpath('//article//div[@class="category-published"]/a[1]/text()') 
    titles = tree.xpath('//article//h3[@class="title-container"]/a[@class="title page-link"]/text()')
    urls = tree.xpath('//article//h3[@class="title-container"]/a[@class="title page-link"]/@href') 
    urls = list(map(lambda x: f'https://www.eltiempo.com{x}', urls))
    columns = {'titles': titles, 'categories':categories, 'urls':urls}

    try:
        df = pd.DataFrame(columns)
        df.to_csv('/tmp/eltiempo.csv', index=False, encoding='utf-16', sep=';')
    except ValueError:
        max_length = max([len(titles),len(urls),len(columns)])
        for column in list(columns.keys()):
            discount = max_length - len(columns[column])
            if discount != 0:
                for i in range(discount):
                    columns[f'{column}'].append(0)
        df = pd.DataFrame(columns)
        df.to_csv('/tmp/eltiempo.csv', index=False, encoding='utf-16', sep=';')


def getElespectadorNews():
    """
    La siguiente función procesa el archivo elespectador.HTML que se encuentra en el direcctorio 
    temporal de Lambda, para ello utiliza los XPATH de la categoria, el título y 
    extrae el texto, url.

    Exceptions: 
        ValueError: Cuando el tamaño de las listas no coincide después de procesar los titulos,
        se llenan los espacios vacios con 0.
    """
    print("Procesando el arcivho elespectador.html".center(50, '='))
    tree = html.parse('/tmp/elespectador.html')

    categories = tree.xpath('//div[@class="Card-Container"]/h4/a/text()') 
    titles = tree.xpath('//div[@class="Card-Container"]/h4/../h2/a/text()')
    urls = tree.xpath('//div[@class="Card-Container"]/h4/../h2/a/@href') 
    urls = list(map(lambda x: f'https://www.elespectador.com{x}', urls))
    columns = {'titles': titles, 'categories':categories, 'urls':urls}

    try:
        df = pd.DataFrame(columns)
        df.to_csv('/tmp/elespectador.csv', index=False, encoding='utf-16', sep=';')
    except ValueError:
        max_length = max([len(titles),len(urls),len(columns)])
        for column in list(columns.keys()):
            discount = max_length - len(columns[column])
            if discount != 0:
                for i in range(discount):
                    columns[f'{column}'].append(0)
        df = pd.DataFrame(columns)
        df.to_csv('/tmp/elespectador.csv', index=False, encoding='utf-16', sep=';')
        
    
def repairPartitions():
    """
    La siguiente función ejecuta un Athena query para actualizar las particiones de la tabla luego 
    de creado un nuevo archivo en S3.

    """
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString='msck repair table final',
        QueryExecutionContext={
            'Database': 'news',
        },
        ResultConfiguration={
        'OutputLocation': 's3://parcial-2do-corte/garbage/'
    },
    WorkGroup='primary'
    )