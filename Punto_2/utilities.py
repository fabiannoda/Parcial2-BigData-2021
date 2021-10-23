import requests
import os
from lxml import html
from datetime import datetime
import boto3
import pandas as pd

date = datetime(2021,10,23)

def uploadNewsHtmlS3(url):
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
    s3 = boto3.client('s3')
    s3Path = f'headlines/raw/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.html'
    localPath = f'/tmp/{newspeaper}.html'
    s3.download_file('parcial-2do-corte',s3Path,localPath)

def uploadNewscsvS3(newspeaper):
    s3 = boto3.resource('s3')
    s3Path = f'headlines/final/periodico={newspeaper}/year={date.year}/month={date.month}/day={date.day}/{newspeaper}.csv'
    localPath = f'/tmp/{newspeaper}.csv'
    s3.meta.client.upload_file(localPath, 'parcial-2do-corte', s3Path)

def getEltiempoNews():
    print("Procesando el arcivho html".center(50,'='))
    tree = html.parse('/tmp/eltiempo.html')

    categories = tree.xpath('//article//div[@class="category-published"]/a[1]/text()') 
    titles = tree.xpath('//article//h3[@class="title-container"]/a[@class="title page-link"]/text()')
    urls = tree.xpath('//article//h3[@class="title-container"]/a[@class="title page-link"]/@href') 
    urls = list(map(lambda x: f'https://www.eltiempo.com{x}', urls))

    columns = {'titles': titles, 'categories':categories, 'urls':urls}
    df = pd.DataFrame(columns)
    df.to_csv('/tmp/eltiempo.csv',index=False, encoding='utf-16', sep=';')

def getElespectadorNews():
    print("Procesando el arcivho html".center(50, '='))
    tree = html.parse('/tmp/elespectador.html')

    categories = tree.xpath('//div[@class="Card-Container"]/h4/a/text()') 
    titles = tree.xpath('//div[@class="Card-Container"]/h4/../h2/a/text()')
    urls = tree.xpath('//div[@class="Card-Container"]/h4/../h2/a/@href') 
    urls = list(map(lambda x: f'https://www.elespectador.com{x}', urls))

    columns = {'titles': titles, 'categories':categories, 'urls':urls}
    df = pd.DataFrame(columns)
    df.to_csv('/tmp/elespectador.csv', index=False, encoding='utf-16', sep=';')

def repairPartitions():
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