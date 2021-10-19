import requests
import os
from lxml import html
import pandas as pd

# url = "https://www.eltiempo.com/"

# headers = {"Accept": "application/json"}

# response = requests.request("GET", url, headers=headers)

# print(response.text)

# myfile = open('sample.html','w')

# myfile.write(response.text)

for i in range(1,4):
    pageContent = requests.get('https://www.eltiempo.com/')
    tree = html.fromstring(pageContent.content)
    category = tree.xpath(f'/html/body//article[{i}]/div[2]/div/a/text()')
    title = tree.xpath(f'/html/body//article[{i}]/div[2]/h3/a/text()')
    removeElementsfromTitle = [title.remove(i) for i in title if i == "\n"]
    url =  tree.xpath(f'/html/body//article[{i}]/div[2]/h3/a[2]/@href')
    url = list(map(lambda x: f'https://www.eltiempo.com/{x}', url))
    print(f'La categoria es:{category}\n\nEl titulo es:{title}\n\nLa url es:{url}')
