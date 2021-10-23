from utilities import *

def handler(event, context):
    print("Hello from zappa")
    print(event)
    uploadNewsHtmlS3('https://www.eltiempo.com/')
    uploadNewsHtmlS3('https://www.elespectador.com/')
    return {'status': 200}

def trigger(event, context):
    print("Hello from zappa in one trigger")
    print(event)
    downloadNewsHtmlS3('eltiempo')
    downloadNewsHtmlS3('elespectador')
    getEltiempoNews()
    getElespectadorNews()
    uploadNewscsvS3('eltiempo')
    uploadNewscsvS3('elespectador')
    repairPartitions()
    return {'status': 200}