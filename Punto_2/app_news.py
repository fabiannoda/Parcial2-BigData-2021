from utilities import *



def handler(event, context):
    """
    La siguiente función contiene el flujo de llamados a funciones necesarias para subir
    los archivos HTML al bucket S3.

    Args:
        event: Parámetro por defecto para una función lambda
        context: Parámetro por defecto para una función lambda
    """
    print("Hello from zappa")
    print(event)
    uploadNewsHtmlS3('https://www.eltiempo.com/')
    uploadNewsHtmlS3('https://www.elespectador.com/')
    return {'status': 200}


def trigger(event, context):
    """
    La siguiente función contiene el flujo de llamados a funciones necesarias para descargar
    los archivos HTML de S3 al direcctorio temporal del lambda, procesar el contenido, subir
    los archivos procesados CSV al bucket en S3 y atualizar las particiones en Athena.

    Args:
        event: Parámetro por defecto para una función lambda
        context: Parámetro por defecto para una función lambda
    """
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