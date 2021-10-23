# Parcial2-BigData-2021

## 1. Programar una función AWS lambda para que realice scraping a yahoo finances(20 puntos): 
* Descargar cada dia las acciones de: 
    * Avianca 
    * Ecopetrol 
    * Grupo Aval 
    * Cementos Argos
* En S3 debe quedar la información con la estructura: s3://bucket/stocks/company=xxx/year=xxx/month=xxx/day=xxx 
* Se debe crear la respectiva base de datos en Athena para leer la información
* Se debe disparar un segundo lambda – para que cada día se cree automáticamente la respectiva partición en AWS Athena 

## 2. Programar una función AWS lambda que realice scraping al eltiempo.com y elespectador.com
* Descargar cada dia la página principal de cada periódico
* En S3 debe quedar la información con la estructura: s3://bucket/headlines/raw/periodico=xxx/year=xxx/month=xxx/day=xxx
* Una vez llega el archivo a la carpeta raw se debe disparar un lambda que procese los datos que llegaron. Este proceso debe extraer la categoría, el titular y el enlace para cada noticia. Estos datos se deben guardar en un csv en la siguiente ruta: s3://bucket/news/final/periodico=xxx/year=xxx/month=xxx/day=xxx
* Ejecutar un lambda usando boto3 – AWS Athena para que cada día se cree automáticamente la respectiva partición

Develop by: Ángel Fabián Nodarse Díaz (@fabiannoda) and Felipe Uribe Guevara (@FelipeUribe81)



