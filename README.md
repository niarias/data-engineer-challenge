# Data Engineer Programming Test

Suponga que se presenta la necesidad de ingestar archivos planos de distintos aplicativos origen, realizarles pequeñas transformaciones (mayoritariamente de formato) y guardarlo en formato Apache Parquet.

Dada esta necesidad se pide generar una solución que contemple las siguientes características:

1.- Debe tener la flexibilidad suficiente para procesar archivos de texto ancho fijo y delimitados.

2.- Debe soportar cargas incrementales.

3.- Debe permitir el agregado de columnas al data frame con valores predefinidos.

4.- Como input debe recibir un archivo con los siguientes parámetros:
	
	a.- El path de origen del archivo a ser ingestado;
	b.- El path destino donde se escribirá ese archivo;
	c.- La metadata necesaria para procesar el archivo crudo (header y anchos de columnas);
	d.- La cantidad de archivos en los que se va a escribir la salida;

En lo posible se solicita que lo desarrollado sea capaz de ser ejecutado en cualquier computadora de forma local, con la menor cantidad de dependencias.	



### Sets de prueba
Se proveen dos archivos para realizar *pruebas*.

nyse_2012.csv, delimitado por comas, con cabecera. El parquet de salida debe estar particionado por una columna extra, llamada partition_date y cuyo valor viene dado por parámetro.

nyse_2012_ts.txt, de ancho fijo, sin cabecera y con las siguientes longitudes de columnas:

|columna|tipo de dato|longitud|
| ------------- | ------------- | ------------- |
|stock|string|6|
|transaction_date|timestamp|24|
|open_price|float|7|
|close_price|float|7|
|max_price|float|7|
|min_price|float|7|
|variation|float|7|

El parquet de salida debe estar particionado por la columna stock.


