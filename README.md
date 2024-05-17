# SPANISH NEWS CRAWLER

## PROCESO

- 1 Leemos la base de datos.
- 2 Cargamos los root_source del archivo de configuración.(ejemplo 20 minutos)
- 3 Para cada root_source generamos las sources.(20 minutos barcelona, 20 minutos madrid...)
- 4 Cargamos las sources que tenemos pendientes en la base de datos.
- 5 Añadimos las sources susceptibles de descargarse para hoy, según el archivo de configuración y las añadimos a su root_source.
- 6 Descargamos medio---> Login automático y descarga.
- 7 Guardamos la fila tanto si se ha descargado como si no con el estado, en la base de datos con fecha de hoy.
- 8 Los root_sources mensuales se intentan descargar desde primero de mes hasta que se haya publicado.
- 9 Ejecución de report cada día a las 23:30 para comprobar los que fallan, y tener un control.

** se ha obviado algunos archivos de configuración por contener login de los medios etc.**
