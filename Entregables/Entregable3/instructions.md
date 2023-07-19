# Entregable 3
Entrega Nº 3 para el curso de Coderhouse de Data Engineer Flex.

# Distribución de los archivos
Los archivos a tener en cuenta son:
* `docker_images/`: Contiene los Dockerfiles para crear las imagenes utilizadas de Airflow y Spark.
* `docker-compose.yml`: Archivo de configuración de Docker Compose. Contiene la configuración de los servicios de Airflow y Spark.
* `.env`: Archivo de variables de entorno. Contiene variables de conexión a Redshift y driver de Postgres. (Incluido en el .gitignore)
* `dags/`: Carpeta con los archivos de los DAGs.
    * `etl_users.py`: DAG principal que ejecuta el pipeline de extracción, transformación y carga de datos de usuarios.
* `logs/`: Carpeta con los archivos de logs de Airflow.
* `plugins/`: Carpeta con los plugins de Airflow.
* `postgres_data/`: Carpeta con los datos de Postgres.
* `scripts/`: Carpeta con los scripts de Spark.
    * `redshift-jdbc42-2.1.0.16.jar`: Driver de Redshift para Spark.
    * `create.sql`: Script de SQL para la creación de la tabla final.
    * `update.sql`: Script de SQL para la transformación de la tabla final.
    * `ETL_usersinformation.py`: Script de Spark que ejecuta el ETL.

# Pasos para ejecutar el Entregable 3
1. Posicionarse en la carpeta `Entregables/Entregable3`. A esta altura debería ver el archivo `docker-compose.yml`.

2. Crear las siguientes carpetas a la misma altura del `docker-compose.yml`.
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```

3. Crear un archivo con variables de entorno llamado `.env` ubicado a la misma altura que el `docker-compose.yml`. Cuyo contenido sea:
```bash
HOST=..
PORT=5439
DATABASE=...
USER=...
PASSWORD=...
DRIVER_PATH=...
```

4. Hacer el build de las imagenes que se encuentran en `Entregables/Entregable3/docker_images/airflow` y `Entregables/Entregable3/docker_images/spark`. Los comandos de ejecución se 
encuentran en los mismos Dockerfiles.

5. Ejecutar el siguiente comando para levantar los servicios de Airflow y Spark.
```bash
docker-compose up --build
```
6. Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.

7. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:
    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`

8. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Spark:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`
    * Extra: `{"queue": "default"}`

9. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `driver_class_path`
    * Value: `/tmp/drivers/redshift-jdbc42-2.1.0.16.jar`

10. En la pestaña `Admin -> Variables` crear una nueva variable con los siguientes datos:
    * Key: `spark_scripts_dir`
    * Value: `/opt/airflow/scripts`

11. Ejecutar el DAG `etl_users`.
