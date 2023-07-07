import requests
import os
from dotenv import load_dotenv
import sys
import psycopg2
from pyspark.sql import SparkSession

load_dotenv()

# Crea una session en Spark
def create_spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    """Se requiere un archivo .env con el path al driver que se encuentra en la carpeta DriverJDBC
    con el siguiente formato
    DRIVER_PATH= path"""
    spark = (
        SparkSession.builder
        .master("local")
        .appName("Conexion entre Pyspark y Amazon Redshift")
        .config("spark.jars", os.getenv('DRIVER_PATH'))
        .getOrCreate()
    )
    return spark

# Pega a la API y genera un DataFrame con los datos que nos interesa, ademas elimina posibles duplicados
def get_data():
    request = requests.get(
        "https://randomuser.me/api/?results=100").json()
    col_names = ["cellphone", "prefix", "first_name", "last_name", "email", "age", "date_of_birth", "nationality"]
    temp = (
        create_spark_session().createDataFrame(request["results"])
        .select("cell",
                "name.title", 
                "name.first",
                "name.last",
                "email",
                "dob.age",
                "dob.date",
                "nat")
        .dropDuplicates()
    )
    df = temp.toDF(*col_names)
    return df


# Crea un conector entre psycopg2 y RedShift
def connector():
    """Se requiere un archivo .env para poder cargar las credecinales de RedShift
    este tiene el sieguiente formato 
    HOST = host de redshift.amazonaws.com 
    PORT = numero de puerto
    DATABASE = database
    USER = usuario
    PASSWORD = contraseña"""
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )
    conn.autocommit = True
    return conn

# Inserta el DataFrame en una tabla en RedShift y luego hace una pequeña transformacion en la tabla 
def main():
    df = get_data()
    conn = connector()

    with conn.cursor() as cur:
        # Crea la tabla UsersInformation
        with open(r"create.sql", 'r') as content_file:
            cur.execute(content_file.read())
        (
        df
        .write
        .format("jdbc")
        .option("url", f"jdbc:redshift://{os.getenv('HOST')}:{int(os.getenv('PORT'))}/{os.getenv('DATABASE')}")
        .option("dbtable", "laferreresantiago_coderhouse.UsersInformation")
        .option("user", os.getenv('USER'))
        .option("password", os.getenv('PASSWORD'))
        .option("driver", "com.amazon.redshift.jdbc42.Driver")
        .mode("append")
        .save()
        )
        # Transforma la tabla UsersInformation
        with open(r"update.sql", 'r') as content_file:
            cur.execute(content_file.read())

        # Transforma la tabla UsersInformation
        with open(r"alter.sql", 'r') as content_file:
            cur.execute(content_file.read())

if __name__ == "__main__":
    main()
