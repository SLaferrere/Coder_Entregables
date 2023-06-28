import os
import sys
import requests
import psycopg2
from pyspark.sql import SparkSession
from dotenv import load_dotenv


load_dotenv()

# Crea session en Spark
def create_spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = (
        SparkSession.builder
        .master("local")
        .appName("Conexion entre Pyspark y Amazon Redshift")
        .config("spark.jars", os.getenv('DRIVER_PATH'))
        .getOrCreate()
    )
    return spark

# Pega a la API y devuelve un DataFrame donde se eliminan los posibles duplicados y con la data que nos interesa
def get_data():
    request = requests.get(
        "https://randomuser.me/api/?results=100").json()
    rows = []
    for people in request.get("results", []):
        rows.append({
            "preferable_prefix": people["name"]["title"],
            "full_name": people["name"]["first"] + " " + people["name"]["last"],
            "country": people["location"]["country"],
            "province": people["location"]["state"],
            "email": people["email"],
            "age": people["dob"]["age"],
            "date_of_birth": people["dob"]["date"][:10],
            "cellphone": people["cell"],
            "nationality": people["nat"]
        })
        df = create_spark_session().createDataFrame(rows).dropDuplicates()
    return df

# Crea una conexion con Redshift
def connector():
    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )
    conn.autocommit = True
    return conn

# Crea la tabla SIN valores y otra temporal con los datos del DataFrame y luego inserta dichos valores a la tabla final
def main():
    df = get_data()
    conn = connector()

    with conn.cursor() as cur:
        (
            df
            .select("preferable_prefix", "full_name", "country", "province", "email", "age", "date_of_birth", "cellphone", "nationality").write
            .format("jdbc")
            .option("url", f"jdbc:redshift://{os.getenv('HOST')}:{int(os.getenv('PORT'))}/{os.getenv('DATABASE')}")
            .option("dbtable", "laferreresantiago_coderhouse.Temp_UsersInformation")
            .option("user", os.getenv('USER'))
            .option("password", os.getenv('PASSWORD'))
            .option("driver", "com.amazon.redshift.jdbc42.Driver")
            .mode("overwrite")
            .save()
        )
        # Crea la tabla final con los data types que necesitamos
        with open(r"create.sql", 'r') as content_file:
            cur.execute(content_file.read())

        # Inserta en la tabla los datos conseguidos en el DataFrame mas una columna que nos avisa de los cumpleaños que hay en el mes en curso
        with open(r"insert.sql", 'r') as content_file:
            cur.execute(content_file.read())
        
        # Elimina la tambla Temp_UserInformation
        dropTable = "drop table laferreresantiago_coderhouse.Temp_UsersInformation"
        cur.execute(dropTable)


if __name__ == "__main__":
    main()
