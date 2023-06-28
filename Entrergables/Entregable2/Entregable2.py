import requests
import os
from dotenv import load_dotenv
import sys
import psycopg2
from pyspark.sql import SparkSession

# Crea session en Spark
def create_spark_session():
    load_dotenv()
    driver_path = r"C:\Users\satul\OneDrive\Documents\GitHub\Coder_Entregables\Entrergables\Entregable2\DriverJDBC\redshift-jdbc42-2.1.0.16.jar"
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    spark = SparkSession.builder \
            .master("local") \
            .appName("Conexion entre Pyspark y Amazon Redshift") \
            .config("spark.jars", driver_path) \
            .getOrCreate()
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

# Crea una conexion con Redshift y crea la tabla SIN valores, luego inserta los valores del DataFrame a la tabla
def main():
    df = get_data()
    load_dotenv()

    conn = psycopg2.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )
    conn.autocommit = True

# Crea la tabla final con los data types que necesitamos
    table = "CREATE TABLE if not exists laferreresantiago_coderhouse.UsersInformation \
                    (preferable_prefix varchar(50) NOT NULL, \
                    full_name varchar(50) NOT NULL, \
                    country varchar(50) NOT NULL, \
                    province varchar(50) NOT NULL, \
                    email varchar(50) NOT NULL DISTKEY, \
                    age int NOT NULL, \
                    date_of_birth DATE NOT NULL, \
                    cellphone varchar(50) NOT NULL, \
                    nationality varchar(50) NOT NULL,\
                    send_congratulations varchar(1) NOT NULL) \
                    compound sortkey(email)"
# Inserta en la tabla los datos conseguidos en el DataFrame mas una columna que nos avisa de los cumpleaños que hay en el mes en curso
    updates = "insert into laferreresantiago_coderhouse.UsersInformation \
                    with birthmonth as ( \
	                    select \
		                preferable_prefix, \
		                full_name, \
		                country, \
		                province, \
		                email, \
		                age, \
		                cast(date_of_birth as date), \
		                cellphone, \
		                nationality,\
	                    case when date_part(month, cast(date_of_birth as date)) = date_part(month, current_date) then 'Y'\
	                    else 'N' \
	                    end as send_congratulations \
	                from laferreresantiago_coderhouse.DataFrame u \
                ) \
                select * from birthmonth"
# Elimina la tambla temporal que contiene el DataFrame
    dropTable = "drop table laferreresantiago_coderhouse.DataFrame"
    with conn.cursor() as cur:
        df.select("preferable_prefix", "full_name", "country", "province", "email", "age", "date_of_birth", "cellphone", "nationality").write \
            .format("jdbc") \
            .option("url", f"jdbc:redshift://{os.getenv('HOST')}:{int(os.getenv('PORT'))}/{os.getenv('DATABASE')}") \
            .option("dbtable", "laferreresantiago_coderhouse.DataFrame") \
            .option("user", os.getenv('USER')) \
            .option("password", os.getenv('PASSWORD')) \
            .option("driver", "com.amazon.redshift.jdbc42.Driver") \
            .mode("overwrite") \
            .save()
        cur.execute(table)
        cur.execute(updates)
        cur.execute(dropTable)

        
        

if __name__ == "__main__":
    main()

