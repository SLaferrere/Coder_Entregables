import requests
import redshift_connector
import pandas as pd
import os
from dotenv import load_dotenv

# Pega a la API y genera un dataframe

def get_data():
    request = requests.get(
        "https://randomuser.me/api/?results=30").json()
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
        df = pd.DataFrame(rows)
    return df


# Crea la tabla (si no existe) en Redshift con los datos del dataframe
def main():
    load_dotenv()

    conn = redshift_connector.connect(
        host=os.getenv("HOST"),
        port=int(os.getenv("PORT")),
        database=os.getenv("DATABASE"),
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD")
    )

    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute("CREATE TABLE if not exists laferreresantiago_coderhouse.UsersInformation \
                    (preferable_prefix varchar(50) NOT NULL, \
                    full_name varchar(50) NOT NULL SORTKEY, \
                    country varchar(50) NOT NULL, \
                    province varchar(50) NOT NULL, \
                    email varchar(50) NOT NULL DISTKEY, \
                    age int NOT NULL, \
                    date_of_birth DATE NOT NULL, \
                    cellphone varchar(50) NOT NULL, \
                    nationality varchar(50) NOT NULL)")
        cur.write_dataframe(get_data(), 'usersinformation')


if __name__ == "__main__":
    main()
