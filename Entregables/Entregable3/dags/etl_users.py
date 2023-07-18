# Este es el DAG que orquesta el ETL de la tabla users

from airflow import DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


from airflow.models import Variable

from datetime import datetime, timedelta


defaul_args = {
    "owner": "Santiago Laferrere",
    "start_date": datetime(2023, 7, 18),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id = "etl_users",
    default_args = defaul_args,
    description = "ETL de la tabla usersinformation",
    schedule_interval = "@daily",
    catchup = False,
) as dag:
    
    # Crea la tabla UsersInformation    
    with open("./scripts/create.sql") as f:
        create_table = SQLExecuteQueryOperator(
            task_id = "crate_table",
            conn_id = "redshift_default",
            sql = f.read(),
            dag = dag,
        )

    # Extrae la data de la API y la inserta en la tabla
    spark_etl_users = SparkSubmitOperator(
        task_id = "spark_etl_users",
        application = f'{Variable.get("spark_scripts_dir")}/ETL_usersinformation.py',
        conn_id = "spark_default",
        dag = dag,
        driver_class_path = Variable.get("driver_class_path"),
    )

    # Transforma la tabla UsersInformation
    with open('./scripts/update.sql') as d:
        update_table = SQLExecuteQueryOperator(
            task_id = "update_table",
            conn_id = "redshift_default",
            sql = d.read(),
            dag = dag,
        )

    create_table >> spark_etl_users >> update_table