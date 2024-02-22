import os
import json
import pendulum
from airflow import DAG
from textwrap import dedent
from etl.raw import source2raw
from etl.utils.spark import Spark
from etl.utils.database import Hook
from etl.utils.datatype import json2object
from airflow.operators.python import PythonOperator

cwd = os.getenv("AIRFLOW_HOME")
info_dag = json2object("%s/dags/config/job/RAW/raw_che_prodictivity_details.json"%(cwd))

def etl_task():
    src_config = json.load(open("%s/dags/config/database/oracle.json"%(cwd)))
    catalog_config = json.load(open("%s/dags/config/database/minio.json"%(cwd)))
    param_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    log_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    spark_config = json.load(open("%s/dags/config/spark/spark_iceberg.json"%(cwd)))
    
    os.environ["AWS_ACCESS_KEY_ID"] = catalog_config["DB_USER"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = catalog_config["DB_PASSWORD"]
    os.environ["AWS_REGION"] = catalog_config["DB_REGION"]
    
    spark = Spark(spark_config)
    hook = Hook(log_config)
    source2raw.thread(init_spark=spark, info_dag=info_dag, src_config=src_config, param_config=param_config, hook=hook)

with DAG(
    info_dag.AIRFLOW_NAME,
    default_args={'retries': 0},
    description=info_dag.AIRFLOW_DESCRIPTION,
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=info_dag.AIRFLOW_TAGS,
) as dag:
    dag.doc_md = __doc__
    
    etl_task = PythonOperator(
        task_id='etl_task',
        python_callable=etl_task,
    )
    # etl_task.doc_md = dedent(
    #     """ extract task
    #         extract data
    #     """
    # )

    etl_task