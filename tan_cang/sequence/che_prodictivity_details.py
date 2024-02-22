import os
import json
import pendulum
from datetime import timedelta
from airflow import DAG
from textwrap import dedent
from etl.raw import source2raw
from etl.stg import raw2stg
from etl.smy import stg2smy
from etl.utils.spark import Spark
from etl.utils.database import Hook
from etl.utils.dataframe import get_param
from etl.utils.datatype import json2object, str2date, date2str
from airflow.operators.python import PythonOperator

cwd = os.getenv("AIRFLOW_HOME")

def raw_task():
    info_dag = json2object("%s/dags/config/job/RAW/raw_che_prodictivity_details.json"%(cwd))
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

def stg_task():
    info_dag = json2object("%s/dags/config/job/STG/stg_che_productivity_details.json"%(cwd))
    dst_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    param_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    catalog_config = json.load(open("%s/dags/config/database/minio.json"%(cwd)))
    log_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    spark_config = json.load(open("%s/dags/config/spark/spark_iceberg.json"%(cwd)))
    
    os.environ["AWS_ACCESS_KEY_ID"] = catalog_config["DB_USER"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = catalog_config["DB_PASSWORD"]
    os.environ["AWS_REGION"] = catalog_config["DB_REGION"]
    
    spark = Spark(spark_config)
    hook = Hook(log_config)
    raw2stg.thread(init_spark=spark, info_dag=info_dag, dst_config=dst_config, param_config=param_config, hook=hook)

def smy_task():
    info_dag = json2object("%s/dags/config/job/SMY/smy_che_productivity_details.json"%(cwd))
    src_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    dst_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    param_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    log_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    spark_config = json.load(open("%s/dags/config/spark/spark_iceberg.json"%(cwd)))
    
    spark = Spark(spark_config)
    hook = Hook(log_config)
    stg2smy.thread(init_spark=spark, info_dag=info_dag, src_config=src_config, dst_config=dst_config, 
                   param_config=param_config, hook=hook)

def update_param():
    info_dag = json2object("%s/dags/config/job/SMY/smy_che_productivity_details.json"%(cwd))
    param_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    spark_config = json.load(open("%s/dags/config/spark/spark_iceberg.json"%(cwd)))
    log_config = json.load(open("%s/dags/config/database/postgres.json"%(cwd)))
    spark = Spark(spark_config)
    hook = Hook(log_config)
    
    dict_param = get_param(init_spark=spark, info_dag=info_dag, param_config=param_config)
    str_current_date = dict_param["ETL_DATE"]
    if str_current_date != "20231002":
        date_obj = str2date(str_current_date)
        next_date = date_obj + timedelta(1)
        str_next_date = date2str(next_date, format="%Y%m%d")
        hook.execute(""" UPDATE "DWH_FACT"."DWH_PARAM"
                        SET "GIA_TRI" = '%s' 
                        WHERE "MA_THAM_SO" = 'ETL_DATE';
                                        """%(str_next_date))
        
with DAG(
    "che_prodictivity_details",
    default_args={'retries': 0},
    description="",
    # schedule_interval='*/3 * * * *',
    schedule_interval=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["job_tong"],
    max_active_runs=1,
) as dag:
    dag.doc_md = __doc__
    
    raw_task = PythonOperator(
        task_id='raw_task',
        python_callable=raw_task,
    )
    # etl_task.doc_md = dedent(
    #     """ extract task
    #         extract data
    #     """
    # )
    stg_task = PythonOperator(
        task_id='stg_task',
        python_callable=stg_task,
    )
    
    smy_task = PythonOperator(
        task_id='smy_task',
        python_callable=smy_task,
    )

    update_param = PythonOperator(
        task_id='update_param',
        python_callable=update_param,
    )
    
    raw_task >> stg_task >> smy_task >> update_param