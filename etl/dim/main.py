import pytz
import concurrent.futures
from datetime import datetime
from etl.dim.common import base_job
from etl.utils.dataframe import get_param
from etl.utils.datatype import remove_symbol

# function for single process (use for test) or parallel for product
def thread(init_spark:object, info_dag:object, src_config:dict, dst_config:dict, param_config:dict, 
           hook:object, parallel:bool=False, etl_date:str=""):
    # Set timezone
    tz = pytz.timezone('Asia/Bangkok')
    
    # Create unique code_log with timestamp
    time_start = datetime.now(tz)
    info_dag.AIRFLOW_NAME = info_dag.AIRFLOW_NAME + "_" + time_start.strftime("%Y%m%d%H%M%S")
    # Insert log to STG_ETL_LOG with status = queue
    # hook.insert_log(info_dag, time_start, time_start, status_job='Q', status_finish='0', error_mess='', job_type="PARALLEL")
    
    try:
        base_job(init_spark=init_spark, info_dag=info_dag, src_config=src_config, dst_config=dst_config, 
                 param_config=param_config, hook=hook, etl_date=etl_date, parallel=parallel)
        # Update status job to STG_ETL_LOG   
        time_finish = datetime.now(tz)
        # hook.update_log(info_dag, time_finish, status_job='S', status_finish='1', error_mess='', job_type="PARALLEL")
        if parallel:
            return True
        
    except Exception as e:
        time_finish = datetime.now(tz)
        # Remove "" from message to avoid error with postgresql
        error_message = remove_symbol(str(e), "\'\"", with_replace='')
        print(error_message)
        # Update error to STG_ETL_LOG  
        # hook.update_log(info_dag, time_finish, status_job='F', status_finish='1', error_mess=error_message, job_type="PARALLEL")
        if parallel:
            # Return error to parallel process
            return info_dag.AIRFLOW_NAME + ": " + error_message
        else:
            # Raise exception to stop job in airflow
            raise Exception("Error run job")

def multithread(init_spark:object, list_job:list, info_dag:object, src_config:dict, dst_config:dict, param_config:dict, hook:object):
    # Get list param
    parallel = True
    dict_params = get_param(init_spark=init_spark, info_dag=info_dag, param_config=param_config)
    # Run multi thread with 15 worker
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        etl_task = {executor.submit(thread, init_spark, dag_config, src_config, dst_config, param_config, hook, 
                                    parallel, dict_params): dag_config for dag_config in list_job}
    return etl_task