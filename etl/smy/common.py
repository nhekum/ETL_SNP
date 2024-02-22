import pytz
from datetime import datetime
from etl.utils.dataframe import get_param
from etl.utils.datatype import string2query
from pyspark.sql.functions import lit, expr
     
def base_job(init_spark:object, info_dag:object, src_config:dict, param_config:dict, dst_config:dict, hook:object, etl_date:str, parallel:bool):
    if parallel:
        str_current_date = etl_date
    else:
        # Get param
        if hasattr(info_dag, 'PARAM_CONDITION') and info_dag.PARAM_CONDITION:
            if len(info_dag.PARAM_CONDITION) >1:
                pass
            else:
                dict_param = get_param(init_spark=init_spark, info_dag=info_dag, param_config=param_config)
                str_current_date = dict_param[info_dag.PARAM_CONDITION[0]]
        else:
            # Check ETL date, if it is empty, we'll get current date
            tz = pytz.timezone('Asia/Bangkok')
            str_current_date = datetime.now(tz).strftime("%Y%m%d")
    
    # Create table name
    dbtable = "\"%s\".\"%s\""%(info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
    # Create query for delete data
    delete_sql_query = """DELETE FROM "{}"."{}" WHERE "{}"={} """.format(info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE, 
                                                                            info_dag.TARGET_ETL_DATE, int(str_current_date))
        
    # Delete records with condition datetime
    hook.execute(delete_sql_query)
    
    query_stg = string2query(src_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
    df_stg = init_spark.read_db(src_config, query_stg)
        
    # Assign datetime for new records
    new_df_stg =  df_stg.withColumn(info_dag.TARGET_ETL_DATE, lit(int(str_current_date)))
    # new_df_source =  df_source.withColumn(info_dag.TARGET_ETL_DATE, df_source["EXEC_TS"])
    # new_df_source.show()
    # Transform data with config from dag_config via pair (TARGET_TRANSFORM_COL & TARGET_TRANSFORM_EXPR)
    # When you need excute sql query be like "CASE WHEN", please use this
    if hasattr(info_dag, 'TARGET_TRANSFORM_EXPR') and info_dag.TARGET_TRANSFORM_EXPR:
        for i,j in zip(info_dag.TARGET_TRANSFORM_COL, info_dag.TARGET_TRANSFORM_EXPR):
            # Excute transform data by list sql 
            new_df_stg = new_df_stg.withColumn(i, expr(j.replace("*", "'")))
    # new_df_source.show()

    # Select col by information from dag_config
    new_df_stg = new_df_stg.select(info_dag.TARGET_ALL_COL)
    # print("Check Data !!")
    # new_df_source.show()
    # Write dataframe to summary table with mode_write = append
    init_spark.write_db(df=new_df_stg, db_config=dst_config, dbtable=dbtable, mode_write='append')
        