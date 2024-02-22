import pytz
from datetime import datetime
from etl.utils.iceberg import Iceberg
from etl.utils.dataframe import get_param
from etl.utils.datatype import string2query, date2str, str2date
from etl.utils.dataframe import trim_col, transform_datatype

def base_job(init_spark:object, info_dag:object, dst_config:dict, param_config:dict, etl_date:str, parallel:bool):
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

    # Create target table name
    dbtable = """ "%s"."%s" """%(info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
    iceberg_tb = "%s.%s"%(info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
    
    # hard code
    date_obj = str2date(str_current_date)
    formatted_date = date2str(date_obj, format="%d-%b-%y").upper()
    ic = Iceberg(init_spark.session, catalog=info_dag.ICEBERG_CATOLOG)
    
    # hard code
    if hasattr(info_dag, 'SOURCE_CONDITION') and info_dag.SOURCE_CONDITION:
        condition = """ WHERE %s = '%s' """%(info_dag.SOURCE_CONDITION[0], formatted_date)
        query_raw = " SELECT * FROM %s %s"%(iceberg_tb, condition)    
        print("QUERY: ", query_raw) 
        df_raw = ic.read_table(sql=query_raw)
    else:
        df_raw = ic.read_table(table=iceberg_tb)
    
    df_raw.show()
    
    query_stg = string2query(dst_config["DB_TYPE"], info_dag.TARGET_ALL_COL, info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
    df_stg = init_spark.read_db(dst_config, query_stg)
    
    # Trim All String Columns
    df_raw = trim_col(df_raw)
    
    # Cast data type of raw data
    df_raw = transform_datatype(df_source=df_raw, df_target=df_stg)

    # Select require cols
    df_raw = df_raw.select(info_dag.TARGET_ALL_COL)
    print("Check data !!")
    # df_raw.show()

    # Mode write: overwrite, append ,ignore 
    init_spark.write_db(df=df_raw, db_config=dst_config, dbtable=dbtable, mode_write="overwrite") 