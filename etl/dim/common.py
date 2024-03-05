import pytz
from datetime import datetime
from etl.utils.iceberg import Iceberg
from etl.utils.dataframe import get_param, split_table, gen_scd_sql
from etl.utils.datatype import string2query
from pyspark.sql.functions import lit, current_date, to_date, col
     
def base_job(init_spark:object, info_dag:object, src_config:dict, dst_config:dict, 
             param_config:dict, hook:object, etl_date:str, parallel:bool):
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
    query_smy = string2query(src_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE, 
                            """ WHERE \"%s\" = '%s' """%(info_dag.SOURCE_ETL_DATE, str_current_date))
    print(query_smy)
    df_smy = init_spark.read_db(src_config, query_smy)
    
    query_dim = string2query(dst_config["DB_URL"], info_dag.TARGET_ALL_COL, info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
    df_dim = init_spark.read_db(src_config, query_dim)
    filter_keycol_smy=[]
    filter_keycol_dim=[]
    for i,j in zip(info_dag.SOURCE_JOIN_KEY, info_dag.TAGRET_JOIN_KEY):
        filter_keycol_smy.append("col(\"%s\").isNotNull()"%(i))
        filter_keycol_dim.append("col(\"%s\").isNotNull()"%(j))
    
    df_dim = df_dim.filter(eval("&".join(filter_keycol_dim)))
    df_smy = df_smy.filter(eval("&".join(filter_keycol_smy)))
    if hasattr(info_dag, 'TARGET_DROP_DUPLICATE') and info_dag.TARGET_DROP_DUPLICATE.upper() == "TRUE":
        df_smy = df_smy.dropDuplicates()
    df_smy = df_smy.select(info_dag.SOURCE_ALL_COL)
    if hasattr(info_dag, 'SCD_TYPE'):
        if info_dag.SCD_TYPE == "2":
            df_dim = df_dim.filter(df_dim[info_dag.STATUS_RECORD_COL] == "A").select(info_dag.SOURCE_ALL_COL+info_dag.TARGET_ID_KEY)
        else:
            df_dim = df_dim.select(info_dag.SOURCE_ALL_COL+info_dag.TARGET_ID_KEY)

    # df_dim.show()
    # df_smy.show()
    
    if info_dag.SCD_TYPE == "1":
        inserted_df, updated_df = split_table(df_dim=df_dim, df_smy=df_smy, id_key=info_dag.TARGET_ID_KEY, 
                                                key_col_dim=info_dag.TAGRET_JOIN_KEY, key_col_smy=info_dag.SOURCE_JOIN_KEY, scd_type=info_dag.SCD_TYPE)
    else:
        inserted_df, deleted_df, updated_df = split_table(df_dim=df_dim, df_smy=df_smy, id_key=info_dag.TARGET_ID_KEY, 
                                                key_col_dim=info_dag.TAGRET_JOIN_KEY, key_col_smy=info_dag.SOURCE_JOIN_KEY, scd_type=info_dag.SCD_TYPE)
    
        inserted_df = inserted_df.withColumn("TRANG_THAI_BG", lit("A"))
        inserted_df = inserted_df.withColumn("NGAY_HL_BG", to_date(lit(str_current_date), "yyyyMMdd")) \
                                .withColumn("NGAY_HH_BG", to_date(lit("2099/01/01"), "yyyy/MM/dd"))
    
    init_spark.write_db(df=inserted_df, db_config=dst_config, dbtable=dbtable, mode_write='append')
    
    if info_dag.SCD_TYPE == "2":
        updated_df = updated_df.withColumn("TRANG_THAI_BG", lit("A"))
        for row in updated_df.rdd.toLocalIterator():
            #
            sql_udp = gen_scd_sql(row=row, id_key=info_dag.TARGET_ID_KEY, all_col_dict=dict(updated_df.dtypes), date_col=info_dag.TARGET_ETL_DATE, 
                            target_schema=info_dag.TARGET_SCHEMA, target_table=info_dag.TARGET_TABLE, scd_type=info_dag.SCD_TYPE)
    
            hook.execute(sql_udp)
            #
            sql_ins = gen_scd_sql(row=row, id_key=info_dag.TARGET_ID_KEY, all_col_dict=dict(updated_df.dtypes), date_col=info_dag.TARGET_ETL_DATE, 
                                target_schema=info_dag.TARGET_SCHEMA, target_table=info_dag.TARGET_TABLE, scd_type=info_dag.SCD_TYPE, insert=True)
            hook.execute(sql_ins)
            
        for row in deleted_df.rdd.toLocalIterator():
            sql_del = gen_scd_sql(row=row, id_key=info_dag.TARGET_ID_KEY, all_col_dict=dict(updated_df.dtypes), date_col=info_dag.TARGET_ETL_DATE, 
                                target_schema=info_dag.TARGET_SCHEMA, target_table=info_dag.TARGET_TABLE, scd_type=info_dag.SCD_TYPE)
            hook.execute(sql_del)
    else:
        for row in updated_df.rdd.toLocalIterator():
            sql_udp = gen_scd_sql(row=row, id_key=info_dag.TARGET_ID_KEY, all_col_dict=dict(updated_df.dtypes), date_col=info_dag.TARGET_ETL_DATE, 
                            target_schema=info_dag.TARGET_SCHEMA, target_table=info_dag.TARGET_TABLE, scd_type=info_dag.SCD_TYPE)
    
            hook.execute(sql_udp)
    