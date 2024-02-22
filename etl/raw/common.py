import pytz
from datetime import datetime
from etl.utils.iceberg import Iceberg
from etl.utils.dataframe import get_param
from etl.utils.datatype import string2query, date2str, str2date

     
def base_job(init_spark:object, info_dag:object, src_config:dict, param_config:dict, etl_date:str, parallel:bool):
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

     # hard code
     date_obj = str2date(str_current_date)
     formatted_date = date2str(date_obj, format="%d-%b-%y").upper()
     ic = Iceberg(init_spark.session, catalog=info_dag.ICEBERG_CATOLOG)
     
     # Create table name
     iceberg_tb = "%s.%s"%(info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
     ic.create_table(table=iceberg_tb, cols=info_dag.TARGET_ALL_COL, dtypes=info_dag.TARGET_ALL_DTYPE)
     # 
     init_spark.session.sql("CALL system.expire_snapshots(table => '%s', retain_last => %s)"%(iceberg_tb, str(info_dag.ICEBERG_NUM_SNAP)))
     
     # hard code
     if hasattr(info_dag, 'SOURCE_CONDITION') and info_dag.SOURCE_CONDITION:
          condition = """ WHERE %s = '%s' """%(info_dag.SOURCE_CONDITION[0], formatted_date)
          query_src = string2query(src_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE, where=condition)
          query_raw = " SELECT * FROM %s %s"%(iceberg_tb, condition)     
          df_raw = ic.read_table(sql=query_raw)
     else:
          query_src = string2query(src_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
          df_raw = ic.read_table(table=iceberg_tb)
     # query_src = string2query(src_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
     print("QUERY: ", query_src)
     df_source = init_spark.read_db(src_config, query_src)

     # 
     if df_raw.count() > 0:
          if df_raw.subtract(df_source).count() > 0:
               ic.write_table(df_source, table=iceberg_tb, exists_table=True, mode="overwrite")
          else:
               pass
     else:
          ic.write_table(df_source, table=iceberg_tb, exists_table=True, mode="append")
     
     #
     list_tbl = init_spark.session.sql("SHOW TBLPROPERTIES %s"%(iceberg_tb)).select("key").rdd.flatMap(lambda x: x).collect()
     ic.set_tbl(table=iceberg_tb, list_tbl=list_tbl, key='history.expire.max-snapshot-age-ms', value=str(int(info_dag.ICEBERG_SNAP_AGE)*24*60*60*1000))
     ic.set_tbl(table=iceberg_tb, list_tbl=list_tbl, key='history.expire.min-snapshots-to-keep', value=str(info_dag.ICEBERG_NUM_SNAP))
     ic.set_tbl(table=iceberg_tb, list_tbl=list_tbl, key='write.metadata.delete-after-commit.enabled', value=str(info_dag.ICEBERG_METADATA_DEL))
     ic.set_tbl(table=iceberg_tb, list_tbl=list_tbl, key='write.metadata.previous-versions-max', value=str(info_dag.ICEBERG_NUM_METADATA))