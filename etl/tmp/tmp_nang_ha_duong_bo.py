import pytz
from datetime import datetime
from pyspark.sql.window import Window
from etl.utils.dataframe import get_param
from etl.utils.datatype import remove_symbol, json2object, string2query
from pyspark.sql.functions import lit, when, substring, row_number, concat
# from pyspark.sql import functions as F

def tmp_nang_ha_duong_bo(init_spark:object, dag_config:dict, stg_config:dict, 
                tmp_config:dict, param_config:dict, hook:object, parallel:bool=False, etl_date:str=""):
    tz = pytz.timezone('Asia/Bangkok')
    info_dag = json2object(dag_config)
    time_start = datetime.now(tz)
    info_dag.AIRFLOW_NAME = info_dag.AIRFLOW_NAME + "_" + time_start.strftime("%Y%m%d%H%M%S")
    # Insert log to SMY_ETL_LOG with status = queue
    # hook.insert_log(info_dag, time_start, time_start, status_job='Q', status_finish='0', error_mess='', job_type="PARALLEL")
    
    try:
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
        
        dbtable = "\"%s\".\"%s\""%(info_dag.TARGET_SCHEMA, info_dag.TARGET_TABLE)
        query_smy = string2query(stg_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
        # SMY_CHE_PRODUCTIVITY_DETAILS
        df_prod_ori = init_spark.read_db(stg_config, query_smy)
        # df_product = df_product.filter((df_product["VES_TYPE"] != "F") & 
        #                                 (df_smy["CAT_TYPE"] != "INTERNAL") & 
        #                                 (df_smy["DOT"] == int(dot)))
        print(df_prod_ori.count())
        # SMY_PREGATE_TRANSACT
        df_pregate = init_spark.read_db(stg_config, """ SELECT "ITEM_KEY", "R_D", "OPERATION_METHOD", "TRANSFER_REASON_CD", "PLACE_OF_DELIVERY", "RECEIVAL_PLACE"
                                                         FROM "%s"."%s" """%("DWH_STG","STG_PREGATE_TRANSACT"))
        df_pregate = df_pregate.withColumnRenamed("OPERATION_METHOD", "OPERATION_METHOD_pregate")\
                            .withColumnRenamed("TRANSFER_REASON_CD", "TRANSFER_REASON_CD_pregate")\
                            .withColumnRenamed("PLACE_OF_DELIVERY", "PLACE_OF_DELIVERY_pregate")
                            
        df_prod = df_prod_ori.alias('A').join(df_pregate.alias('B'), [df_prod_ori["ITEM_KEY"] == df_pregate["ITEM_KEY"],
                                                                    ((df_pregate["R_D"] == 'R') & (df_prod_ori["MOVE_TYPE"] == 'LIFTOFF') |
                                                                     (df_pregate["R_D"] == 'D') & (df_prod_ori["MOVE_TYPE"] == 'LIFTON') )
                                                                    ],"inner").select(["A.*", "B.OPERATION_METHOD_pregate", "B.TRANSFER_REASON_CD_pregate",
                                                                                       "B.PLACE_OF_DELIVERY_pregate", "B.RECEIVAL_PLACE"])
        
        df_oper = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_OPERATION_METHOD"))
        df_oper = df_oper.withColumnRenamed("OPERATION_METHOD", "OPERATION_METHOD_oper") \
                        .withColumnRenamed("OPERATION_NAME", "OPERATION_NAME_oper") 
        df_prod_1 = df_prod.alias('A').join(df_oper.alias('B'), [df_prod["OPERATION_METHOD_pregate"] == df_oper["OPERATION_METHOD_oper"],
                                                                 df_oper["TRANFER_LINK_DEPOT"] == 'Y'
                                                               ],"inner").select(["A.*", "B.OPERATION_METHOD_oper", "B.OPERATION_NAME_oper"])

        df_charge = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_CHARGE_CODES"))
        df_prod = df_prod_1.alias('A').join(df_charge.alias('B'), [df_prod_1["OPERATION_METHOD_pregate"] == df_charge["CHARGE_CD"],
                                                                    df_charge["TRANSPORT_TYPE"] != 'B'
                                                                    ],"inner").select(["A.*"])
        
        df_company = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_CHE_COMPANY"))
        df_company_tmp = df_company.withColumnRenamed("CHE_ID", "CHE_ID_m")\
                                .withColumnRenamed("INTERNAL_CHE_ID", "INTERNAL_CHE_ID_m") \
                                .withColumnRenamed("COMPANY", "COMPANY_m") \
                                .withColumnRenamed("MANAGE_BY", "MANAGE_BY_m")
        df_prod = df_prod.alias("A").join(df_company_tmp.alias("B"), [df_company_tmp["CHE_ID_m"] != " ",
                                                                df_prod["CHE_ID"] == df_company_tmp["CHE_ID_m"], 
                                                                df_prod["EXEC_TS"] >= df_company_tmp["AS_AT_TS"],
                                                                df_prod["EXEC_TS"] <= df_company_tmp["EXPIRY_TS"]
                                                                ], "leftouter").select(["A.*", "B.CHE_ID_m", "B.INTERNAL_CHE_ID_m",
                                                                                        "B.COMPANY_m", "B.MANAGE_BY_m"])
        
        df_yard = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_YARD_AREA"))
        df_yard_tmp = df_yard.withColumnRenamed("AREA", "AREA_area") \
                            .withColumnRenamed("SCAN_AREA", "SCAN_AREA_area") \
                            .withColumnRenamed("MANAGE_BY", "MANAGE_BY_area") \
                            .withColumnRenamed("FUMIGATE_AREA", "FUMIGATE_AREA_area") \
                            .withColumnRenamed("IS_VIRTUAL_HEAP_STACK", "IS_VIRTUAL_HEAP_STACK_area") 
        df_prod = df_prod.alias("A").join(df_yard_tmp.alias("B"), [df_prod["MOVE_TYPE"] == 'LIFTON',
                                                                df_prod["STACK"] == df_yard_tmp["STACK"], 
                                                                df_prod["EXEC_TS"] >= df_yard_tmp["FR_DATE"],
                                                                df_prod["EXEC_TS"] <= df_yard_tmp["TO_DATE"]
                                                                ], "leftouter").select(["A.*", "B.AREA_area", "B.SCAN_AREA_area", "B.FUMIGATE_AREA_area",
                                                                                        "B.MANAGE_BY_area", "B.IS_VIRTUAL_HEAP_STACK_area"])

        df_yard_tmp = df_yard.withColumnRenamed("AREA", "AREA_arTo") \
                            .withColumnRenamed("SCAN_AREA", "SCAN_AREA_arTo") \
                            .withColumnRenamed("MANAGE_BY", "MANAGE_BY_arTo") \
                            .withColumnRenamed("FUMIGATE_AREA", "FUMIGATE_AREA_arTo") \
                            .withColumnRenamed("IS_VIRTUAL_HEAP_STACK", "IS_VIRTUAL_HEAP_STACK_arTo") 
        df_prod = df_prod.alias("A").join(df_yard_tmp.alias("B"), [df_prod["MOVE_TYPE"] == 'LIFTOFF',
                                                                df_prod["TO_STACK"] == df_yard_tmp["STACK"], 
                                                                df_prod["EXEC_TS"] >= df_yard_tmp["FR_DATE"],
                                                                df_prod["EXEC_TS"] <= df_yard_tmp["TO_DATE"]
                                                                ], "leftouter").select(["A.*", "B.AREA_arTo", "B.SCAN_AREA_arTo", "B.FUMIGATE_AREA_arTo",
                                                                                        "B.MANAGE_BY_arTo", "B.IS_VIRTUAL_HEAP_STACK_arTo"])
        
        df_item = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_ITEM_OOG"))
        df_item = df_item.withColumnRenamed("ITEM_KEY", "ITEM_KEY_item")
        df_prod = df_prod.alias('A').join(df_item.alias('B'), [df_prod["ITEM_KEY"] == df_item["ITEM_KEY_item"]], "leftouter").select(["A.*", "B.ITEM_KEY_item"])

        df_sys = init_spark.read_db(stg_config, """ SELECT * FROM "%s"."%s" """%("DWH_STG","STG_SYS_CODES"))
        df_sys = df_sys.withColumnRenamed("DESCR", "DESCR_sys")
        df_prod = df_prod.alias('A').join(df_sys.alias('B'), [df_sys["CODE_TP"] == 'CHUYEN_CANG',
                                                                df_prod["TRANSFER_REASON_CD_pregate"] == df_sys["CODE_REF"]], "leftouter").select(["A.*", "B.DESCR_sys"])
        df_danger = init_spark.read_db(stg_config, """ SELECT "ITEM_KEY", "CHARGE_FLG" FROM "%s"."%s" """%("DWH_STG","STG_ITEM_DANGEROUS"))
        df_danger = df_danger.withColumnRenamed("ITEM_KEY", "ITEM_KEY_danger")
        df_prod = df_prod.alias('A').join(df_danger.alias('B'), [df_danger["CHARGE_FLG"] == 'Y',
                                                                df_prod["ITEM_KEY"] == df_danger["ITEM_KEY_danger"]], "left").select(["A.*", "B.ITEM_KEY_danger"])
        
        df_prod = df_prod.filter((df_prod["CAT_TYPE"] == 'YARD') &
                                 (~df_prod["CHE_ID"].isin(['???', 'TOP', 'A00', ' '])) &
                                 (df_prod["MOVE_TYPE"].isin(['LIFTON', 'LIFTOFF'])) &
                                 ((df_prod["MOVE_TYPE"] != 'TRANS') | (df_prod["CAT_TYPE"] != 'INTERNAL') | 
                                  (df_prod["SCAN_AREA_area"].isNull()) | (df_prod["SCAN_AREA_area"] != 'Y') | (df_prod["IS_VIRTUAL_HEAP_STACK_area"] != 'Y')) &
                                 ((df_prod["MOVE_TYPE"] != 'LIFTOFF') | (df_prod["CAT_TYPE"] != 'VES') | 
                                  (df_prod["SCAN_AREA_arTo"].isNull()) | (df_prod["SCAN_AREA_arTo"] != 'Y') | (df_prod["IS_VIRTUAL_HEAP_STACK_arTo"] != 'Y')) &
                                 ((df_prod["MOVE_TYPE"] != 'LIFTOFF') | (df_prod["CAT_TYPE"] != 'INTERNAL') | 
                                  (df_prod["SCAN_AREA_arTo"].isNull()) | (df_prod["SCAN_AREA_arTo"] != 'Y') | (df_prod["IS_VIRTUAL_HEAP_STACK_arTo"] != 'Y')) &
                                 ((df_prod["MOVE_TYPE"] != 'LIFTON') | (df_prod["CAT_TYPE"] != 'INTERNAL') | 
                                  (df_prod["SCAN_AREA_area"].isNull()) | (df_prod["SCAN_AREA_area"] != 'Y') | (df_prod["IS_VIRTUAL_HEAP_STACK_area"] != 'Y'))
                                 )

        df_prod = df_prod.withColumn("CHE_ID", when(df_prod["INTERNAL_CHE_ID_m"].isNotNull(), df_prod["INTERNAL_CHE_ID_m"]).otherwise(" "))\
                        .withColumn("YARD_AREA_FR", when(df_prod["AREA_area"].isNotNull(), df_prod["AREA_area"]).otherwise(" ")) \
                        .withColumn("YARD_AREA_TO", when(df_prod["AREA_arTo"].isNotNull(), df_prod["AREA_arTo"]).otherwise(" ")) \
                        .withColumn("FROM_AREA_REPORT", when(df_prod["AREA_area"].isNotNull(), df_prod["AREA_area"]).otherwise(" ")) \
                        .withColumn("TO_AREA_REPORT", when(df_prod["AREA_arTo"].isNotNull(), df_prod["AREA_arTo"]).otherwise(" ")) \
                        .withColumn("FROM_MANAGE_REPORT", when(df_prod["MANAGE_BY_area"].isNotNull(), df_prod["MANAGE_BY_area"]).otherwise(" ")) \
                        .withColumn("TO_MANAGE_REPORT", when(df_prod["MANAGE_BY_arTo"].isNotNull(), df_prod["MANAGE_BY_arTo"]).otherwise(" ")) \
                        .withColumn("IS_REEFER", when((substring(df_prod["ISO"], 3, 1)).isin(['3', '4', 'R']), 'Y').otherwise(" ")) \
                        .withColumn("IS_OOG", when(df_prod["ITEM_KEY_item"].isNull(), ' ').otherwise("Y")) \
                        .withColumn("IS_DANGEROUS", when(df_prod["ITEM_KEY_danger"].isNull(), ' ').otherwise("Y")) \
                        .withColumn("COMPANY", when(df_prod["COMPANY_m"].isNotNull(), df_prod["COMPANY_m"]).otherwise(" ")) \
                        .withColumn("MANAGEBY", when(df_prod["MANAGE_BY_m"].isNotNull(), df_prod["MANAGE_BY_m"]).otherwise(" ")) \
                        .withColumn("PLACE_OF_DELIVERY", when(df_prod["IN_OUT"] == 'LIFTON', df_prod["PLACE_OF_DELIVERY_pregate"]).otherwise("CTL")) \
                        .withColumn("PLACE_OF_RECEIPT", when(df_prod["IN_OUT"] == 'LIFTOFF', df_prod["RECEIVAL_PLACE"]).otherwise("CTL")) \
                        .withColumn("EXEC_TS", when(df_prod["IN_OUT"] == 'LIFTON', df_prod["DEP_TS"]).otherwise(df_prod["ARR_TS"])) \
                        .withColumn("TMP_1", when(df_prod["IN_OUT"] == 'IN', "Hạ từ depot ").otherwise("Chuyển depot ")) \
                        .withColumn("TMP_2", when(df_prod["DESCR_sys"].isNotNull(), df_prod["DESCR_sys"]).otherwise(df_prod["OPERATION_NAME_oper"])) \
                        .withColumn("TMP_3", when(((df_prod["IS_VIRTUAL_HEAP_STACK_area"] == 'Y') & (df_prod["SCAN_AREA_area"] == 'Y')), " Soi")
                                                    .when(((df_prod["IS_VIRTUAL_HEAP_STACK_area"] == 'Y') & (df_prod["FUMIGATE_AREA_area"] == 'Y')), " Khử trùng")
                                                    .when(((df_prod["IS_VIRTUAL_HEAP_STACK_arTo"] == 'Y') & (df_prod["SCAN_AREA_arTo"] == 'Y')), " Soi")
                                                    .when(((df_prod["IS_VIRTUAL_HEAP_STACK_arTo"] == 'Y') & (df_prod["FUMIGATE_AREA_arTo"] == 'Y')), " Khử trùng")
                                                    .otherwise(" ")) \
                        .withColumn("IS_SCAN", when(((df_prod["IS_VIRTUAL_HEAP_STACK_area"] == 'Y') & (df_prod["SCAN_AREA_area"] == 'Y')), "Y")
                                                .when(((df_prod["IS_VIRTUAL_HEAP_STACK_arTo"] == 'Y') & (df_prod["SCAN_AREA_arTo"] == 'Y')), "Y")
                                                .otherwise(" ")) 
        df_prod = df_prod.withColumn("OPERATION_NAME", concat(df_prod["TMP_1"], df_prod["TMP_2"], df_prod["TMP_3"]))\
                        .withColumn("IS_VIRTUAL_SCAN", lit(" ")) \
                        .withColumn("REPORT_MOVE_TYPE", df_prod["MOVE_TYPE"])\
                        .withColumn("OLD_EXEC_TS", df_prod["EXEC_TS"])
        
        window_spec = Window.orderBy("ITEM_KEY")
        df_prod = df_prod.withColumn("ROW_ID", row_number().over(window_spec))

        df_prod = df_prod.select(info_dag.TARGET_ALL_COL)
        df_prod = df_prod.withColumn(info_dag.TARGET_ETL_DATE, lit(int(str_current_date)))
        print(df_prod.count())
        df_prod.show()
        init_spark.write_db(df=df_prod, db_config=tmp_config, dbtable=dbtable, mode_write='overwrite')
        
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
        # Raise exception to stop job in airflow
        if parallel:
            # return info_dag.AIRFLOW_NAME + ": " + error_message
            pass
        else:
            raise Exception("Error run job")