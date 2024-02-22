from datetime import date
from pyspark.sql.functions import col, trim, when, to_date
from pyspark.sql.types import *


def trim_col(df_source:object):
    # Trim All String Columns
    for field in df_source.schema.fields:
        if(str(field.dataType) == "StringType()"):
            df_source = df_source.withColumn(field.name, trim(col(field.name)))
    return df_source

def transform_datatype(df_source:object, df_target:object):
    for field in df_target.schema.fields:
        if not (field.dataType == DateType()):
            df_source = df_source.withColumn(field.name, df_source[field.name].cast(field.dataType))
        else:
            df_source = df_source.withColumn(field.name, when(col(field.name) == "31-DEC-00", None).otherwise(col(field.name)))
            df_source = df_source.withColumn(field.name, to_date(col(field.name), "dd-MMM-yy"))
            df_source = df_source.withColumn(field.name, when(col(field.name).isNull(), date(1900, 12, 31)).otherwise(col(field.name)))
    return df_source
     
def get_param(init_spark:object, info_dag:object, param_config:dict):
    dwh_param = init_spark.read_db(param_config, f""" SELECT * FROM "{info_dag.PARAM_SCHEMA}"."{info_dag.PARAM_TABLE}" """)
    dwh_param = dwh_param.filter(dwh_param["MA_THAM_SO"].isin(info_dag.PARAM_CONDITION)).select(["MA_THAM_SO", "GIA_TRI"])
    dwh_param = dwh_param.withColumn("MA_THAM_SO", trim(col("MA_THAM_SO")))
    dwh_param = dwh_param.withColumn("GIA_TRI", trim(col("GIA_TRI")))
    dict_param = {}
    for param in dwh_param.collect():
        print(param.MA_THAM_SO, param.GIA_TRI)
        dict_param.update({param.MA_THAM_SO:param.GIA_TRI})
    return dict_param
     