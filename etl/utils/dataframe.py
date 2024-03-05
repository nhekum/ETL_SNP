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

def enrich_data(df_src:object, df_enr:object, n_df_src:str, n_df_enr:str, condition:str, 
                get_col_enr:list, get_col_src:list=[], mode:str="left"):
    """
        df_src: a dataframe that need enriching data
        df_enr: a dataframe that used to supplement data
        n_df_src: name of source dataframe alias when join
        n_df_enr: name of enrich dataframe alias when join
        condition: that we need to join dataframe
        get_col_src: name of column, we use select in source dataframe after join dataframe
        get_col_enr: name of column, we use select in enrich dataframe after join dataframe
        mode: type of join
    """
    # Select all column or specific column in source dataframe
    if get_col_src:
        list_get = ["%s.%s"%(n_df_src, i) for i in get_col_src]
    else:
        list_get = ["%s.*"%(n_df_src)]
    
    for i in get_col_enr:
        # Select specific column in enrich dataframe
        list_get.append("%s.%s_%s"%(n_df_enr, i, n_df_enr))
        # Rename column with specific tail in enrich dataframe
        df_enr = df_enr.withColumnRenamed(i, "%s_%s"%(i, n_df_enr))
        # Rename column in condition
        if "df_%s[\"%s\"]"%(n_df_enr, i) in condition:
            condition = condition.replace("df_%s[\"%s\"]"%(n_df_enr, i), "df_%s[\"%s_%s\"]"%(n_df_enr, i, n_df_enr))
    
    # Rename dataframe in condition
    condition = condition.replace("df_%s"%(n_df_src), "df_src").replace("df_%s"%(n_df_enr), "df_enr")
    print(condition)
    print(list_get)
    # Join 2 dataframe with condition and only select requirement column
    df_src = df_src.alias(n_df_src).join(df_enr.alias(n_df_enr), eval(condition), mode).select(list_get)
    return df_src

def split_table(df_dim, df_smy, id_key, key_col_dim, key_col_smy, scd_type:str="1"):
    get_col_dim = list(dict(df_dim.dtypes).keys())
    get_col_smy = list(dict(df_smy.dtypes).keys())
    coalesce_src=['smy.%s_smy'%(col) for col in get_col_smy] + id_key
    id_key = id_key[0]
    
    cond = ["df_dim[\"%s\"] == df_smy[\"%s\"]"%(col1, col2) for col1, col2 in zip(key_col_dim, key_col_smy)]
    
    joined_df = enrich_data(df_src=df_dim, df_enr=df_smy, n_df_src="dim", n_df_enr="smy", 
                            condition="[" + ",".join(cond) + "]", get_col_enr=get_col_smy, mode="full")
    
    print("------INSERTED------")
    # Lọc lấy ra các bản ghi thêm mới ở bảng smy bằng cách lấy bản ghi có id là null ở bảng join sau 
    # khi join 2 bảng dim và smy với nhau
    inserted_df = joined_df.filter(joined_df[key_col_dim[0]].isNull()).selectExpr(coalesce_src).drop(id_key)
    # Lọc bỏ đi các bản ghi mới ở bảng smy có mã là null
    for i in get_col_smy:
        inserted_df = inserted_df.withColumnRenamed("%s_smy"%(i), i)
    inserted_df.show()

    if scd_type == "1":
        print("------UPDATED------")
        # Tạo bảng source mới có id col của bảng sink, đồng thời chỉ lấy các bản ghi còn hiệu lực ở bảng sink
        joined_df = joined_df.selectExpr(coalesce_src)
        for i in get_col_smy:
            joined_df = joined_df.withColumnRenamed("%s_smy"%(i), i)
        # Sắp xếp thứ cột bảng join đồng nhất với bảng dim
        df_dim = df_dim.select(list(dict(joined_df.dtypes).keys()))
        # Subtract bảng source với bảng sink, sau đó lọc các bỏ đi các bản ghi có ID_KEY là null
        updated_df = joined_df.subtract(df_dim).filter(~joined_df[id_key].isNull())
        # Lọc bản ghi trong bản source mà KEY là null 
        updated_df = updated_df.filter(~updated_df['%s'%(key_col_smy[0])].isNull())
        updated_df.show()
        return inserted_df, updated_df
    else:
        print("------DELETED------")
        # Lọc lấy ra các bản ghi bị xóa ở bảng source bằng cách lấy bản ghi có mã là null ở bảng source sau 
        # khi join 2 bảng source và sink với nhau
        deleted_df = joined_df.filter(joined_df[key_col_dim[0]+"_smy"].isNull())
        deleted_df = deleted_df.select(get_col_dim)
        deleted_df.show()

        print("------UPDATED------")
        # Tạo bảng source mới có id col của bảng sink, đồng thời chỉ lấy các bản ghi còn hiệu lực ở bảng sink
        joined_df = joined_df.selectExpr(coalesce_src)
        for i in get_col_smy:
            joined_df = joined_df.withColumnRenamed("%s_smy"%(i), i)
        # Sắp xếp thứ cột bảng sink đồng nhất với bảng source
        df_dim = df_dim.select(list(dict(joined_df.dtypes).keys()))
        # Subtract bảng source với bảng sink, sau đó lọc các bỏ đi các bản ghi có ID_KEY là null
        updated_df = joined_df.subtract(df_dim).filter(~joined_df[id_key].isNull())
        # Lọc bản ghi trong bản source mà KEY là null 
        updated_df = updated_df.filter(~updated_df['%s'%(key_col_smy[0])].isNull())
        updated_df.show()
        return inserted_df, deleted_df, updated_df

def concate_namecol_datetimecase(row, ignore_key=''):
    set_sql = ""
    for idx in row:
        if idx == row[ignore_key]:
            pass
        else:
            if isinstance(idx, str):
                if "to_date" in idx :
                    string_sql = idx
                else:
                    string_sql = "\'%s\'" % (str(idx))
            else:
                string_sql = "\'%s\'" % (str(idx))
            set_sql += string_sql + ','
        
    set_sql = set_sql[:-1].replace("\'None\'", "NULL")
    return set_sql

def gen_scd_sql(row, id_key, all_col_dict, date_col, target_schema, target_table, scd_type, insert=False):
    id_key = id_key[0]
    if scd_type == "1":
        set_sql = ""
        for idx in all_col_dict:
            if idx.upper() in date_col:
                set_sql = set_sql + "\"" + idx.upper()+"\""+" =" + row[idx]
                set_sql += ','
            elif idx.upper() in id_key:
                pass
            else:
                if "decimal" in all_col_dict[idx]:
                    set_sql = set_sql + "\"" + idx.upper()+"\""+" =\'" + str(row[idx])+'\''
                else:
                    set_sql = set_sql + "\"" + idx.upper()+"\""+" =\'" + row[idx]+'\''
                set_sql += ','
        set_sql = "SET "+set_sql[:-1]
    else:
        set_sql = """SET "TRANG_THAI_BG" ='C',"NGAY_HH_BG"= TO_DATE(TO_CHAR(CURRENT_DATE, 'DD-MM-YYYY'), 'DD-MM-YYYY')"""
    
    if insert:
        all_col_list = []
        for key, _ in all_col_dict.items():
            if key != id_key:
                all_col_list.append(key)
        set_sql_in = concate_namecol_datetimecase(row, ignore_key=id_key)
        sql = """INSERT INTO "{}"."{}" 
                ({},"NGAY_HL_BG","NGAY_HH_BG") 
                VALUES ({}, TO_DATE(TO_CHAR(CURRENT_DATE, 'DD-MM-YYYY'), 'DD-MM-YYYY'), TO_DATE('2099-01-01','YYYY/MM/DD'));
                """.format(target_schema, target_table,
                    str(all_col_list).replace("[", "").replace("]", "").replace("\'", "\""),
                    set_sql_in)
    else:
        sql = """ UPDATE "{}"."{}" """.format(target_schema, target_table)
        where = """ WHERE "{}" = '{}';
                    """.format(id_key.replace("[", "").replace("]", "").replace("\'", "\"").replace("\"", ""), row[id_key])
        sql = sql+set_sql+where
    
    return sql
