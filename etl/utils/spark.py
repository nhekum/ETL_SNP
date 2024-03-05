import os
import pyspark
from threading import Thread
from pyspark.sql import SparkSession
from etl.utils.datatype import json2object, string2query
# from airflow.providers.postgres.hooks.postgres import PostgresHook

class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)
            
    def join(self, *args):
        Thread.join(self, *args)
        return self._return

class Spark():
    def __init__(self, spark_config:dict):
        # Set enviroment variable
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--master %s pyspark-shell"%("local[*]")
        # Create Spark Config from dict information of spark
        self.spark_config = pyspark.SparkConf().setAll(spark_config.items())
        # initialization SparkSession
        self.session = self.create_session()

    # Rewrite  database config for Spark
    def set_db_config(self, db_config:dict):
        # Create object from dict
        info_database = json2object(db_config)
        self.user = info_database.DB_USER
        self.password = info_database.DB_PASSWORD
        self.format = info_database.DB_FORMAT
        self.db_type = info_database.DB_TYPE
        self.url = info_database.DB_URL
        # Check config contain information of database's driver
        try:
            self.driver = info_database.DB_DRIVER
        except:
            self.driver = None

    # Define create spark session
    def create_session(self, **args):
        """
            parameters:
                - **args : là dict chứa config cho spark config
                    Ex: {'spark.master', "local[*]",
                         'spark.hadoop.fs.defaultFS', "hdfs://namenode:9000"}
            output:
                - Sparksession của pyspark
        """
        for i,j in zip(args.keys(), args.values()):
            self.spark_config.set(i, j)

        return SparkSession.builder.config(conf=self.spark_config).getOrCreate()
    
    # Define read data from database with spark session
    def read_db(self, db_config, query=""):
        """
            parameters:
                - query : là string câu lệnh sql 
                - db_config : là thong so ket noi toi database
            output:
                - dataframe cuả pyspark
        """
        # print(db_config, "---", query)
        format_read = db_config["DB_FORMAT"]
        if "jdbc" in format_read:
            self.set_db_config(db_config)
            try:
                if self.driver :
                    print("Have driver", query)
                    df = self.session.read.format(format_read) \
                                    .option("url", self.url) \
                                    .option("query", query) \
                                    .option("user", self.user) \
                                    .option("password", self.password) \
                                    .option("driver", self.driver)\
                                    .load()
                else:
                    print("Don't have driver")
                    df = self.session.read.format(format_read) \
                                    .option("url", self.url) \
                                    .option("query", query) \
                                    .option("user", self.user) \
                                    .option("password", self.password) \
                                    .load()
            except Exception as error:
                print(error)
                raise Exception("Read failed, check query or path. %s"%(error))
        else:
            path_file = query
            try:
                if "parquet" in format_read:
                    try:
                        df = self.session.read.parquet(path_file)
                    except:
                        df = None
                else:
                    # print("self.session.read.%s(\"%s\", header=True, inferSchema=True)"%(format_read, path_file))
                    df = eval("self.session.read.%s(\"%s\")"%(format_read, path_file))     
            except Exception as error:
                raise Exception("Read failed, check format_read or path_file. %s"%(error))
        return df
    
    # Define write data to database with spark session
    def write_db(self, df, db_config, dbtable, mode_write):
        """
            parameters:
                df : là dataframe bằng pyspark
                db_config : là thong so ket noi toi database
                dbtable : là tên bảng trong database
                format_write : định dạng muốn lưu lên database
                mode_write : là lựa chọn write khi có dữ liệu trùng trên pyspark, Ex: append, overwrite
        """
        # print(db_config)
        try:
            format_write = db_config["DB_FORMAT"]
            if "jdbc" in format_write:
                self.set_db_config(db_config)
                df.repartition(10).write.format(format_write) \
                            .option("url", self.url) \
                            .option("dbtable", dbtable) \
                            .option("user", self.user) \
                            .option("password", self.password) \
                            .option("driver", self.driver)\
                            .option("truncate", "true") \
                            .mode(mode_write).save()
            else:
                path_file = dbtable
                try:
                    df.repartition(10).write.format(format_write).mode(mode_write).save(path_file)
                except Exception as error:
                    raise Exception("Write failed, check format_read or path_file. %s"%(error))
        except Exception as error:
            raise Exception("Error: %s"%(error))

# def check_sequence_log(init_spark:object, start_time:str, schema:str, table:str):
#     sql_query="""SELECT "STATUS_JOB" FROM "{}"."{}"  WHERE "TIME_START" > '{}'""".format(schema, table, start_time)
#     # query=""" SELECT "STATUS_JOB" FROM "DWH_LOG"."STG_LOG" WHERE "TIME_START" >= '2023-05-07 19:34:00.000' """
#     df = init_spark.read_db(sql_query)
#     list_status_job = df.rdd.map(lambda x: x.STATUS_JOB).collect()
#     if 'F' in list_status_job:
#         success = False
#     elif '' in list_status_job:
#         return check_sequence_log(init_spark, start_time, schema, table)
#     else:
#         success = True
#     return success

# def get_list_config(folder_path:str):
#     list_config = glob.glob(os.path.join(folder_path, "*.json"))
#     return list_config

# def read_with_join(init_spark:object, info_dag:object, scd:object, db_config:dict):
#     query_stg = string2query(db_config["DB_URL"], info_dag.SOURCE_ALL_COL, info_dag.SOURCE_SCHEMA, info_dag.SOURCE_TABLE)
#     df_stg = init_spark.read_db(db_config, query_stg)
#     try:
#         df_stg = scd.multi_join(mapping_list=info_dag.JOIN_COL_LIST, key_list=info_dag.JOIN_KEY_LIST, 
#                                 all_col=info_dag.TARGET_ALL_COL, df=df_stg, sess=init_spark, 
#                                 db_config=db_config, ignore_key=True)
#     except Exception as error:
#         print(error)
#         pass
    
#     return df_stg

# def send_alarm(chat_id:str, token:str, message:str):
#     url = f"https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}"
#     response = requests.get(url)
#     if response.ok:
#         pass
#     else:
#         raise Exception("Failed to send alarm")
