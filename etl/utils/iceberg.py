#!/usr/bin/env python3  
# -*- coding: utf-8 -*- 
#----------------------------------------------------------------------------
# Created By  : quyetnn
# Created Date: 02/01/2024
# version ='0.1'
# ---------------------------------------------------------------------------

import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


# refs table chứa thông tin thời gian tồn tại của các bản snapshot
# snapshots table chứa thông tin về id_snapshot, phương thức ghi, thời gian, vị trí manifest_list, summary về snapshot (thông tin tầng metadata)
# history table chứa thông tin các bản snapshot đã được commit
# khi commit sẽ update dữ liệu lên nhánh main 

class Iceberg:
    def __init__(self, session:SparkSession, catalog:str='hadoop'):
        self.catalog = catalog+"_catalog"
        self.session = session
        try:
            self.__check_session__()
        except Exception as error:
            raise Exception("Error 401: Spark config is valid. %s"%(error))
    
    def __check_session__(self):
        dict_config = self.session.sparkContext.getConf().getAll()
        spark_config = str(dict_config)
        assert 'iceberg-spark-runtime' in spark_config,  \
            "Spark driver don't contain iceberg-spark-runtime. Please create a spark session with iceberg-runtime jar"
        
        assert 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions' in spark_config, \
            "Spark Session did't use extension of Iceberg. Please set 'spark.sql.extensions' with require class of iceberg" 
        
        assert 'org.apache.iceberg.spark.SparkCatalog' in spark_config, \
            "Spark did't use catalog of Iceberg. Please set 'spark.sql.catalog_%s' with require class of iceberg"%(self.catalog)
        
        assert 'spark.sql.catalog.%s.type'%(self.catalog) in spark_config, \
                "Type of catalog wasn't set. Please set 'spark.sql.catalog.%s.type' first"%(self.catalog)
        
        assert 'spark.sql.catalog.%s.warehouse'%(self.catalog) in spark_config, \
            "Please set the %s storage"%(self.catalog)
        
        if "minio" in self.catalog:
            assert 'rest' in spark_config, \
                "Minio catalog need config rest protocol. Please set 'spark.sql.catalog.%s.type' is 'rest'"%(self.catalog)
            
            assert 'spark.sql.catalog.%s.uri'%(self.catalog) in spark_config, \
                "Icebreg rest server URI not defined. Please 'spark.sql.catalog.%s.type' with uri of rest"%(self.catalog)
            
            assert "org.apache.iceberg.aws.s3.S3FileIO" in spark_config, \
                "io-impl should be 'org.apache.iceberg.aws.s3.S3FileIO' for minio catalog. Please 'spark.sql.catalog.%s.io-impl' with this class"%(self.catalog)
            
            assert 'spark.sql.catalog.%s.s3.endpoint'%(self.catalog) in spark_config, \
                "Minio endpointnot defined. Please set minio endpoint."
    
    def find_snapid(self, table:str, date_str:str):
        try:
            cur_date = datetime.strptime(date_str, '%Y%m%d')
            next_date = cur_date + timedelta(1)
        except ValueError as e:
            print("Error:", e)
        df = self.excute_sql("SELECT * FROM %s.history WHERE (made_current_at >= '%s') and (made_current_at< '%s') ;"%(table, cur_date.strftime("%Y-%m-%d"), next_date.strftime("%Y-%m-%d")))
        list_snapid = df.sort("made_current_at", ascending=False).select("snapshot_id").rdd.flatMap(lambda x: x).collect()
        return list_snapid[0]
    
    def get_cur_snapinfo(self, table:str):
        self.excute_sql("SELECT * FROM %s.refs;"%(table)).show()
        
    def set_cur_snapshot(self, table:str, snapshot_id:str):
        self.excute_sql("CALL system.set_current_snapshot('%s', %s);"%(table, snapshot_id))
    
    def excute_sql(self, query:str):
        try:
            return self.session.sql(query)
        except Exception as error:
            raise Exception(error)
    
    def create_database(self, database:str):
        return self.excute_sql("CREATE DATABASE IF NOT EXISTS %s"%(database))
        
    def create_table(self, table:str, cols:list, dtypes:list, partition:str="", tblproperties:str=""):
        schema = """,""".join(list(map(lambda x,y: "%s %s"%(str(x),str(y)), cols, dtypes)))
        if partition:
            query = """ CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg PARTITIONED BY %s"""%(table, schema, partition)
        else:
            query = """ CREATE TABLE IF NOT EXISTS %s (%s) USING iceberg """%(table, schema)
        if tblproperties:
            query += tblproperties
        print("ICEBERG_TABLE: ", query)
        return self.excute_sql(query) 
    
    def set_tbl(self, table:str, list_tbl:list, key:str, value:str):
        if not key in list_tbl:
            self.excute_sql(""" ALTER TABLE %s SET TBLPROPERTIES ('%s'='%s') """%(table, key, value))
    
    def control_branch(self, table:str, branch:str, method:str="CREATE"):
        status = self.__check_wap__()
        if not status:
            self.excute_sql(""" ALTER TABLE %s SET TBLPROPERTIES ('write.wap.enabled'='true')) """%(table))
        return self.excute_sql(""" ALTER TABLE %s %s BRANCH %s """%(table, method, branch))       
    
    def control_tag(self, table:str, tag:str, method:str="CREATE", expire_time:str="31"):
        status = self.__check_wap__()
        if not status:
            self.excute_sql(""" ALTER TABLE %s SET TBLPROPERTIES ('write.wap.enabled'='true')) """%(table))
        if expire_time:
            return self.excute_sql(""" ALTER TABLE %s %s TAG %s RETAIN %s DAYS """%(table, method, tag, expire_time))
        else:
            return self.excute_sql(""" ALTER TABLE %s %s TAG %s """%(table, method, tag))
    
    def set_branch(self, branch:str):
        try:
            self.session.conf.set('spark.wap.branch', branch)
        except Exception as error:
            raise Exception(error)
    
    def merge_table(self):
        return 
        
    def expire_snapshot(self, table:str, snapshot_id:str, timestamp:str, num_snapshot:int=10):
        # assert not(snapshot_id and timestamp), "Only use snapshot_id or timestamp"
        if timestamp: 
            return self.excute_sql("CALL system.expire_snapshots('%s', TIMESTAMP '%s', %s)"%(table, timestamp, str(num_snapshot)))
        elif snapshot_id:
            # snapshot ID should not be the current snapshot
            return self.excute_sql("CALL system.expire_snapshots(table => '%s', snapshot_ids => ARRAY(%s))"%(table, snapshot_id))
        else:
            return self.excute_sql("CALL system.expire_snapshots('%s')"%(table))
    
    def rollback_snapshot(self, table:str, snapshot_id:str, timestamp:str):
        assert not(snapshot_id and timestamp), "Only use snapshot_id or timestamp"
        if timestamp: 
            return self.excute_sql("CALL system.rollback_to_timestamp('%s', TIMESTAMP '%s')"%(table, timestamp))
        if snapshot_id:
            # snapshot ID should not be the current snapshot
            return self.excute_sql("CALL system.rollback_to_snapshot('%s', %s)"%(table, snapshot_id))
    
    def publish_snapshot(self, wap_snapshot_id:str):
        return self.excute_sql(f"CALL system.cherrypick_snapshot('nyc.permits', {wap_snapshot_id})")
    
    def read_table(self, table:str="", sql:str="", tag:str="", branch:str="main", 
                   snapshot_id:str="", timestamp:str=""):
        assert not(table and sql), "Use either table or sql"
        assert not(snapshot_id and tag and branch and timestamp), "Use either snapshot_id or (tag/branch/timestamp/sql)"
        if table:
            if snapshot_id:
                df = self.session.read \
                        .option("snapshot-id", int(snapshot_id)) \
                        .format("iceberg") \
                        .load(table) 
            elif tag:
                df = self.session.read \
                        .format("iceberg") \
                        .load(table+".tag_"+tag) 
            elif branch:
                df = self.session.read \
                        .format("iceberg") \
                        .load(table+".branch_"+branch) 
            else:
                df = self.session.read \
                        .option("as-of-timestamp", timestamp) \
                        .format("iceberg") \
                        .load(table)
        if sql:
            df = self.session.sql(sql)
        return df
    
    def write_table(self, df:DataFrame, table:str, mode:str="overwrite", 
                    write_format:str="parquet", exists_table:bool=True):
        if exists_table:
            print(1)
            if mode == 'append':
                df.writeTo(table).append()
            elif mode == 'overwrite':
                df.writeTo(table).overwritePartitions()
            else:
                df.writeTo("prod.db.table") \
                    .tableProperty("write.format.default", write_format) \
                    .createOrReplace() 
        else:
            print(2)
            df.write.mode(mode).format(write_format).saveAsTable(table)