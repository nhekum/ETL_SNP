from sqlalchemy import create_engine
        
class Hook():
    def __init__(self, db_config):
        self.db_config = db_config
        
    def execute(self, sql_query):
        engine = create_engine("postgresql+psycopg2://%s:%s@%s"%(self.db_config["DB_USER"], self.db_config["DB_PASSWORD"], self.db_config["DB_URL"].replace("jdbc:postgresql://", "")))
        
        print("postgresql+psycopg2://%s:%s@%s"%(self.db_config["DB_USER"], self.db_config["DB_PASSWORD"], self.db_config["DB_URL"].replace("jdbc:postgresql://", "")))
        print(sql_query)
        try:
            with engine.begin() as conn:
                conn.execute(sql_query)
                # print("Success run sql query")
        except Exception as error:
            print(error)
            raise Exception("Fail run sql query. %s"%(error))
    
    def insert_log(self, info_dag, time_start, time_finish, status_job='', status_finish='0', error_mess='', job_type=''):
        sql_query = """
                    INSERT INTO "%s"."%s"
                    ("DAG_ID", "JOB_NAME", "TIME_START", "TIME_FINISH", "STATUS_JOB", "STATUS_FINISH", "ERROR_MESSAGE", "JOB_TYPE")
                    VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');
                    """%(info_dag.LOG_SCHEMA, info_dag.LOG_TABLE, info_dag.AIRFLOW_NAME, info_dag.DAG_APP_NAME, 
                         time_start, time_finish, status_job, status_finish, error_mess, job_type)
        self.execute(sql_query)
    
    def update_log(self, info_dag, time_finish, status_job='S', status_finish='1', error_mess='', job_type=''):
        sql_query = """
                    UPDATE "%s"."%s" SET
                    "DAG_ID"='%s', "JOB_NAME"='%s',  "TIME_FINISH"='%s', "STATUS_JOB"='%s', "STATUS_FINISH"='%s', "ERROR_MESSAGE"='%s', "JOB_TYPE"='%s'
                    WHERE "DAG_ID"='%s'
                    """%(info_dag.LOG_SCHEMA, info_dag.LOG_TABLE,
                        info_dag.AIRFLOW_NAME, info_dag.DAG_APP_NAME, time_finish, status_job, status_finish, error_mess, job_type,
                        info_dag.AIRFLOW_NAME)
        self.execute(sql_query)