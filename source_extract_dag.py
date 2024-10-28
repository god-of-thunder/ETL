import os 
import csv
import logging
import pandas as pd
import subprocess
import boto3
from datetime import datetime,timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook

default_args = {
    "owner":"airflow",
    "start_date":datetime(2022,3,11),
    "retries":2,
    "retry_delay":timedelta(minutes=1),
    "dependents_on_past":True
}

def download_from_s3(**context):
    s3 = boto3.client(
        's3',
        aws_access_key_id = Variable.get("s3_access_key"),
        aws_secret_access_key = Variable.get("s3_scecret_key"),
        region_name = context["dag_run"].conf.get("region")
    )
    s3.download_file(context["dag_run"].conf.get("bucket"), '/source/file.csv', '/tmp/file.csv')

def ETL_sample(**context):    
    if len(os.listdir("/tmp"))>0:
        column_format_list = Variable.get("drop_column_list", deserialize_json=True)
        column_replacing_pair = context["dag_run"].conf.get("column_replacing_pair")
        count_int = 0
        try:
            df = pd.read_csv("/tmp/file.csv",encoding="utf-8",engine='python')
            counts = len(df.to_dict("records"))
            logging.info('{0} {1}'.format("資料上傳的資料量",counts))
            df = df.rename(columns=lambda x: x.strip())
            df = df.rename(columns=lambda c: column_replacing_pair[c] if c in column_replacing_pair.keys() else c)
            column_name_list = df.columns.values.tolist()
            set(column_name_list) & set(column_format_list)==set(column_format_list)
            df["date"] = pd.to_datetime(df["date"])        
            df["date"] = pd.Series(
                        df["date"]
                        .dt.tz_localize("Etc/GMT-8")
                        .dt.tz_convert("UTC")
                        .dt.tz_localize(None)
                        .dt.to_pydatetime(),
                        dtype=object,
                    )
            df["value"] = pd.to_numeric(df["value"], "coerce")
            df["description"] = df["description"].astype(str).apply(lambda s:s.strip()) + "-" + df["sample need to concatenate"].astype(str)
            df = df.drop(Variable.get("drop_column_list", deserialize_json=True), axis=1)

            import_df = df.rename(
                columns={
                    "name": "name.string()",
                    "description": "result.string()",
                    "value": "value.double()",
                    "date": "time.date(2006-01-02 15:04:05)",
                }
            )
            # use mongoimport to load file
            conn = BaseHook.get_connection('demo_db')
            collection = "your insert mongodb collection"
            mongoimport_process = subprocess.Popen(
                    [
                    "mongoimport",
                    # Auth
                    f"--host={conn.host}",
                    f"--port={conn.port}",
                    f"--username={conn.login}",
                    f"--password={conn.password}",
                    f"--authenticationDatabase={conn.schema}",
                    # # setting
                    f"--db={conn.schema}",
                    f"--collection={collection}",
                    "--type=csv",
                    "--headerline",
                    "--ignoreBlanks",
                    "--parseGrace=skipField",
                    "--columnsHaveTypes",
                    ],
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            mongoimport_process.communicate(
                input=import_df.to_csv(
                    index=False,
                    quoting=csv.QUOTE_NONNUMERIC,
                    encoding="utf-8",
                    date_format="%Y-%m-%d %H:%M:%S",
                ).encode()
            )
            count_int+=1
            ok_counts = len(import_df.to_dict("records"))
            logging.info('{0} {1} {2}'.format(count_int,"檔名,欄位名稱及編碼正確的資料量",ok_counts)) 
            if mongoimport_process.returncode != 0:
                raise RuntimeError                                 
        except:           
            logging.info('{0} {1}'.format("tmp.csv","檔案編碼錯誤")) 
            

with DAG("ETL_sample", catchup=False, default_args=default_args,schedule_interval='* */1 * * *') as dag:
    download_task = PythonOperator(
        task_id = "download_from_s3",
        python_callable = download_from_s3,
        provide_context = True
    )
    ETL_process = PythonOperator(
        task_id = "ETL_sample",
        python_callable = ETL_sample,
        dag = dag
    )
    
    
    # define workflow
    download_task >> ETL_process  
