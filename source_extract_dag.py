import os 
import logging
import pandas as pd
from pymongo import MongoClient
import boto3
from datetime import datetime,timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.email import send_email
import warnings,traceback
warnings.filterwarnings('ignore')

default_args = {
    "owner":"airflow",
    "start_date":datetime(2024,10,28),
    "dependents_on_past":False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': lambda context: send_email(
        to='your-email@example.com',
        subject=f"Airflow Task Failed: {context['task_instance_key_str']}",
        html_content="Task failed. Check Airflow logs for details."
    )
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
        records = df.to_dict(orient='records')

        # load to mongodb
        conn = BaseHook.get_connection('demo_db')
        client = MongoClient(
        host = conn.host,
        username = conn.login,
        password = conn.password,
        port = conn.port,
        authMechanism='SCRAM-SHA-256'
        )
        session = client.start_session()
        db = client[conn.schema]
        collection = db[ Variable.get("insert_collection")]
        collection.insert_many(records)
        try:
            with session.start_transaction():
                for record in records:
                    collection.insert_one(record, session=session)
            ok_counts = len(records)
            logging.info('{0} {1}'.format("檔名,欄位名稱及編碼正確的資料量",ok_counts))
            os.remove("/tmp/file.csv")
        except:
            message = traceback.format_exc()
            error_message = f"""error : {message}"""
            logging.error('交易失敗, 操作回滾復原') 
            logging.error(error_message)     
                                             
        

with DAG("ETL_sample", catchup=False, default_args=default_args,schedule_interval='0 5 * * *') as dag:
    download_task = PythonOperator(
        task_id = "download_from_s3",
        python_callable = download_from_s3,
        provide_context = True,
        dag = dag
    )
    ETL_process = PythonOperator(
        task_id = "ETL_sample",
        python_callable = ETL_sample,
        provide_context = True,
        dag = dag
    )
    
    
    # define workflow
    download_task >> ETL_process  
