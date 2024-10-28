import os 
import csv
import chardet
import re
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

def ETL_sample(data_source,**context):    
    if len(os.listdir("your os workdir path"))>0:
        os_path = "your os workdir path"
        backup_path = "your backup path"
        raw_data_path = "your raw data path"
        info_log_path = "your info log path"
        regex = re.compile(r'your correct filename format')
        error_filename_path = "your error filename path"
        column_replacing_pair = {"your replacing column name":"your ETL column name"}
        column_format_list = ["your ETL column name format list"]
        error_format_path = "your error format path"
        error_encode_path = "your error encode path"
        count_int = 0
        filename_list = os.listdir(os_path+raw_data_path)
        for filename in filename_list:
            logging.info("{} delete in raw data {} folder".format(filename,raw_data_path.split("/")[3]))
            os.system("mv {0} {1}".format(os_path+raw_data_path+filename,os_path+backup_path))
            check_encoding = open(os_path+backup_path+filename,"rb")
            target_encoding = check_encoding.read()
            encode =chardet.detect(target_encoding)["encoding"].lower()
            check_encoding.close()
            match = regex.search(filename)
            if match:
                try:
                    df_org = pd.read_csv(os_path+backup_path+filename,encoding="utf-8",engine='python')
                    counts = len(df_org.to_dict("records"))
                    logging.info('{0} {1}'.format("資料上傳的資料量",counts))
                    with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                        f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"資料上傳的資料量",counts))
                        f.close()
                    match_dataname_file = os_path+backup_path+match.group()
                    df = pd.read_csv(match_dataname_file,encoding="utf-8",engine='python')
                    df = df.rename(columns=lambda x: x.strip())
                    df = df.rename(columns=lambda c: column_replacing_pair[c] if c in column_replacing_pair.keys() else c)
                    column_name_list = df.columns.values.tolist()
                    if set(column_name_list) & set(column_format_list)==set(column_format_list):
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
                        df = df.drop(["no use column name list"], axis=1)

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
                        with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                            f.write('{0}-INFO-count {1} {2} {3}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),count_int,"檔名,欄位名稱及編碼正確的資料量",ok_counts))
                            f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),match.group(),"insert mongo success"))
                            f.close()
                        if mongoimport_process.returncode != 0:
                            raise RuntimeError                     
                    else:
                        error_counts = len(df.to_dict("records"))
                        logging.info("{0} {1}".format("上傳的欄位名稱",column_name_list))
                        logging.info('{0} {1}'.format("欄位名稱錯誤的資料量",error_counts))
                        with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                            f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"欄位名稱錯誤的資料量",error_counts))
                            f.close()
                        os.system("cp {0} {1}".format(match_dataname_file,os_path+error_format_path+match_dataname_file.split("/")[-1])) 
                        logging.info("{} is format error to be sent to format error folder".format(match_dataname_file.split("/")[-1]))
                   
                except:           
                    logging.info("{} is encode error to be sent to encode error folder".format(filename))
                    logging.info('{0} {1}'.format(filename,"檔案編碼錯誤")) 
                    with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                        f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename,"檔案編碼錯誤"))
                        f.close() 
                    os.system("cp {0} {1}".format(os_path+backup_path+filename,os_path+error_encode_path))
            else:
                logging.info("{} is filename error to be sent to filename error folder".format(filename))
                df_org = pd.read_csv(os_path+backup_path+filename,encoding="your encode",engine='python')
                counts = len(df_org.to_dict("records"))
                logging.info('{0} {1}'.format("檔名錯誤的資料量",counts)) 
                with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                        f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"檔名錯誤的資料量",counts))
                        f.close() 
                os.system("cp {0} {1}".format(os_path+backup_path+filename,os_path+error_filename_path))    


with DAG("ETL_sample", catchup=False, default_args=default_args,schedule_interval='* */1 * * *') as dag:
    download_task = PythonOperator(
        task_id = "download_from_s3",
        python_callable = download_from_s3,
        provide_context = True
    )
    ETL_process = PythonOperator(
        task_id = "ETL_sample",
        python_callable = ETL_sample,
        op_args = ["sample"],
    )
    
    
    # define workflow
    download_task >> ETL_process  
