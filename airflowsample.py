import os 
import csv
import chardet
import re
import logging
import pandas as pd
import subprocess
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner":"Vincent",
    "start_date":datetime(2022,3,11),
    "retries":2,
    "retry_delay":timedelta(minutes=1),
    "dependents_on_past":True
}

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
        encode_list=["big5","utf-8"]
        count_int = 0
        filename_list = os.listdir(os_path+raw_data_path)
        for filename in filename_list:
            logging.info("{} delete in raw data {} folder".format(filename,raw_data_path.split("/")[3]))
            os.system("mv {0} {1}".format(os_path+raw_data_path+filename,os_path+backup_path))
            check_encoding = open(os_path+backup_path+filename,"rb")
            target_encoding = check_encoding.read()
            encode =chardet.detect(target_encoding)["encoding"].lower()
            check_encoding.close()
            if encode in encode_list:
                df_org = pd.read_csv(os_path+backup_path+filename,encoding=encode,engine='python')
                counts = len(df_org.to_dict("records"))
                logging.info('{0} {1}'.format("資料上傳的資料量",counts))
                with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                    f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"客戶資料FTP上傳至NAS的資料量",counts))
                    f.close()

                match = regex.search(filename)
                if match:
                    match_dataname_file = os_path+backup_path+match.group()
                    df = pd.read_csv(match_dataname_file,encoding=encode,engine='python')
                    df = df.rename(columns=lambda x: x.strip())
                    df = df.rename(columns=lambda c: column_replacing_pair[c] if c in column_replacing_pair.keys() else c)
                    column_name_list = df.columns.values.tolist()
                    if set(column_name_list) & set(column_format_list)==set(column_format_list):
                        df["sample time"] = pd.to_datetime(df["sample time"])        
                        df["sample time"] = pd.Series(
                                    df["sample time"]
                                    .dt.tz_localize("Etc/GMT-8")
                                    .dt.tz_convert("UTC")
                                    .dt.tz_localize(None)
                                    .dt.to_pydatetime(),
                                    dtype=object,
                                )
                        df["sample numeric value"] = pd.to_numeric(df["sample numeric value"], "coerce")
                        df["sample concatenate result"] = df["sample need to concatenate"].astype(str).apply(lambda s:s.strip()) + "-" + df["sample need to concatenate"].astype(str)
                        df = df.drop(["no use column name list"], axis=1)

                        import_df = df.rename(
                            columns={
                                "sample name": "name.string()",
                                "sample concatenate result": "result.string()",
                                "sample numeric value": "value.double()",
                                "sample time": "time.date(2006-01-02 15:04:05)",
                            }
                        )
                        # use mongoimport to load file
                        MONGO_HOST = "your mongodb host"
                        MONGO_PORT = "your mongodb port"
                        MONGO_USER = "your mongodb user"
                        MONGO_PASSWORD = "your mongodb password"
                        MONGO_DB = "your mongodb database"
                        data_type = "your insert mongodb collection"
                        mongoimport_process = subprocess.Popen(
                                [
                                "mongoimport",
                                # Auth
                                f"--host={MONGO_HOST}",
                                f"--port={MONGO_PORT}",
                                f"--username={MONGO_USER}",
                                f"--password={MONGO_PASSWORD}",
                                f"--authenticationDatabase={MONGO_DB}",
                                # # setting
                                f"--db={MONGO_DB}",
                                f"--collection={data_type}",
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
                        logging.info('{0} {1}'.format("欄位名稱錯誤的資料量",error_counts))
                        with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                            f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"欄位名稱錯誤的資料量",error_counts))
                            f.close()
                        os.system("cp {0} {1}".format(match_dataname_file,os_path+error_format_path+match_dataname_file.split("/")[-1])) 
                        logging.info("{} is format error to be sent to format error folder".format(match_dataname_file.split("/")[-1]))
                else:
                    logging.info("{} is filename error to be sent to filename error folder".format(filename))
                    df_org = pd.read_csv(os_path+backup_path+filename,encoding=encode,engine='python')
                    counts = len(df_org.to_dict("records"))
                    logging.info('{0} {1}'.format("檔名錯誤的資料量",counts)) 
                    with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                            f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"檔名錯誤的資料量",counts))
                            f.close() 
                    os.system("cp {0} {1}".format(os_path+backup_path+filename,os_path+error_filename_path))
            else:           
                logging.info("{} is encode error to be sent to encode error folder".format(filename))
                logging.info('{0} {1}'.format(filename,"檔案編碼錯誤")) 
                with open (os_path+info_log_path+"{}.log".format(datetime.now().strftime("%Y%m%d")),"a") as f:
                    f.write('{0}-INFO-{1} {2}\n'.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),filename,"檔案編碼錯誤"))
                    f.close() 
                os.system("cp {0} {1}".format(os_path+backup_path+filename,os_path+error_encode_path))         


with DAG("ETL_sample", catchup=False, default_args=default_args,schedule_interval='* */1 * * *') as dag:
    ETL_sample = PythonOperator(
        task_id="ETL_sample",
        python_callable=ETL_sample,
        op_args=["sample"],
    )
    
    
    # define workflow
    ETL_sample  
