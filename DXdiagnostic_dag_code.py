# --------------------------------
# LIBRARIES
# --------------------------------

# Import Airflow Operators
from airflow import DAG
from airflow.operators.dummy import DummyOperator 
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import BranchPythonOperator
# Import Libraries to handle dataframes
import pandas as pd
import sqlalchemy as db
import numpy as np
# Import Library to send Slack Messages
import requests
# Import Library to set DAG timing
from datetime import timedelta, datetime

# --------------------------------
# CONSTANTS
# --------------------------------

# Set input and output paths
FILE_PATH_INPUT = '/home/airflow/gcs/data/input/'
FILE_PATH_OUTPUT = '/home/airflow/gcs/data/output/'
# Import CSV into a pandas dataframe
df = pd.read_csv(FILE_PATH_INPUT + "diag_metrics_P0015.csv")
# Slack webhook link
slack_webhook = 'https://hooks.slack.com/services/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'

# --------------------------------
# FUNCTION DEFINITIONS
# --------------------------------

# Function to send messages over slack using the slack_webhook
def send_msg(text_string): requests.post(slack_webhook, json={'text': text_string})
 

# Function to generate the diagnostics report
# Add print statements to each variable so that it appears on the Logs
def send_report():
    avg_o2_level = df['o2_level'].mean()
    print(avg_o2_level)
    avg_hr_level = df['heart_rate'].mean()
    print(avg_hr_level)
    std_o2_level = df['o2_level'].std()
    print(std_o2_level)
    std_hr_level = df['heart_rate'].std()
    print(std_hr_level)
    min_o2_level = df['o2_level'].min()
    print(min_o2_level)
    min_hr_level = df['heart_rate'].min()
    print(min_hr_level)
    max_o2_level = df['o2_level'].max()
    print(max_o2_level)
    max_hr_level = df['heart_rate'].max()
    print(max_hr_level)
    body = """
    -------------------------
    Diagnostic Report
    -------------------------
    #1. Average O2 level: {0}
    #2. Average Heart Rate: {1}
    #3. Standard Deviation of O2 level: {2}
    #4. Standard Deviation of Heart Rate: {3}
    #5. Minimum O2 level: {4}
    #6. Minimum Heart Rate: {5}
    #7. Maximum O2 level: {6}
    #8. Maximum Heart Rate: {7}
    """.format(avg_o2_level, avg_hr_level, std_o2_level, std_hr_level, min_o2_level, min_hr_level,max_o2_level, max_hr_level)
    send_msg(str(body))

  
#3 Function to filter anomalies in the data
# Add print statements to each output dataframe so that it appears on the Logs
def flag_anomaly():
    hr_mu=80.81
    hr_sigma=10.28
    o2_mu=96.19
    o2_sigma=1.69
    
    df['heart_rate'] = np.where((df['heart_rate'] > (hr_mu + 3*hr_sigma)) | (df['heart_rate'] < (hr_mu - 3*hr_sigma)), 1, 0)
    hrdata = df[df['heart_rate'] == 1]
    hrdata.to_csv(FILE_PATH_OUTPUT + 'hr_anomaly_P0015.csv')
    print(hrdata)
    
    df['o2_level'] = np.where((df['o2_level'] > (o2_mu + 3*o2_sigma)) | (df['o2_level'] < (o2_mu - 3*o2_sigma)), 1, 0)
    o2data = df[df['o2_level'] == 1]
    o2data.to_csv(FILE_PATH_OUTPUT + 'o2_anomaly_P0015.csv')
    print(o2data)
    
  
# --------------------------------
# DAG definition
# --------------------------------

# Define the defualt args
default_args = {
    'owner': 'user_name',
    'start_date': datetime(2022, 2, 3),
    'depends_on_past': False,
    'email': ['faithtirtarahardja@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

# Create the DAG object
with DAG(
    'DXdiagnostic_dag_code',
    default_args=default_args,
    description='DAG',
    catchup=True
) as dag:

    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )
    file_sensor_task = FileSensor(
        task_id='file_sensor_task',
        poke_interval=15,
        filepath=FILE_PATH_INPUT,
        timeout=5,
        dag=dag
    )
    send_report_task = PythonOperator(
    task_id='send_report_task',
    python_callable=send_report,
    dag=dag
    )
    flag_anomaly_task=PythonOperator(
        task_id='flag_anomaly_task',
        python_callable=flag_anomaly,
        dag=dag
    )
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )

    start_task >> file_sensor_task >> [send_report_task, flag_anomaly_task] >> end_task
        
    # start_task
    # file_sensor_task
    # send_report_task
    # flag_anomaly_task
    # end_task
# Set the dependencies

# --------------------------------