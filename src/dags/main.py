from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '/home/nguyentuanduong7/airflow/src/pluggin')))

from stock_subscription import vm_pub_pubsub_1H,vm_pub_pubsub_1M
from collect_gcs import stock_collection_1Y,stock_collection_1D,send_gsc_1D,send_gsc_1Y
from vm_pubsub_bq import vm_pubsub_3M,vm_pubsub_1Y

default_args = {
    'owner': 'Duong_',
    'depends_on_past': False,
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["nguyentuanduong555@gmail.com"],
    "start_date": datetime(2023, 8, 30),
    'retries': 3,
    "retry_delay": timedelta(minutes=5),

}
# def simulate_failure():
#     raise Exception("This task has failed intentionally.")
with DAG(
        default_args=default_args,
        dag_id="vnstock-pipeline-1D",
        schedule_interval="0 16 * * *",
        catchup=False
    ) as dag_16H:

    # failure_task = PythonOperator(
    #     task_id='simulate_failure_task',
    #     python_callable=simulate_failure,
    # )

    Stock_collection_1D = PythonOperator(
        task_id = "Stock_collection_1D",
        python_callable=stock_collection_1D
    )

    Send_gsc_1D= PythonOperator(
        task_id = "Send_gsc_1D",
        python_callable=send_gsc_1D
    )

    Successful_Alert_Project_1D = EmailOperator(
        task_id="Successful_Alert_Project_1D",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>1D ok</h1>"""
    )

with DAG(
        default_args=default_args,
        dag_id="vnstock-pipeline-1H",
    schedule_interval="*/60 8-16 * * *"
    ) as dag_1H:

    Vm_pub_pubsub_1H = PythonOperator(
        task_id = "Vm_pub_pubsub_1H",
        python_callable=vm_pub_pubsub_1H
    )
    Successful_Alert_Project_1H = EmailOperator(
        task_id="Successful_Alert_Project_1H",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>1H ok</h1>"""
    )

with DAG(
        default_args=default_args,
        dag_id="vnstock-pipeline-1M",
    schedule_interval="*/5 8-16 * * *"
    ) as dag_1M:

    Vm_pub_pubsub_1M = PythonOperator(
        task_id = "Vm_pub_pubsub_1M",
        python_callable=vm_pub_pubsub_1M
    )

with DAG(
        default_args=default_args,
        dag_id="vnstock-pipeline-3M-1Y",
    schedule_interval="0 20 * * *"
    ) as dag_3M_1Y:

    Stock_collection_1Y = PythonOperator(
        task_id = "Stock_collection_1Y",
        python_callable=stock_collection_1Y
    )
    Send_gsc_1Y = PythonOperator(
        task_id = "Send_gsc_1Y",
        python_callable=send_gsc_1Y
    )
    Vm_pubsub_3M = PythonOperator(
        task_id = "Vm_pubsub_3M",
        python_callable=vm_pubsub_3M
    )
    Vm_pubsub_1Y = PythonOperator(
        task_id = "Vm_pubsub_1Y",
        python_callable=vm_pubsub_1Y
    )
    Successful_Alert_Project_1Y_3M = EmailOperator(
        task_id="Successful_Alert_Project_1Y_3M",
        to="nguyentuanduong555@gmail.com",
        subject="Airflow success alert",
        html_content="""<h1>1Y 3M ok</h1>"""
    )



Vm_pub_pubsub_1M
Vm_pub_pubsub_1H >> Successful_Alert_Project_1H
# failure_task >> Stock_collection_1D >> Send_gsc_1D >> Successful_Alert_Project_1D
Stock_collection_1D >> Send_gsc_1D >> Successful_Alert_Project_1D
 
Stock_collection_1Y >> Send_gsc_1Y >> Vm_pubsub_1Y >> Vm_pubsub_3M >> Successful_Alert_Project_1Y_3M


