# !/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
---
author:

- name: Nawfel Bacha
- email: nawfel@astrafy.io
- updated on: 03-09-2024

---
## **READ CAREFULLY** 
## 1. Data Marketing Team DAG works: 
The DAG is Pulling data using the PubSubPullSensor in order to check whenenever a message has been pushed from the Central Data team.
When a message is pulled the Downstream pipeline from the Data Marketing team will start doing their analysis and their data processing.
**IMPORTNAT**: there is a self triggered because the PubSubPullSensor will not continuesly sensing but as soon as it sense the first message
the DAG will be in a succesfull mode and this is not what we want.
"""


from datetime import datetime, timedelta
import base64

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label

def pull_function(**kwargs):
    ti = kwargs['ti']
    topic_xcom = ti.xcom_pull(task_ids='pull_messages',key='return_value')
    topic_message = topic_xcom[0]['message']['data']
    decoded_message = base64.b64decode(topic_message).decode()
    print(decoded_message)
    if "EU and US" in decoded_message:
        return "global_task"
    else:
        return None

def process_global(**kwargs):
    bq_hook = BigQueryHook(use_legacy_sql=False, location="europe-west1")
    sql_query = """
        SELECT * FROM `prj-demo-airflow-c69f.bqdts_sales.global_payment_transactions`
        ORDER BY transaction_ts DESC
        LIMIT 1
    """
    result = bq_hook.get_records(
        sql=sql_query,
    )
    print("Latest Global Transaction Data:", result)

    
with DAG(
    dag_id="data_marketing_team",
    start_date=datetime(2024, 8, 22),
    schedule=None,
    tags=["airflow_summit"],
    max_active_runs=1,
    concurrency=1,
    default_args={
        "owner": "nawfel.bacha",
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
    },
    doc_md=__doc__,
) as dag:
    pull_messages = PubSubPullSensor(
        task_id="pull_messages",
        ack_messages=True,
        project_id="prj-demo-airflow-c69f",
        subscription="airflow-dataset-demo-sub",
    )
    
    pull_task = BranchPythonOperator(
        task_id='pull_task', 
        python_callable=pull_function,
        provide_context=True
    )
    
    global_task = PythonOperator(
        task_id='global_task',
        python_callable=process_global
    )
    
    self_trigger_task = TriggerDagRunOperator(
        task_id='self_trigger_task',
        trigger_dag_id=dag.dag_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    pull_messages >> Label("Data pulling")  >> pull_task >> Label("Data Analysis") >> global_task>> Label("Sense new data") >> self_trigger_task
