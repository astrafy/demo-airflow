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
## 1. Global Data Consolidation DAG works: 
The DAG is triggered when the dataset_eu and dataset_us have been updated.
So, as soon as both of them have been updated the pipeline will consolidate in BigQuery all the sales data coming from EU and US
in a single table.
After this step a PubSub message will be sent to the Marketing team to acknoledge them that new data are available and they can consume it.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.models.dataset import Dataset
from airflow.utils.edgemodifier import Label

dataset_eu = Dataset("dataset-eu")
dataset_us = Dataset("dataset-us")

with DAG(
    dag_id="global_data_consolidation",
    start_date=datetime(2024, 8, 22),
    schedule=[dataset_eu, dataset_us],
    tags=["airflow_summit"],
    default_args={
        "owner": "nawfel.bacha",
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
    },
    doc_md=__doc__,
) as dag:
    consolidate_data = BigQueryExecuteQueryOperator(
        task_id="consolidate_data",
        sql="""
            INSERT INTO `{{ params.project_id }}.{{ params.dataset_id }}.global_payment_transactions`
            (transaction_id, transaction_ts, region, customer_country, currency_code, amount, final_amount_eur)
            
            -- Insert new EU transactions
            SELECT eu.transaction_id, eu.transaction_ts, eu.region, eu.customer_country, eu.currency_code, eu.amount, eu.final_amount_eur
            FROM `{{ params.project_id }}.{{ params.dataset_id }}.payment_transactions_eu` eu
            LEFT JOIN `{{ params.project_id }}.{{ params.dataset_id }}.global_payment_transactions` global
            ON eu.transaction_id = global.transaction_id
            WHERE global.transaction_id IS NULL
            
            UNION ALL
            
            -- Insert new US transactions
            SELECT us.transaction_id, us.transaction_ts, us.region, us.customer_country, us.currency_code, us.amount, us.final_amount_eur
            FROM `{{ params.project_id }}.{{ params.dataset_id }}.payment_transactions_us` us
            LEFT JOIN `{{ params.project_id }}.{{ params.dataset_id }}.global_payment_transactions` global
            ON us.transaction_id = global.transaction_id
            WHERE global.transaction_id IS NULL;
        """,
        use_legacy_sql=False,
        params={
            'project_id': 'prj-demo-airflow-c69f',
            'dataset_id': 'bqdts_sales',
        },
    )
    
    message = {
        "data": b"Data is updated for EU and US",
    }
    
    publish_message = PubSubPublishMessageOperator(
        task_id="publish_message",
        project_id="prj-demo-airflow-c69f",
        topic="airflow-dataset-demo",
        messages=[message],
    )
    
    consolidate_data >> Label("Data Aggregation") >> publish_message
