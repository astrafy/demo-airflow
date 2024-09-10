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
## 1. How Ingest Regional Data Sale DAG works: 
The workflow is going to ingest Sales data coming from the different region (EU or US) into BigQuery.

#### 1.1 Execution options
The DAG will be triggered manually where we are going to have the parameter that represent the columns of the bigquery table

1. **transaction_id**: String containing the unique identifier of the transaction
2. **transaction_ts**: Timestamp of when the transaction it has been executed
3. **customer_country**: Country where the transaction it comes from
4. **region**: Region either EU or US valus accepted
5. **currency_code**: Code of the specific currency like EUR,USD
6. **amount**: Amount of the transaction representing the sales
7. **project_id**: BigQuery project 
8. **dataset_id**: BigQuery dataset where the tables are stored
9. **table_id**: BigQuery table name either **payment_transactions_eu** either **payment_transactions_us**

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.models.dataset import Dataset
from airflow.models import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.edgemodifier import Label
import requests
import uuid
from datetime import datetime

dataset_eu = Dataset("dataset-eu")
dataset_us = Dataset("dataset-us")

def region_selector(**kwargs):
    region = kwargs['dag_run'].conf.get('region', '').lower()
    if 'eu' in region:
        return 'process_eu_transactions'
    elif 'us' in region:
        return 'fetch_conversion_rate'
    else:
        return 'no_region_operator'

def dataset_selector(**kwargs):
    region = kwargs['dag_run'].conf.get('region', '').lower()
    if 'eu' in region:
        return 'dataset_eu_operator'
    elif 'us' in region:
        return 'dataset_us_operator'
    else:
        return 'no_region_operator'

def process_eu_transactions(**kwargs):
    params = kwargs['params']
    amount_with_vat = params['amount'] * 1.20  # Assuming 20% VAT
    kwargs['ti'].xcom_push(key='amount_with_vat', value=amount_with_vat)
    print(f"Processed EU transaction: {params['amount']} -> {amount_with_vat} (with VAT)")

def fetch_conversion_rate(**kwargs):
    params = kwargs['params']
    transaction_date = str(params['transaction_ts']).split("T")[0]
    
    response = requests.get(f"https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@{transaction_date}/v1/currencies/usd.json")
    data = response.json()
    conversion_rate = data['usd']['eur']
    kwargs['ti'].xcom_push(key='conversion_rate', value=conversion_rate)
    print(f"Fetched conversion rate of {transaction_date}: 1 USD -> {conversion_rate} EUR")

def process_us_transactions(**kwargs):
    params = kwargs['params']
    conversion_rate = kwargs['ti'].xcom_pull(key='conversion_rate')
    amount_eur = params['amount'] * conversion_rate
    kwargs['ti'].xcom_push(key='amount_eur', value=amount_eur)
    print(f"Processed US transaction: {params['amount']} -> {amount_eur} EUR (converted)")

with DAG(
    dag_id="ingest_regional_sales_data",
    start_date=datetime(2024, 8, 22),
    schedule=None,
    tags=["airflow_summit"],
    default_args={
        "owner": "nawfel.bacha",
        "retries": 1,
        "retry_delay": timedelta(seconds=60),
    },
    params={
        'transaction_id': Param(default=str(uuid.uuid4()), type="string"),
        'transaction_ts': Param(default=datetime.now().isoformat(), type="string"),
        'customer_country': Param(default="italy", type="string"),
        'region': Param(default="EU", type="string"),
        'currency_code': Param(default="EUR", type="string"),
        'amount': Param(default=200, type="number"),
        'project_id': Param(default="prj-demo-airflow-c69f", type="string"),
        'dataset_id': Param(default="bqdts_sales", type="string"),
        'table_id': Param(default="payment_transactions_REGION", type="string"),
    },
    doc_md=__doc__,
) as dag:
    
    region_selector_operator = BranchPythonOperator(
        task_id='region_selector_operator',
        python_callable=region_selector,
        provide_context=True
    )
    
    dataset_selector_operator = BranchPythonOperator(
        task_id='dataset_selector_operator',
        python_callable=dataset_selector,
        provide_context=True
    )
    
    process_eu = PythonOperator(
        task_id='process_eu_transactions',
        python_callable=process_eu_transactions,
        provide_context=True
    )

    fetch_rate = PythonOperator(
        task_id='fetch_conversion_rate',
        python_callable=fetch_conversion_rate,
        provide_context=True
    )

    process_us = PythonOperator(
        task_id='process_us_transactions',
        python_callable=process_us_transactions,
        provide_context=True
    )
    
    insert_data_task = BigQueryInsertJobOperator(
        task_id='insert_data_into_bq',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `{{ params.project_id }}.{{ params.dataset_id }}.{{ params.table_id }}`
                    (transaction_id,transaction_ts,region,customer_country,currency_code,amount,final_amount_eur)
                    VALUES (
                        '{{ params.transaction_id }}',
                        '{{ params.transaction_ts }}',
                        '{{ params.region }}',
                        '{{ params.customer_country }}',
                        '{{ params.currency_code }}',
                        ROUND(CAST({{ params.amount }} AS NUMERIC), 2),
                        CASE
                            WHEN '{{ params.region }}' = 'EU' THEN ROUND(CAST({% if ti.xcom_pull(key='amount_with_vat') is not none %}{{ ti.xcom_pull(key='amount_with_vat') }}{% else %}NULL{% endif %} AS NUMERIC), 2)
                            WHEN '{{ params.region }}' = 'US' THEN ROUND(CAST({% if ti.xcom_pull(key='amount_eur') is not none %}{{ ti.xcom_pull(key='amount_eur') }}{% else %}NULL{% endif %} AS NUMERIC), 2)
                            ELSE ROUND(CAST({{ params.amount }} AS NUMERIC), 2)
                        END
                    );
                """,
                "useLegacySql": False,
            }
        },
        trigger_rule=TriggerRule.NONE_FAILED
    )
    
    dataset_eu_operator = EmptyOperator(
        task_id='dataset_eu_operator',
        outlets=[dataset_eu]
    )
    
    dataset_us_operator = EmptyOperator(
        task_id='dataset_us_operator',
        outlets=[dataset_us]
    )
    
    no_region_operator = EmptyOperator(
        task_id='no_region_operator',
        trigger_rule="none_failed"
    )
    
    region_selector_operator >> Label("Region selector") >> [process_eu, fetch_rate, no_region_operator]
    process_eu >> Label("Processing EU data") >>  insert_data_task >> Label("Update Dataset") >>  dataset_selector_operator
    fetch_rate >> Label("Processing US data")  >> process_us >> insert_data_task >> Label("Update Dataset") >> dataset_selector_operator
    dataset_selector_operator >> Label("Dataset Selector") >> [dataset_eu_operator, dataset_us_operator, no_region_operator]