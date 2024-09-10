# Global E-commerce Data Pipeline

## Context

The context is a global e-commerce company that processes payment transactions from customers in Europe (EU) and the United States (US). 

In the company, there is a central team, often referred to as the **Data Central Team**, **Datalake Team**, or simply **Data Team**, which has the role of centralizing data from different sources and then dispatching it to other departments or teams.

In this case, the **Marketing Team** wants to consolidate this data across regions to analyze global sales trends, customer behavior, and campaign effectiveness.

---

## DAG A: Ingest and Transform Data

**Objective**: Ingest and transform data coming from different sources (EU and US).

### EU Payment Data Ingestion Task
- Ingest payment transaction data from the EU, ensuring compliance with **GDPR**.
- Load data into a **BigQuery**.

### US Payment Data Ingestion Task
- Ingest payment transaction data from the US.
- Validate US data for **CCPA** compliance.

### Data Processing
- **EU**: Calculate and append **VAT** information to each transaction.
- **US**: Convert all transaction amounts to **EUR** based on the currency rate of the day.

### Data Consolidation and Union
Since the marketing team wants to have a global, region-independent view, data from the EU and US needs to be consolidated.

---

## DAG B: Dataset Consolidation

The concept of a dataset per region is used. After ingestion and processing of EU and US data, **DAG B** is triggered (thanks to dataset updates).

### Key Steps
- **Consolidate** data from different regions.
- Notify the marketing team via a **Pub/Sub message** that the data is ready for their analysis.

---

## DAG C: Marketing Team Processing

The marketing team (usually with their own Airflow instance) will have a DAG that listens for messages from the Central Data Team. When it receives the notification, it will execute the marketing DAG, which processes the newly available data.