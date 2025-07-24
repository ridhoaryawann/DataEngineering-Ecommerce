from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import logging

from datetime import datetime, timedelta
import pandas as pd
import os 

log = logging.getLogger(__name__)

# --- Local Var Config ---
PG_SCHEMA       = 'ecommerce'
EXPORT_DIR      = '/opt/airflow/datas/exports'

# --- GCP Var Config
PROJECT_ID      = 'purwadika'
BUCKET_NAME     = 'jdeol003-bucket'
GCS_PREFIX      = 'finpro_rido'
BQ_DATASET      = 'jcdeol3_finpro_rido'


# --- Function ---

# 1. Postgres to GCS for Dim table
def extract_to_gcs(ds, table, transform = None, **kwargs):
    pg_hook     = PostgresHook(postgres_conn_id = 'postgres')
    query       = f"SELECT * FROM {PG_SCHEMA}.{table}"
    df          = pg_hook.get_pandas_df(query)

    if transform:
        df      = transform(df)

    folder_path = os.path.join(EXPORT_DIR, table)
    os.makedirs(name=folder_path, exist_ok=True)

    file_path   = os.path.join(folder_path, f"{table}.parquet")
    df.to_parquet(file_path, index=False)

    gcs_path    = f"{GCS_PREFIX}/{table}/{table}.parquet"
    gcs_hook    = GCSHook(gcp_conn_id = 'google_cloud_default')
    gcs_hook.upload(
        bucket_name     = BUCKET_NAME,
        object_name     = gcs_path,
        filename        = file_path
    )

# 1.B Postgres to GCS Incermental for Transaction table 
def extract_to_gcs_inc(ds, prev_ds, table, col_inc, transform = None, **kwargs):
    pg_hook     = PostgresHook(postgres_conn_id = 'postgres')
    query       = f"SELECT * FROM {PG_SCHEMA}.{table} WHERE DATE({col_inc}) = '{prev_ds}' "
    df          = pg_hook.get_pandas_df(query)

    if df.empty:
        raise AirflowSkipException(f"No data found for table '{table}' on {prev_ds}. Skipping task.")
    
    if transform:
        df      = transform(df)

    folder_path = os.path.join(EXPORT_DIR, table)
    os.makedirs(name=folder_path, exist_ok=True)

    file_path   = os.path.join(folder_path, f"{table}_{ds}.parquet")
    df.to_parquet(file_path, index=False)

    gcs_path    = f"{GCS_PREFIX}/{table}/{table}_{ds}.parquet"
    gcs_hook    = GCSHook(gcp_conn_id = 'google_cloud_default')
    gcs_hook.upload(
        bucket_name     = BUCKET_NAME,
        object_name     = gcs_path,
        filename        = file_path
    )

# 1.C. Postgres to GCS Incremental CSV Version
def extract_to_gcs_inc_csv(ds, prev_ds, table, col_inc, transform=None, **kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    query = f"SELECT * FROM {PG_SCHEMA}.{table} WHERE DATE({col_inc}) = '{prev_ds}'"
    df = pg_hook.get_pandas_df(query)

    if df.empty:
        raise AirflowSkipException(f"No data found for table '{table}' on {prev_ds}. Skipping task.")

    if transform:
        df = transform(df)

    folder_path = os.path.join(EXPORT_DIR, table)
    os.makedirs(name=folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, f"{table}_{ds}.csv")
    df.to_csv(file_path, index=False)

    gcs_path = f"{GCS_PREFIX}/{table}/{table}_{ds}.csv"
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_path,
        filename=file_path
    )

# 2. GSC to BQ_staging
def extract_to_bq_stg(
        gcs_file_name,
        table_name,
        schema_fields,
        partition_field = None,
        cluster_fields = None,
        write_disposition = "WRITE_APPEND"
):
    config      = {
        "load"  : {
            "destinationTable" : {
                "projectId"     : PROJECT_ID,
                "datasetId"     : BQ_DATASET,
                "tableId"       : table_name,
            },
            "sourceUris": [
                f"gs://{BUCKET_NAME}/{GCS_PREFIX}/{gcs_file_name}/{gcs_file_name}_{{{{ds}}}}.parquet"
            ],
            "sourceFormat"      : "PARQUET",
            "autodetect"        : False,
            "schema"            : {"fields" : schema_fields},
            "writeDisposition"  : write_disposition
        }
    }

    if partition_field:
        config["load"]["timePartitioning"] = {
            "type"  : "DAY",
            "field" : partition_field
        }

    if cluster_fields:
        config["load"]["clustering"] = {
            "fields" : cluster_fields
        }

    return config
# ----------------------------------------------------------
# 2. GSC to BQ_staging for dimension
def extract_to_bq_stgd(
        gcs_file_name,
        table_name,
        schema_fields,
        partition_field = None,
        cluster_fields = None,
        write_disposition = "WRITE_TRUNCATE"
):
    config      = {
        "load"  : {
            "destinationTable" : {
                "projectId"     : PROJECT_ID,
                "datasetId"     : BQ_DATASET,
                "tableId"       : table_name,
            },
            "sourceUris": [
                f"gs://{BUCKET_NAME}/{GCS_PREFIX}/{gcs_file_name}/{gcs_file_name}.parquet"
            ],
            "sourceFormat"      : "PARQUET",
            "autodetect"        : False,
            "schema"            : {"fields" : schema_fields},
            "writeDisposition"  : write_disposition
        }
    }

    if partition_field:
        config["load"]["timePartitioning"] = {
            "type"  : "DAY",
            "field" : partition_field
        }

    if cluster_fields:
        config["load"]["clustering"] = {
            "fields" : cluster_fields
        }

    return config
# ----------------------------------------------------------
# 2.B. GCS to BQ_staging csv
def extract_to_bq_stg_csv(
        gcs_file_name,
        table_name,
        schema_fields,
        partition_field=None,
        cluster_fields=None,
        write_disposition="WRITE_APPEND"
):
    config = {
        "load": {
            "destinationTable": {
                "projectId": PROJECT_ID,
                "datasetId": BQ_DATASET,
                "tableId": table_name,
            },
            "sourceUris": [
                f"gs://{BUCKET_NAME}/{GCS_PREFIX}/{gcs_file_name}/{gcs_file_name}_{{{{ds}}}}.csv"
            ],
            "sourceFormat": "CSV",
            "autodetect": False,
            "schema": {"fields": schema_fields},
            "writeDisposition": write_disposition,
            "skipLeadingRows": 1
        }
    }

    if partition_field:
        config["load"]["timePartitioning"] = {
            "type": "DAY",
            "field": partition_field
        }

    if cluster_fields:
        config["load"]["clustering"] = {
            "fields": cluster_fields
        }

    return config

# 3.Slack Notification

SLACK_CONN_ID = 'slack_webhook_connection'

def task_fail_slack_alert(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date'].isoformat()
    log_url = context['task_instance'].log_url

    message = f"""
:red_circle: *Airflow Task Failed!*
*DAG*: `{dag_id}`
*Task*: `{task_id}`
*Execution Time*: `{execution_date}`
*Log URL*: <{log_url}|View Logs>
"""

    slack_alert = SlackWebhookOperator(
            task_id='slack_fail_notification',
            slack_webhook_conn_id=SLACK_CONN_ID,
            message=message,
            channel='#your-alert-channel'  # optional if set in the Slack connection
        )
    try:
            slack_alert.execute(context=context)
            log.info(f"Slack alert sent for failed task {task_id} in DAG {dag_id}")
    except Exception as e:
            log.error(f"Failed to send Slack alert: {e}")    


# --- DAG ---

default_args    = {
    'owner'         : 'airflow',
    'start_date'    : datetime(2025,6,10,4),
    'retries'       : 2,
    'retry_delay'   : timedelta(seconds=10),
    'on_failure_callback': task_fail_slack_alert
}

with DAG(
    dag_id              = 'DAG_local_to_staging_to_DimFact_to_mart',
    default_args        = default_args,
    schedule_interval   = '@daily',
    catchup             = True
) as dag:
    
    # ----------------------------------- A. Seller Table -----------------------------------

    # 1. Seller Extract to GCS Task
    seller_to_gcs  = PythonOperator(
        task_id         = 'seller_to_gcs',
        python_callable = extract_to_gcs,
        op_kwargs       = {'table':'seller'}
    )

    # 2. Seller Load to Staging BQ
    seller_schema = [
        {"name": "seller_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_zip_code_prefix", "type": "INT64", "mode": "NULLABLE"},
        {"name": "seller_city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "seller_state", "type": "STRING", "mode": "NULLABLE"}
    ]

    seller_to_stg = BigQueryInsertJobOperator(
        task_id         = 'seller_to_bq_staging',
        configuration   = extract_to_bq_stgd(
            gcs_file_name   = 'seller',
            table_name      = 'stg_seller',
            schema_fields   = seller_schema,
            cluster_fields  = ["seller_state"],
            write_disposition= "WRITE_TRUNCATE"
        ),
        location = "US"
    )

    # 3. Seller Load to Dim BQ
    seller_to_dim = BigQueryInsertJobOperator(
        task_id         = 'seller_to_bq_dim',
        configuration   = {
            "query"     : {
                "query" : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.dim_seller` (
                    seller_id STRING NOT NULL,
                    seller_zip_code_prefix INT64,
                    seller_city STRING,
                    seller_state STRING )
                    CLUSTER BY seller_state;

                    MERGE `purwadika.jcdeol3_finpro_rido.dim_seller` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_seller` s
                    ON t.seller_id  = s.seller_id

                    WHEN NOT MATCHED THEN
                    INSERT (seller_id, seller_zip_code_prefix, seller_city, seller_state)
                    VALUES (s.seller_id, s.seller_zip_code_prefix, s.seller_city, s.seller_state);
            """,
            "useLegacySql" : False
            }
        }, location="US"
    )

    # ----------------------------------- B. Product Table -----------------------------------

    # 1. Product Extract to GCS Task
    prod_to_gcs = PythonOperator(
        task_id         = 'product_to_gcs',
        python_callable = extract_to_gcs,
        op_kwargs       = {'table':'product'}
    )

    # 2. Product Load to Staging BQ
    prod_schema = [
    {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "product_category_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_category_name_english", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_name_lenght", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_description_lenght", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_photos_qty", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_weight_g", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_length_cm", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_height_cm", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "product_width_cm", "type": "FLOAT64", "mode": "NULLABLE"}
    ]

    prod_to_stg = BigQueryInsertJobOperator(
        task_id         = 'product_to_bq_staging',
        configuration   = extract_to_bq_stgd(
            gcs_file_name   = 'product',
            table_name      = 'stg_product',
            schema_fields   = prod_schema,
            cluster_fields  = ["product_category_name_english"],
        ),
        location = "US"
    )

    # 3. Product Load to Dim BQ
    prod_to_dim = BigQueryInsertJobOperator(
        task_id         = 'product_to_bq_dim',
        configuration   = {
            "query"     : {
                "query" : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.dim_product`
                    (
                    product_id STRING NOT NULL,
                    product_category_name STRING,
                    product_category_name_english STRING,
                    product_name_lenght FLOAT64,
                    product_description_lenght FLOAT64,
                    product_photos_qty FLOAT64,
                    product_weight_g FLOAT64,
                    product_length_cm FLOAT64,
                    product_height_cm FLOAT64,
                    product_width_cm FLOAT64
                    )
                    CLUSTER BY product_category_name_english;
                           
                    MERGE `purwadika.jcdeol3_finpro_rido.dim_product` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_product` s
                    ON t.product_id = s.product_id
                           
                    WHEN NOT MATCHED THEN
                    INSERT (product_id, product_category_name, product_category_name_english, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
                    VALUES (s.product_id, s.product_category_name, s.product_category_name_english, s.product_name_lenght, s.product_description_lenght, s.product_photos_qty, s.product_weight_g, s.product_length_cm, s.product_height_cm, s.product_width_cm);
                """,
                "useLegacySql"  : False
                }
            }, location="US"
    )


    # ----------------------------------- C. Customer Table ----------------------------------

    # 1. Customer Extract to GCS Task
    def transform_customer_df(df):
        df['first_purchase_date'] = pd.to_datetime(df['first_purchase_date'], errors='coerce')
        df['first_purchase_date'] = df['first_purchase_date'].astype('datetime64[us]')
        return df
    
    cust_to_gcs = PythonOperator(
        task_id         = 'customer_to_gcs',
        python_callable = extract_to_gcs,
        op_kwargs       = {'table':'customer',
                           'transform': transform_customer_df}
    )
    # -------------------------------------------------------------------
    # 2. Customer Load to Staging BQ
    customer_schema = [
    {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "customer_unique_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_zip_code_prefix", "type": "INT64", "mode": "NULLABLE"},
    {"name": "customer_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_state", "type": "STRING", "mode": "NULLABLE"},
    {"name": "first_purchase_date", "type": "DATETIME", "mode": "REQUIRED"}
    ]

    cust_to_stg = BigQueryInsertJobOperator(
        task_id         = 'customer_to_bq_staging',
        configuration   = extract_to_bq_stgd(
            gcs_file_name   = 'customer',
            table_name      = 'stg_customer',
            schema_fields   = customer_schema,
            partition_field = 'first_purchase_date',
            cluster_fields  = ["customer_state"]            
        ),
        location = "US"
    )
    # ---------------------------------------------------------------
    # 3. Customer Make Dim BQ
    cust_create_dim = BigQueryInsertJobOperator(
        task_id         = 'customer_create_dim',
        configuration   = {
            "query"     : {
                "query" : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.dim_customer`
                    (
                    customer_id STRING NOT NULL,
                    customer_unique_id STRING,
                    customer_zip_code_prefix INT64,
                    customer_city STRING,
                    customer_state STRING,
                    first_purchase_date DATETIME
                    )
                    PARTITION BY DATE(first_purchase_date)
                    CLUSTER BY customer_state;
            """,
            "useLegacySql" : False
            }
        }, location="US"
    )

    # 4. Customer Load to Dim BQ
    cust_to_dim = BigQueryInsertJobOperator(
        task_id         = 'customer_to_bq_dim',
        configuration   = {
            "query"     : {
                "query" : """
                    MERGE `purwadika.jcdeol3_finpro_rido.dim_customer` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_customer` s
                    ON t.customer_id = s.customer_id

                    WHEN NOT MATCHED THEN
                    INSERT (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state, first_purchase_date)
                    VALUES (s.customer_id, s.customer_unique_id, s.customer_zip_code_prefix, s.customer_city, s.customer_state, s.first_purchase_date);
            """,
            "useLegacySql" : False
            }
        }, location="US"
    )

    # ----------------------------------- D. Order Table -------------------------------------
    # 1. Order Extract to GCS Task
    def transform_order_df(df):
            df['order_purchase_timestamp']       = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')
            df['order_approved_at']              = pd.to_datetime(df['order_approved_at'], errors='coerce')
            df['order_delivered_carrier_date']   = pd.to_datetime(df['order_delivered_carrier_date'], errors='coerce')
            df['order_delivered_customer_date']  = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')
            df['order_estimated_delivery_date']  = pd.to_datetime(df['order_estimated_delivery_date'], errors='coerce')

            df['order_purchase_timestamp']       = df['order_purchase_timestamp'].astype('datetime64[us]')
            df['order_approved_at']              = df['order_approved_at'].astype('datetime64[us]')
            df['order_delivered_carrier_date']   = df['order_delivered_carrier_date'].astype('datetime64[us]')
            df['order_delivered_customer_date']  = df['order_delivered_customer_date'].astype('datetime64[us]')
            df['order_estimated_delivery_date']  = df['order_estimated_delivery_date'].astype('datetime64[us]')

            return df 
    
    order_to_gcs = PythonOperator(
        task_id         = 'order_to_gcs',
        python_callable = extract_to_gcs_inc,
        op_kwargs       = {'table': 'orders',
                           'col_inc': 'order_purchase_timestamp',
                           'transform' : transform_order_df}
    )

    # 2. Order Load to Staging BQ
    order_schema = [
    {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_status", "type": "STRING", "mode": "NULLABLE"},
    {"name": "order_purchase_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "order_approved_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "order_delivered_carrier_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "order_delivered_customer_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "order_estimated_delivery_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "payment_type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "payment_installments", "type": "FLOAT64", "mode": "NULLABLE"}
    ]

    order_to_stg = BigQueryInsertJobOperator(
        task_id         = 'order_to_bq_staging',
        trigger_rule    = 'none_failed',
        configuration   = extract_to_bq_stg(
            gcs_file_name   = 'orders',
            table_name      = 'stg_orders',
            schema_fields   = order_schema,
            partition_field = 'order_purchase_timestamp',
            cluster_fields  = ["payment_type"],
            write_disposition= "WRITE_APPEND"
        ),
        location = "US"
    )

    # 3. Order Make Fact BQ
    order_create_fact    = BigQueryInsertJobOperator(
        task_id             = 'orders_create_fact',
        trigger_rule        = 'none_failed',
        configuration       = {
            "query"         : {
                "query"     : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.fact_order`
                    (
                    order_id STRING NOT NULL,
                    customer_id STRING,
                    order_status STRING,
                    order_purchase_timestamp TIMESTAMP NOT NULL,
                    order_approved_at TIMESTAMP,
                    order_delivered_carrier_date TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP,
                    payment_type STRING,
                    payment_installments FLOAT64
                    )
                    PARTITION BY DATE(order_purchase_timestamp)
                    CLUSTER BY payment_type;
                """,
                "useLegacySql" : False
            }
        }, location = "US"
    )

    # 4. Order Load to Fact BQ
    order_to_fact    = BigQueryInsertJobOperator(
        task_id             = 'orders_to_fact',
        trigger_rule    = 'none_failed',
        configuration       = {
            "query"         : {
                "query"     : """
                    MERGE `purwadika.jcdeol3_finpro_rido.fact_order` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_orders` s
                    ON t.order_id = s.order_id

                    WHEN NOT MATCHED THEN
                    INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, payment_type, payment_installments)
                    VALUES (s.order_id, s.customer_id, s.order_status, s.order_purchase_timestamp, s.order_approved_at, s.order_delivered_carrier_date, s.order_delivered_customer_date, s.order_estimated_delivery_date, s.payment_type, s.payment_installments);
                """,
                "useLegacySql" : False 
            }
        }, location = "US"
    )

    # ----------------------------------- E. Order FOP Table ---------------------------------

    # 1. Order_FOP to GCS
    order_fop_to_gcs = PythonOperator(
        task_id         = 'order_fop_to_gcs',
        trigger_rule    = 'always',
        python_callable = extract_to_gcs_inc,
        op_kwargs       = {'table': 'order_fop',
                           'col_inc': 'order_purchase_timestamp',
                           'transform' : transform_order_df}
    )

    # 2. Order_FOP Load to Staging BQ
    order_fop_to_stg = BigQueryInsertJobOperator(
        task_id         = 'order_fop_to_bq_staging',
        trigger_rule    = 'always',
        configuration   = extract_to_bq_stg(
            gcs_file_name   = 'order_fop',
            table_name      = 'stg_order_fop',
            schema_fields   = order_schema,
            partition_field = 'order_purchase_timestamp',
            cluster_fields  = ["payment_type","order_status"],
            write_disposition= "WRITE_APPEND"
        ),
        location = "US"
    )

    # 3. Order_FOP Make Fact BQ
    order_fop_create_fact    = BigQueryInsertJobOperator(
        task_id             = 'order_fop_create_fact',
        trigger_rule    = 'always',
        configuration       = {
            "query"         : {
                "query"     : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.fact_order_fop`
                    (
                    order_id STRING NOT NULL,
                    customer_id STRING,
                    order_status STRING,
                    order_purchase_timestamp TIMESTAMP NOT NULL,
                    order_approved_at TIMESTAMP,
                    order_delivered_carrier_date TIMESTAMP,
                    order_delivered_customer_date TIMESTAMP,
                    order_estimated_delivery_date TIMESTAMP,
                    payment_type STRING,
                    payment_installments FLOAT64
                    )
                    PARTITION BY DATE(order_purchase_timestamp)
                    CLUSTER BY payment_type, order_status;
                """,
                "useLegacySql" : False
            }
        }, location = "US"
    )

    # 4. Order_FOP Load to Fact BQ
    order_fop_to_fact    = BigQueryInsertJobOperator(
        task_id             = 'orders_fop_to_fact',
        trigger_rule    = 'always',
        configuration       = {
            "query"         : {
                "query"     : """
                    MERGE `purwadika.jcdeol3_finpro_rido.fact_order_fop` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_order_fop` s
                    ON t.order_id = s.order_id

                    WHEN NOT MATCHED THEN
                    INSERT (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date, payment_type, payment_installments)
                    VALUES (s.order_id, s.customer_id, s.order_status, s.order_purchase_timestamp, s.order_approved_at, s.order_delivered_carrier_date, s.order_delivered_customer_date, s.order_estimated_delivery_date, s.payment_type, s.payment_installments);
                """,
                "useLegacySql" : False 
            }
        }, location = "US"
    )


    # ----------------------------------- F. Order item Table --------------------------------
    # 1. Order Item Extract to GCS (CSV)

    def transform_item_df(df):
        df['order_item_id'] = pd.to_numeric(df['order_item_id'], errors='coerce').astype(float)
        df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
        df['order_purchase_timestamp'] = pd.to_datetime(df['order_purchase_timestamp'], errors='coerce')
        df['order_purchase_timestamp'] = df['order_purchase_timestamp'].astype('datetime64[us]')
        return df

    item_to_gcs = PythonOperator(
        task_id='order_item_to_gcs',
        python_callable=extract_to_gcs_inc_csv, 
        op_kwargs={
            'table': 'order_item',
            'col_inc': 'order_purchase_timestamp',
            'transform': transform_item_df
        }
    )

    # 2. Item Load to Staging BQ (CSV)

    order_item_schema = [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "order_item_id", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "seller_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "shipping_limit_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "price", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "freight_value", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "order_purchase_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"}
    ]

    item_to_stg = BigQueryInsertJobOperator(
        task_id='order_item_to_bq_staging',
        trigger_rule    = 'none_failed',
        configuration=extract_to_bq_stg_csv(  
            gcs_file_name='order_item',
            table_name='stg_order_item',
            schema_fields=order_item_schema,
            partition_field='order_purchase_timestamp',
            write_disposition="WRITE_APPEND"
        ),
        location="US"
    )

    # 3. Item Make Fact BQ
    item_create_fact    = BigQueryInsertJobOperator(
        task_id             = 'orders_item_create_fact',
        trigger_rule    = 'none_failed',
        configuration       = {
            "query"         : {
                "query"     : """
                    CREATE TABLE IF NOT EXISTS `purwadika.jcdeol3_finpro_rido.fact_order_item`
                    (
                    order_id STRING NOT NULL,
                    order_item_id FLOAT64,
                    product_id STRING NOT NULL,
                    seller_id STRING,
                    shipping_limit_date TIMESTAMP,
                    price FLOAT64,
                    freight_value FLOAT64,
                    order_purchase_timestamp TIMESTAMP NOT NULL
                    )
                    PARTITION BY DATE(order_purchase_timestamp);
                """,
                "useLegacySql" : False
            }
        }, location = "US"
    )

    # 4. Item Load to Fact BQ
    item_to_fact    = BigQueryInsertJobOperator(
        task_id             = 'orders_item_to_fact',
        trigger_rule    = 'none_failed',
        configuration       = {
            "query"         : {
                "query"     : """
                    MERGE `purwadika.jcdeol3_finpro_rido.fact_order_item` t
                    USING `purwadika.jcdeol3_finpro_rido.stg_order_item` s
                    ON t.order_id = s.order_id
                    AND t.product_id = s.product_id
                    AND t.seller_id = s.seller_id
                    AND t.order_purchase_timestamp = s.order_purchase_timestamp 

                    WHEN NOT MATCHED THEN 
                    INSERT (
                    order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value, order_purchase_timestamp)
                    VALUES (
                    s.order_id, s.order_item_id, s.product_id, s.seller_id, s.shipping_limit_date, s.price, s.freight_value, s.order_purchase_timestamp)
                """,
                "useLegacySql" : False 
            }
        }, location = "US"
    )
    # ----------------------------------- G. Mart ---------------------------------
    # 1. Mart Activity
    MART_TRANSACTIONS_TABLE_FULL_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.mart_user_activity`"

    
    FACT_ORDER_ITEM_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_item`"
    FACT_ORDER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order`"
    FACT_ORDER_FOP_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_fop`"
    DIM_CUSTOMER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_customer`"
    DIM_PRODUCT_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_product`"
    DIM_SELLER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_seller`"

    mart_user_activity = BigQueryInsertJobOperator(
        task_id='create_mart_user_activity',
        configuration={
            "query": {
                "query": f"""
                    -- This statement creates or completely overwrites the table 'mart_user_activity'
                    CREATE OR REPLACE TABLE {MART_TRANSACTIONS_TABLE_FULL_PATH}
                    PARTITION BY DATE(order_purchase_timestamp)
                    CLUSTER BY customer_state, seller_state, product_category_name_english AS
                    SELECT 
                        oi.order_id,
                        p.product_category_name_english, 
                        oi.order_item_id,
                        oi.price,
                        oi.freight_value,
                        o.order_status,
                        o.payment_type,
                        o.payment_installments,
                        oi.order_purchase_timestamp,
                        oi.shipping_limit_date,
                        c.customer_id,
                        c.customer_unique_id, 
                        c.customer_zip_code_prefix,
                        c.customer_state,
                        s.seller_id,
                        s.seller_zip_code_prefix,
                        s.seller_state, 
                        o.order_estimated_delivery_date,
                        o.order_delivered_carrier_date,
                        o.order_delivered_customer_date
                    FROM 
                        {FACT_ORDER_ITEM_PATH} AS oi
                    LEFT JOIN 
                        (SELECT * FROM {FACT_ORDER_PATH} UNION ALL SELECT * FROM {FACT_ORDER_FOP_PATH}) AS o
                    ON oi.order_id = o.order_id
                    LEFT JOIN 
                        {DIM_CUSTOMER_PATH} AS c 
                    ON o.customer_id = c.customer_id
                    LEFT JOIN 
                        {DIM_PRODUCT_PATH} AS p 
                    ON oi.product_id = p.product_id
                    LEFT JOIN 
                        {DIM_SELLER_PATH} AS s  
                    ON oi.seller_id = s.seller_id;
                """,
                "useLegacySql": False,
            }
        },
        location="US", 
        trigger_rule='always' 
    ) 

    # 2. Product Performances
    MART_PROD_PERFORMANCE_TABLE_FULL_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.mart_product_performances`"

    FACT_ORDER_ITEM_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_item`"
    FACT_ORDER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order`"
    FACT_ORDER_FOP_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_fop`"
    DIM_CUSTOMER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_customer`"
    DIM_PRODUCT_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_product`"
    DIM_SELLER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_seller`"
    mart_product_performance = BigQueryInsertJobOperator(
        task_id='create_mart_product_performances',
        trigger_rule='always',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE {MART_PROD_PERFORMANCE_TABLE_FULL_PATH} 
                    PARTITION BY order_date
                    CLUSTER BY product_category_name_english AS
                    SELECT 
                        p.product_id,
                        p.product_category_name_english,
                        COUNT(DISTINCT s.seller_id) AS seller_count,
                        COUNT(DISTINCT c.customer_unique_id) AS unique_customer_count,
                        COUNT(oi.order_item_id) AS total_units_sold,
                        COUNT(DISTINCT oi.order_id) AS total_orders,
                        SUM(oi.price) AS total_revenue,
                        ROUND(SAFE_DIVIDE(SUM(oi.price), COUNT(oi.order_item_id)), 2) AS avg_revenue_per_unit,
                        ROUND(SAFE_DIVIDE(SUM(oi.price), COUNT(DISTINCT c.customer_unique_id)), 2) AS avg_revenue_per_customer,
                        SUM(oi.freight_value) AS total_shipping_cost,
                        AVG(oi.freight_value) AS avg_shipping_cost,
                        AVG(oi.price) AS avg_price,
                        MIN(oi.price) AS min_price,
                        MAX(oi.price) AS max_price,
                        SAFE_DIVIDE(COUNT(oi.order_item_id) * 1.0, COUNT(DISTINCT oi.order_id)) AS avg_items_per_order,
                        DATE(o.order_purchase_timestamp) AS order_date
                    FROM {FACT_ORDER_ITEM_PATH} oi
                    LEFT JOIN {FACT_ORDER_PATH} o ON oi.order_id = o.order_id
                    LEFT JOIN {DIM_CUSTOMER_PATH} c ON o.customer_id = c.customer_id
                    LEFT JOIN {DIM_PRODUCT_PATH} p ON oi.product_id = p.product_id
                    LEFT JOIN {DIM_SELLER_PATH} s ON oi.seller_id = s.seller_id
                    WHERE o.order_status = 'delivered'
                    GROUP BY p.product_id, p.product_category_name_english, DATE(o.order_purchase_timestamp)
                """,
                "useLegacySql": False
            }
        },
        location='US',
        )

    # 3. Mart Customer Characteristics
    MART_CC_TABLE_FULL_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.mart_customer_characteristics`"
    FACT_ORDER_ITEM_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_item`"
    FACT_ORDER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order`"
    FACT_ORDER_FOP_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.fact_order_fop`"
    DIM_CUSTOMER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_customer`"
    DIM_PRODUCT_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_product`"
    DIM_SELLER_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.dim_seller`"
    mart_customer_character = BigQueryInsertJobOperator(
        task_id='create_mart_customer_character',
        trigger_rule='always',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE {MART_CC_TABLE_FULL_PATH} 
                    PARTITION BY order_month
                    CLUSTER BY customer_state AS
                    SELECT
                        c.customer_unique_id,
                        c.customer_city,
                        c.customer_state,
                        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
                        COUNT(DISTINCT o.order_id) AS total_orders,
                        COUNT(oi.order_item_id) AS total_items_bought,
                        SUM(oi.price) AS total_revenue,
                        ROUND(SAFE_DIVIDE(SUM(oi.price), COUNT(DISTINCT o.order_id)), 2) AS avg_revenue_per_order,
                        MIN(o.order_purchase_timestamp) AS first_purchase_date,
                        MAX(o.order_purchase_timestamp) AS last_purchase_date
                    FROM {DIM_CUSTOMER_PATH} c
                    LEFT JOIN {FACT_ORDER_PATH} o ON c.customer_id = o.customer_id
                    LEFT JOIN {FACT_ORDER_ITEM_PATH} oi ON o.order_id = oi.order_id
                    WHERE o.order_status = 'delivered'
                        AND o.order_purchase_timestamp IS NOT NULL
                    GROUP BY
                        c.customer_unique_id,
                        c.customer_city,
                        c.customer_state,
                        order_month 
                """,
                "useLegacySql": False,
            }
        },
        location="US"
        )

    # 4. Mart Order FOP
    MART_FOP_TABLE_FULL_PATH = f"`{PROJECT_ID}.{BQ_DATASET}.mart_lost_revenue`"

    mart_order_fop = BigQueryInsertJobOperator(
        task_id='create_mart_order_fop',
        trigger_rule='always',
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE {MART_FOP_TABLE_FULL_PATH} 
                    PARTITION BY order_month
                    CLUSTER BY order_status AS
                    SELECT
                        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
                        o.order_status,
                        COUNT(o.order_id) AS total_fop_orders,
                        COUNT(DISTINCT o.order_id) AS total_unique_fop_orders,
                        SUM(i.order_item_id) AS total_fop_items,
                        SUM(i.price) AS total_est_lost_revenue,
                        SUM(i.freight_value) AS total_est_lost_shipping_cost
                    FROM {FACT_ORDER_FOP_PATH} o
                    LEFT JOIN {FACT_ORDER_ITEM_PATH} i
                    ON o.order_id = i.order_id
                    GROUP BY order_month, order_status
                """,
                "useLegacySql": False,
            }
        },
        location="US"
        )

  

    # ----------------------------------- H. Slack --------------------------------
    slack_success = SlackWebhookOperator(
    task_id='slack_notify_success',
    slack_webhook_conn_id=SLACK_CONN_ID,
    message="""
:white_check_mark: *Airflow DAG Success!*
All tasks completed successfully.
""",
    trigger_rule='none_failed')



    # --- Steps ---
    dim_and_fact_tasks = [
    seller_to_dim,
    prod_to_dim,
    cust_to_dim,
    order_to_fact,
    order_fop_to_fact,
    item_to_fact
]

    seller_to_gcs   >> seller_to_stg    >> seller_to_dim
    prod_to_gcs     >> prod_to_stg      >> prod_to_dim
    cust_to_gcs     >> cust_to_stg      >> cust_create_dim      >> cust_to_dim
    order_to_gcs    >> order_to_stg     >> order_create_fact    >> order_to_fact
    order_fop_to_gcs>> order_fop_to_stg >> order_fop_create_fact>> order_fop_to_fact
    item_to_gcs     >> item_to_stg      >> item_create_fact     >> item_to_fact

    dim_and_fact_tasks >> mart_user_activity >> mart_order_fop >> mart_customer_character >> mart_product_performance >> slack_success
