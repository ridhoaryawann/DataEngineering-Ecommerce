from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os

import pandas as pd

# variables
conn_ids        = "postgres" 
schema_name     = "ecommerce"
data_path       = "/opt/airflow/datas"

# A. Function to create schemas postgres
def create_schema(conn_id):
    schema_name         = "ecommerce"
    create_schema_sql   = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    try:
        hook    = PostgresHook(postgres_conn_id = conn_id)
        engine  = hook.get_sqlalchemy_engine()

        with engine.begin() as connection:
            connection.execute(create_schema_sql)

            logging.info(f"Schema {schema_name} created succesffuly")

    except Exception as e:
        logging.error(f"Failed to create {schema_name}:{e} ")
        raise 

# B. Function to create table
def create_table(conn_id, sql_statement):

    try:
        hook    = PostgresHook(postgres_conn_id = conn_id)
        engine  = hook.get_sqlalchemy_engine()

        with engine.begin() as connection:
            connection.execute(sql_statement)

            logging.info(f"Table created succesffuly")

    except Exception as e:
        logging.error(f"Failed to create table:{e} ")
        raise 

# B.1 Orders DB
create_order_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.orders (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR(100),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,        
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    payment_type VARCHAR(100),
    payment_installments FLOAT
);
"""

# B.2. Orders Failed or Pending
create_order_fop_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.order_fop (
    order_id VARCHAR,
    customer_id VARCHAR,
    order_status VARCHAR(100),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,        
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    payment_type VARCHAR(100),
    payment_installments FLOAT
);
"""

# B.3. Order Item
create_order_item_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.order_item(
    order_id VARCHAR,
    order_item_id VARCHAR,
    product_id VARCHAR,
    seller_id VARCHAR,
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    order_purchase_timestamp TIMESTAMP
);
"""

# B.4. Customer 
create_customer_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.customer (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix INTEGER ,
    customer_city VARCHAR(100),
    customer_state VARCHAR(20),
    first_purchase_date TIMESTAMP
);
"""

# B.5. Product
create_product_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.product (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR ,
    product_category_name_english VARCHAR ,
    product_name_lenght FLOAT,
    product_description_lenght FLOAT,
    product_photos_qty FLOAT,
    product_weight_g FLOAT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT
);
"""

# B.6. Seller
create_seller_pgdb = f"""
CREATE TABLE IF NOT EXISTS {schema_name}.seller (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state VARCHAR(25)
);
"""

# C. Function to transform data

# C.1. Order
def transform_order_df(x):
    x['order_purchase_timestamp']       = pd.to_datetime(x['order_purchase_timestamp'])
    x['order_approved_at']              = pd.to_datetime(x['order_approved_at'])
    x['order_delivered_carrier_date']   = pd.to_datetime(x['order_delivered_carrier_date'])
    x['order_delivered_customer_date']  = pd.to_datetime(x['order_delivered_customer_date'])
    x['order_estimated_delivery_date']  = pd.to_datetime(x['order_estimated_delivery_date'])
    x['payment_installments']           = x['payment_installments'].astype(float)
    return x

# C.2. Order_fop
def transform_order_fop_df(x):
    x['order_purchase_timestamp']       = pd.to_datetime(x['order_purchase_timestamp'])
    x['order_approved_at']              = pd.to_datetime(x['order_approved_at'])
    x['order_delivered_carrier_date']   = pd.to_datetime(x['order_delivered_carrier_date'])
    x['order_delivered_customer_date']  = pd.to_datetime(x['order_delivered_customer_date'])
    x['order_estimated_delivery_date']  = pd.to_datetime(x['order_estimated_delivery_date'])
    x['payment_installments']           = x['payment_installments'].astype(float)
    return x

# C.3. Order_item
def transform_item_df(x):
    x['order_item_id']              = x['order_item_id'].astype(float)
    x['shipping_limit_date']        = pd.to_datetime(x['shipping_limit_date'])
    x['price']                      = x['price'].astype(float)
    x['freight_value']              = x['freight_value'].astype(float)
    x['order_purchase_timestamp']   = pd.to_datetime(x['order_purchase_timestamp'])
    return x

# C.4. Customer
def transform_customer_df(x):
    x['customer_zip_code_prefix']   = x['customer_zip_code_prefix'].astype(int)
    x['first_purchase_date']        = pd.to_datetime(x['first_purchase_date'])
    return x

# C.5. Product
def transform_product_df(x):
    x['product_name_lenght']         =  x['product_name_lenght'].astype(float)
    x['product_description_lenght']  =  x['product_description_lenght'].astype(float)
    x['product_photos_qty']          =  x['product_photos_qty'].astype(float)
    x['product_weight_g']            =  x['product_weight_g'].astype(float)
    x['product_length_cm']           =  x['product_length_cm'].astype(float)
    x['product_height_cm']           =  x['product_height_cm'].astype(float)
    x['product_width_cm']            =  x['product_width_cm'].astype(float)
    return x

# C.6. Seller
def transform_seller_df(x):
    x['seller_zip_code_prefix'] =   x['seller_zip_code_prefix'].astype(int)
    return x

# D. Function to insert data
def insert_data_pgdb(conn_id, file_name, table_name, transform_func):
    try:
        file_path   = os.path.join(data_path, file_name)
        x           = pd.read_csv(file_path)

        x           = transform_func(x)

        hook        = PostgresHook(postgres_conn_id=conn_id)
        engine      = hook.get_sqlalchemy_engine()

        x.to_sql(table_name, con=engine, schema= schema_name, if_exists='append', index=False)

        logging.info(f"{table_name} data inserted succesfully")

    except Exception as e:
        logging.error(f"error inserting {table_name}: {e}")
        raise



# ------------ DAG SET UP --------------------
default_args    = {
    'owner'             : 'airflow',
    'start_date'        : datetime(2025,6,10,3),
    'retries'           : 1,
    'retry_delay'       : timedelta(seconds=10)
}

with DAG(
    dag_id              =   'raws_to_postgres',
    default_args        =   default_args,
    description         =   'import raw data to postgres',
    schedule_interval   =   '@daily',
    catchup             =   False
) as dag:
    
    # create schema
    create_schema_task  = PythonOperator(
        task_id         = 'create_schema_postgres',
        python_callable = create_schema,
        op_kwargs       = {
            'conn_id'   : 'postgres',
        }
    )

    # 1. create table order
    create_order_table  = PythonOperator(
        task_id         = 'create_order_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_order_pgdb
        }
    )

    # 2. create table order_fob
    create_order_fob_table  = PythonOperator(
        task_id         = 'create_order_fop_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_order_fop_pgdb
        }
    )

    # 3. create table order_item
    create_order_item_table = PythonOperator(
        task_id         = 'create_order_item_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_order_item_pgdb
        }
    )

    # 4. create table customer
    create_customer_table = PythonOperator(
        task_id         = 'create_customer_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_customer_pgdb
        }
    )

    # 5. create table product
    create_product_table = PythonOperator(
        task_id         = 'create_product_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_product_pgdb
        }
    )

    # 6. create table seller
    create_seller_table = PythonOperator(
        task_id         = 'create_seller_db_postgres',
        python_callable = create_table,
        op_kwargs       = {
            'conn_id'       : 'postgres',
            'sql_statement' : create_seller_pgdb
        }
    )

    # ---------- transform & insert task ----------

    # 1. insert order
    insert_order_task = PythonOperator(
        task_id='insert_order_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_order.csv',
            'table_name': 'orders',
            'transform_func': transform_order_df
        }
    ) 

    # 2. insert order fob
    insert_order_fop_task = PythonOperator(
        task_id='insert_order_fob_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_order_failed_or_pending.csv',
            'table_name': 'order_fop',
            'transform_func': transform_order_fop_df
        }
    )

    # 3. insert order item
    insert_order_item_task = PythonOperator(
        task_id='insert_order_item_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_item.csv',
            'table_name': 'order_item',
            'transform_func': transform_item_df
        }
    )

    # 4. insert customer
    insert_customer_task = PythonOperator(
        task_id='insert_customer_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_customer.csv',
            'table_name': 'customer',
            'transform_func': transform_customer_df
        }
    )

    # 5. insert product
    insert_product_task = PythonOperator(
        task_id='insert_product_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_product.csv',
            'table_name': 'product',
            'transform_func': transform_product_df
        }
    )

    # 6. insert seller
    insert_seller_task = PythonOperator(
        task_id='insert_seller_pgdb',
        python_callable=insert_data_pgdb,
        op_kwargs={
            'conn_id': 'postgres',
            'file_name': 'raw_seller.csv',
            'table_name': 'seller',
            'transform_func': transform_seller_df
        }
    )

    create_schema_task >> create_order_table >> insert_order_task 
    create_schema_task >> create_order_fob_table >> insert_order_fop_task 
    create_schema_task >> create_order_item_table >> insert_order_item_task
    create_schema_task >> create_customer_table >> insert_customer_task 
    create_schema_task >> create_product_table >> insert_product_task
    create_schema_task >> create_seller_table >> insert_seller_task
