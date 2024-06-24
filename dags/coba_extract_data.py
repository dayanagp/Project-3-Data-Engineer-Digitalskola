from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import psycopg2
import pandas as pd
import requests
import io
import logging
import snowflake.connector

# Daftar URL file CSV
csv_files = [
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/categories.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/customers.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/employees.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/orders.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/regions.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/shippers.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/suppliers.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/order_details.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/territories.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/employee_territories.csv",
    "https://raw.githubusercontent.com/captDzuL/graphql-compose-examples/master/examples/northwind/data/csv/products.csv"
]

logging.basicConfig(level=logging.DEBUG)

def drop_existing_tables():
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    for url in csv_files:
        table_name = url.split('/')[-1].replace('.csv', '')
        drop_table_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE;"
        cursor.execute(drop_table_sql)
    
    drop_table_sql = f"DROP TABLE IF EXISTS supplier_gross_revenue CASCADE;"
    cursor.execute(drop_table_sql)
    drop_table_sql = f"DROP TABLE IF EXISTS category_top_sales CASCADE;"
    cursor.execute(drop_table_sql)
    drop_table_sql = f"DROP TABLE IF EXISTS top_emp_rev CASCADE;"
    cursor.execute(drop_table_sql)

    connection.commit()
    cursor.close()
    connection.close()

def download_and_clean_csv(url):
    response = requests.get(url)
    response.raise_for_status()

    df = pd.read_csv(io.StringIO(response.text))
    df.columns = df.columns.str.lower()
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    logging.info(f"Data shape after dropna and cleaning: {df.shape}")

    date_columns = ['orderdate', 'requireddate', 'shippeddate']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    logging.info(f"Data shape after processing date columns: {df.shape}")

    return df

def create_table_and_insert_data(df, table_name):
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'DOUBLE PRECISION',
        'object': 'TEXT',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP'
    }

    columns = ', '.join([f"{col} {dtype_mapping[str(df[col].dtype)]}" for col in df.columns])
    create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns});"
    cursor.execute(create_table_sql)

    for _, row in df.iterrows():
        values = ', '.join([
            'NULL' if pd.isna(val) else f"'{str(val).replace("'", "''")}'" if isinstance(val, str) or isinstance(val, pd.Timestamp) else str(val)
            for val in row
        ])
        insert_sql = f"INSERT INTO {table_name} VALUES ({values});"
        cursor.execute(insert_sql)

    connection.commit()
    cursor.close()
    connection.close()
    logging.info(f"Inserted data into table {table_name}")

def download_clean_and_insert(url):
    table_name = url.split('/')[-1].replace('.csv', '')
    df = download_and_clean_csv(url)
    create_table_and_insert_data(df, table_name)

def create_gross_revenue_data_mart():
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    create_data_mart_sql = """
    CREATE TABLE IF NOT EXISTS supplier_gross_revenue AS
    SELECT 
        s.supplierid,
        s.companyname,
        TO_CHAR(DATE_TRUNC('month', o.orderdate), 'YYYY-MM') AS month_order,
        SUM((od.unitprice - (od.unitprice * od.discount)) * od.quantity) AS gross_revenue
    FROM 
        orders o
    JOIN 
        order_details od ON o.orderid = od.orderid
    JOIN 
        products p ON od.productid = p.productid
    JOIN 
        suppliers s ON p.supplierid = s.supplierid
    GROUP BY 
        s.supplierid, s.companyname, TO_CHAR(DATE_TRUNC('month', o.orderdate), 'YYYY-MM');
    """
    cursor.execute(create_data_mart_sql)
    connection.commit()

    cursor.close()
    connection.close()

def create_top_sales_data_mart():
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    create_data_mart_sql = """
    CREATE TABLE IF NOT EXISTS category_top_sales AS
    WITH category_sales AS (
    SELECT
        c.CATEGORYNAME AS category_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM(od.QUANTITY) AS total_sold
    FROM
        order_details od
        JOIN orders o ON od.ORDERID = o.ORDERID
        JOIN products p ON od.PRODUCTID = p.PRODUCTID
        JOIN categories c ON p.CATEGORYID = c.CATEGORYID
    GROUP BY
        category_name, month_order
    ),
    ranked_categories AS (
    SELECT
        category_name,
        month_order,
        total_sold,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY total_sold DESC) AS rank
    FROM
        category_sales
    )

    SELECT
    category_name,
    month_order,
    total_sold
    FROM
    ranked_categories
    WHERE
    rank = 1
    """
    cursor.execute(create_data_mart_sql)
    connection.commit()

    cursor.close()
    connection.close()

def create_top_emp_rev_data_mart():
    postgres_hook = PostgresHook(postgres_conn_id='airflow_db_conn')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()

    create_data_mart_sql = """
    CREATE TABLE IF NOT EXISTS top_emp_rev AS
    WITH employee_revenue AS (
    SELECT
        e.FIRSTNAME || ' ' || e.LASTNAME AS employee_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
    FROM
        order_details od
        JOIN orders o ON od.ORDERID = o.ORDERID
        JOIN employees e ON o.EMPLOYEEID = e.EMPLOYEEID
    GROUP BY
        employee_name, month_order
    ),
    ranked_employees AS (
    SELECT
        employee_name,
        month_order,
        gross_revenue,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY gross_revenue DESC) AS rank
    FROM
        employee_revenue
    )
    SELECT
    employee_name,
    month_order,
    gross_revenue
    FROM
    ranked_employees
    WHERE
    rank = 1
        """
    cursor.execute(create_data_mart_sql)
    connection.commit()

    cursor.close()
    connection.close()

def extract_data_from_postgresql(query):
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="172.31.176.1",
        port="5434"
    )
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def load_data_to_snowflake(df, table_name, schema):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user='dzulfdz',
        password='Tarun@STPI20',
        account='ir03211.ap-southeast-3.aws',
        database='PROJECT3_AIRFLOW',
        role='ACCOUNTADMIN'
    )
    cursor = conn.cursor()
    
    # Create schema if not exists
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    # Create table if not exists (assumes DataFrame columns are named appropriately)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
        {", ".join([f"{col} {pd_dtype_to_snowflake_dtype(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])}
    );
    """
    cursor.execute(create_table_sql)
    
    # Insert data into Snowflake table
    for i, row in df.iterrows():
        values = ", ".join([f"'{str(val).replace("'", "''")}'" if isinstance(val, str) else str(val) for val in row])
        insert_sql = f"INSERT INTO {schema}.{table_name} VALUES ({values});"
        cursor.execute(insert_sql)
    
    conn.commit()
    cursor.close()
    conn.close()

def pd_dtype_to_snowflake_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"
    
def load_data_mart_to_snowflake():
    supplier_gross_revenue_query = "SELECT * FROM supplier_gross_revenue;"
    supplier_gross_revenue_df = extract_data_from_postgresql(supplier_gross_revenue_query)
    load_data_to_snowflake(supplier_gross_revenue_df, "supplier_gross_revenue", "project")

    category_top_sales_query = "SELECT * FROM category_top_sales;"
    category_top_sales_df = extract_data_from_postgresql(category_top_sales_query)
    load_data_to_snowflake(category_top_sales_df, "category_top_sales", "project")

    top_emp_rev_query = "SELECT * FROM public.top_emp_rev;"
    top_emp_rev_df = extract_data_from_postgresql(top_emp_rev_query)
    load_data_to_snowflake(top_emp_rev_df, "top_emp_rev", "project")


default_args = {
    'owner': 'dzulfdz',
    'start_date': datetime(2024, 6, 6),
    'retries': 1
}

dag = DAG(
    'data_warehouse_and_data_mart',
    default_args=default_args,
    description='DAG to download, clean, validate CSV files, insert into PostgreSQL and create data marts',
    schedule_interval='@daily',
    max_active_runs=1,
    catchup=False
)

# Task to drop existing tables
drop_tables_task = PythonOperator(
    task_id='drop_existing_tables',
    python_callable=drop_existing_tables,
    dag=dag
)

previous_task = drop_tables_task

for i, url in enumerate(csv_files):
    task = PythonOperator(
        task_id=f'download_clean_and_insert_{i}',
        python_callable=download_clean_and_insert,
        op_args=[url],
        dag=dag
    )

    previous_task >> task
    previous_task = task

# Task to create the gross revenue data mart
create_gross_revenue_task = PythonOperator(
    task_id='create_gross_revenue_data_mart',
    python_callable=create_gross_revenue_data_mart,
    dag=dag
)

# Task to create the top sales per category
create_top_sales_task = PythonOperator(
    task_id='create_top_sales_data_mart',
    python_callable=create_top_sales_data_mart,
    dag=dag
)

# Task to create the top employee revenue
create_top_emp_rev_task = PythonOperator(
    task_id='create_top_emp_rev_data_mart',
    python_callable=create_top_emp_rev_data_mart,
    dag=dag
)

load_data_mart_task = PythonOperator(
    task_id='load_data_mart_to_snowflake',
    python_callable=load_data_mart_to_snowflake,
    dag=dag
)

previous_task >> create_gross_revenue_task >> create_top_sales_task >> create_top_emp_rev_task >> load_data_mart_task
