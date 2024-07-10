from airflow import DAG
from datetime import datetime
from airflow.decorators import task

from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models.variable import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonVirtualenvOperator
import sys
from pathlib import Path
# Chemin que vous souhaitez ajouter
custom_path = '/opt/airflow'

if custom_path not in sys.path:
    sys.path.append(custom_path)
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG,  DBT_EXECUTION_CONFIG 
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig,  ExecutionConfig




def _store_data():
    try:
        hook = PostgresHook(postgres_conn_id='postgres_2')
        csv_filepath = '/opt/airflow/include/dataset/online_retail.csv'

        # Spécification explicite des colonnes dans la commande COPY
        copy_sql = """
        COPY sales(Invoice, StockCode, Description, Quantity, InvoiceDate, Price, CustomerID, Country)
        FROM stdin WITH DELIMITER ',' CSV HEADER
        """

        # Exécuter la commande COPY avec open() pour lire le fichier
        with open(csv_filepath, 'r') as f:
            hook.copy_expert(sql=copy_sql, filename=csv_filepath)

    except Exception as e:
        print(f"Error: {e}")

with DAG(
    dag_id="store_data_from_kaggle",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
 
    
 create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_2',
    sql='''
    CREATE TABLE  IF NOT EXISTS sales (
        Invoice VARCHAR(20),
        StockCode VARCHAR(20),
        Description TEXT,
        Quantity INTEGER,
        InvoiceDate TIMESTAMP,
        Price DECIMAL(10, 2),
        CustomerID NUMERIC,
        Country VARCHAR(100)
    );
    ''')
 store_data = PythonOperator(
    task_id='store_data',
    python_callable=_store_data,
     )
 
 create_table_country = PostgresOperator(
        task_id="create_table_country",
        postgres_conn_id="postgres_2",
        sql='creation_table_country.sql')

 @task.external_python(python ='/opt/airflow/soda_venv/bin/python')
 def check_load():
    PROJECT_ROOT = "/opt/airflow/include"
    custom_path = '/opt/airflow'

    if custom_path not in sys.path:
        sys.path.append(custom_path)
    from include.soda.check_function import run_soda_scan


    
    
    a = run_soda_scan(project_root = PROJECT_ROOT, scan_name ="checks_ingest", checks_subpath = "sources/checks.yml")
 check_load =check_load() 
 transform_data = DbtTaskGroup(
        group_id="transform",
        project_config=DBT_PROJECT_CONFIG,
        
        profile_config=DBT_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,

        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        ))
 transform_data

 @task.external_python(python ='/opt/airflow/soda_venv/bin/python')
 def check_transform():
    PROJECT_ROOT = "/opt/airflow/include"
    
    custom_path = '/opt/airflow'

    if custom_path not in sys.path:
        sys.path.append(custom_path)
    from include.soda.check_function import run_soda_scan


    a = run_soda_scan(project_root = PROJECT_ROOT, scan_name ="checks_transform", checks_subpath = "transform")
 check_transform =check_transform() 


 report = DbtTaskGroup(
        group_id="report",
        project_config=DBT_PROJECT_CONFIG,
        
        profile_config=DBT_CONFIG,
        execution_config=DBT_EXECUTION_CONFIG,

        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        ))
 report



 @task.external_python(python ='/opt/airflow/soda_venv/bin/python')
 def check_report():
    PROJECT_ROOT = "/opt/airflow/include"
    custom_path = '/opt/airflow'

    if custom_path not in sys.path:
        sys.path.append(custom_path)
    from include.soda.check_function import run_soda_scan
    
    a = run_soda_scan(project_root = PROJECT_ROOT, scan_name ="checks_report", checks_subpath = "report")
 check_report =check_report() 

 

 

create_table >> store_data >> check_load >>  create_table_country >>  transform_data >> check_transform >> report  >> check_report

