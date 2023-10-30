import datetime
import os
from airflow import DAG

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator

dag = DAG('create_tables_dag',
          description='Drop and Create tables in Redshift using airflow',
          schedule_interval=None,
          start_date=datetime.datetime(2023, 2, 1, 0, 0, 0, 0)
        )

drop_tables_task = PostgresOperator(
    task_id="drop_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="drop_tables.sql"
)

create_tables_task = PostgresOperator(
    task_id="create_tables_task",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

drop_tables_task >> create_tables_task