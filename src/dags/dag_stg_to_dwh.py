
import pandas as pd
import psycopg2
import pendulum
import boto3
import vertica_python


from getpass import getpass
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

import psycopg
from psycopg import Connection


postgres_conn_id = 'postgresql_de'

pg_user = Variable.get("pg_user")
pg_password = Variable.get("pg_password")
vert_user = Variable.get("vert_user")
vert_password = Variable.get("vert_password")

pg_params = f"""
            host=rc1b-w5d285tmxa8jimyn.mdb.yandexcloud.net
            port=6432
            dbname=db1
            user={pg_user}
            password={pg_password}
            target_session_attrs=read-write
            sslmode=require"""

vert_params = {
    'host': '51.250.75.20',
    'port': 5433,
    'user': vert_user,      
    'password': vert_password, 
    'database': 'dwh',
    'autocommit': True}


def get_data(vert_insert:str, vert_params:dict)->None:
    with vertica_python.connect(**vert_params) as vert_conn:
            сur_vert = vert_conn.cursor()
            сur_vert.execute(vert_insert)
            result = сur_vert.fetchone()
            return(result[0])
            
vert_get_currencies = """select max(date_update)::varchar              
                          from ANLOKHANOVYANDEXRU__STAGING.currencies"""
vert_get_transactions = """select max(transaction_dt)::varchar              
                          from ANLOKHANOVYANDEXRU__STAGING.transactions"""

currencies_max_dt = get_data(vert_get_currencies,vert_params)
transactions_max_dt = get_data(vert_get_transactions,vert_params)                          


with DAG('data_from_stg_to_dwh',
    schedule_interval='0 24 * * *',
    start_date=pendulum.datetime(2022, 8, 15, tz="UTC"),
    tags=['final_project']

) as dag:

 from_stg_to_dwh = PostgresOperator(
        task_id='from_stg_to_dwh',
        postgres_conn_id=postgres_conn_id,
        sql="sql/dwh_insert.sql",
        params={"currencies_max_dt": currencies_max_dt,
                "transactions_max_dt": transactions_max_dt})