import time
import requests
import json
import pandas as pd
import psycopg2
import pendulum
import boto3
import vertica_python

from getpass import getpass
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import psycopg
from psycopg import Connection

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



def inserting_data(pg_sql:str,pg_params:str, vert_insert:str, vert_params:dict)->None:
    with psycopg.connect(pg_params) as pg_conn:
        cur = pg_conn.cursor()
        cur.execute(pg_sql)
        result = cur.fetchall()
        with vertica_python.connect(**vert_params) as vert_conn:
            index_in = 0
            index_out = 5000
            while True:
                value = result[index_in:index_out]
                value = ', '.join(map(str, value))
                сur_vert = vert_conn.cursor()
                сur_vert.execute(vert_insert.format(values=value))
                if index_out>len(result):
                    break
                else:
                    index_in += 5000
                    index_out += 5000           
                    if index_out%100000==0:
                        print(index_out,"True")



pg_select_currencies = f"""select 
                                date_update::varchar, 
                                currency_code, 
                                currency_code_with, 
                                currency_with_div::varchar                 
                          from public.currencies
                          where date_update>{currencies_max_dt}"""

pg_select_transactions = f"""select 
                                  operation_id, 
                                  account_number_from, 
                                  account_number_to, 
                                  currency_code, 
                                  country, status, 
                                  transaction_type, 
                                  amount, 
                                  transaction_dt::varchar 
                            from public.transactions
                            where transaction_dt>{transactions_max_dt}"""

vert_insert_currencies = """INSERT INTO ANLOKHANOVYANDEXRU__STAGING.currencies values{values}"""
vert_insert_transactions = """INSERT INTO ANLOKHANOVYANDEXRU__STAGING.transactions values{values}"""

with DAG('data_from_source_to_stg',
    schedule_interval='0 24 * * *',
    start_date=pendulum.datetime(2022, 8, 15, tz="UTC"),
    tags=['final_project']

) as dag:

    from_stg_to_dwh_currencies = PythonOperator(
                                    task_id='from_stg_to_dwh_currencies',
                                    python_callable=inserting_data,
                                    op_kwargs={'pg_sql': pg_select_currencies, 
                                            'pg_params': pg_params,
                                            'vert_insert': vert_insert_currencies,
                                            'vert_params': vert_params}
                                )   
    from_stg_to_dwh_transactions = PythonOperator(
                                    task_id='from_stg_to_dwh_transactions',
                                    python_callable=inserting_data,
                                    op_kwargs={'pg_sql': pg_select_transactions, 
                                            'pg_params': pg_params,
                                            'vert_insert': vert_insert_transactions,
                                            'vert_params': vert_params}
                                )   

    [from_stg_to_dwh_currencies,from_stg_to_dwh_transactions]
