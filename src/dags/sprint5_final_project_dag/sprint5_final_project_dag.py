import requests
import json
import psycopg
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.base import BaseHook

http_conn_id = HttpHook.get_connection('delivers_api')
postgres_conn_id = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
NICKNAME = 'SodoiUral'
COHORT = '6'
API_KEY = http_conn_id.password

headers = {'X-Nickname': NICKNAME, 'X-Cohort': COHORT, 'X-API-KEY': API_KEY}

host = postgres_conn_id.host
port = postgres_conn_id.port
dbname = postgres_conn_id.schema
user = postgres_conn_id.login
password = postgres_conn_id.password


def get_api_couriers_to_stg():
    # Используем логику из предыдущих заданий: psycopg и его метод connection
    # с целью выполнить весь код как одну транзакцию
    conn = psycopg.connect(host=host,
                           port=port,
                           dbname=dbname,
                           user=user,
                           password=password,
                           sslmode='require')
        with conn.cursor() as cur:
            # устанавливаем offset в ноль для следующей загрузки            
            drop_offset = {'last_offset': 0}
            drop_offset = json.dumps(drop_offset)
            cur.execute("""UPDATE stg.srv_wf_settings
                            SET workflow_settings = %(drop_offset)s
                            WHERE workflow_key = 'api_couriers_to_stg_worflow'
                            """,
                            {
                                "drop_offset": drop_offset
                            })
            now_date = datetime.now()
            data = ['start']
            while len(data) > 0:
            # в соответствии с комментариями к задаче прохоимся циклом                        
                cur.execute('''SELECT sws.workflow_settings 
                                FROM stg.srv_wf_settings sws 
                                WHERE sws.workflow_key = 'api_couriers_to_stg_worflow'
                            ''')
                offset = cur.fetchone()[0]['last_offset']
                url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?offset={offset}'
                response = requests.get(url, headers=headers)
                data = list(json.loads(response.content))
                new_offset = {'last_offset': len(data) + offset}
                new_offset_json = json.dumps(new_offset)
                # Выход из цикла, если данные закончились
                if len(data) == 0:
                    break
                for row in data:
                    cur.execute(
                        """
                            INSERT INTO stg.api_couriers(object_value, update_ts)
                            VALUES (%(object_value)s, %(update_ts)s)
                            ON CONFLICT (object_value) DO UPDATE
                            SET
                                update_ts = EXCLUDED.update_ts                   
                        """,
                        {
                            "object_value": str(row),
                            "update_ts": now_date
                        })
                cur.execute("""UPDATE stg.srv_wf_settings
                            SET workflow_settings = %(new_offset)s
                            WHERE workflow_key = 'api_couriers_to_stg_worflow'
                            """,
                            {
                                "new_offset": new_offset_json
                            })

    conn.close


def get_api_deliveries_to_stg():
    # Используем логику из предыдущих заданий: psycopg и его метод connection
    # с целью выполнить весь код как одну транзакцию
    conn = psycopg.connect(host=host,
                           port=port,
                           dbname=dbname,
                           user=user,
                           password=password,
                           sslmode='require')
    # указываю дату, т.к. в задаче требует последние 7 дней
    from_dt = '2025-06-20 00:00:00'
    with conn:
        with conn.cursor() as cur:
            # устанавливаем offset в ноль для следующей загрузки
            drop_offset = {'last_offset': 0}
            drop_offset = json.dumps(drop_offset)
            cur.execute("""UPDATE stg.srv_wf_settings
                            SET workflow_settings = %(drop_offset)s
                            WHERE workflow_key = 'api_deliveries_to_stg_worflow'
                            """,
                            {
                                "drop_offset": drop_offset
                            })

            now_date = datetime.now()
            data = ['start']
            # в соответствии с комментариями к задаче прохоимся циклом
            while len(data) > 0:
                cur.execute('''SELECT sws.workflow_settings 
                                FROM stg.srv_wf_settings sws 
                                WHERE sws.workflow_key = 'api_deliveries_to_stg_worflow'
                            ''')
                offset = cur.fetchone()[0]['last_offset']
                url = f'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?offset={offset}&from={from_dt}&sort_field=date'
                response = requests.get(url, headers=headers)
                data = list(json.loads(response.content))
                new_offset = {'last_offset': len(data) + offset}
                new_offset_json = json.dumps(new_offset)
                # Выход из цикла, если данные закончились
                if len(data) == 0:
                    break
                for row in data:
                    cur.execute(
                        """
                            INSERT INTO stg.api_deliveries(object_value, update_ts)
                            VALUES (%(object_value)s, %(update_ts)s)
                            ON CONFLICT (object_value) DO UPDATE
                            SET
                                update_ts = EXCLUDED.update_ts                   
                        """,
                        {
                            "object_value": str(row),
                            "update_ts": now_date
                        })
                cur.execute("""UPDATE stg.srv_wf_settings
                            SET workflow_settings = %(new_offset)s
                            WHERE workflow_key = 'api_deliveries_to_stg_worflow'
                            """,
                            {
                                "new_offset": new_offset_json
                            })
    conn.close


def stg_couriers_to_dds():
    conn = psycopg.connect(host=host,
                           port=port,
                           dbname=dbname,
                           user=user,
                           password=password,
                           sslmode='require')
    with conn:
        with conn.cursor() as cur:
            # Иницируем последнюю дату обновления DDS
            cur.execute("""
                        SELECT TO_timestamp(workflow_settings ->> 'last_update_ts', 'YYYY-MM-DD HH24:MI:SS')
                        FROM dds.srv_wf_settings
                        WHERE workflow_key = 'stg_couriers_to_dds_worflow'
                        """)
            last_update = cur.fetchone()[0]
            last_update_str = last_update.strftime("%Y-%m-%d %H:%M:%S")
            # ИЗ STG выгружаем данные свежее послденго обновления DDS
            cur.execute("""
                        SELECT 
                            REPLACE(ac.object_value, '''', '"')::JSON ->> '_id' AS courier_id,
                            REPLACE(ac.object_value, '''', '"')::JSON ->> 'name' AS courier_name
                        FROM stg.api_couriers ac
                        WHERE ac.update_ts > %(last_update_str)s
                        """,
                        {
                            "last_update_str": last_update_str
                        })
            data = objs = cur.fetchall()
            # Записываем в DDS
            for row in data:
                cur.execute(
                    """
                        INSERT INTO dds.dm_couriers(courier_id, courier_name)
                        VALUES (%(courier_id)s, %(courier_name)s)
                        ON CONFLICT (courier_id) DO UPDATE
                        SET
                            courier_name = EXCLUDED.courier_name                   
                    """,
                    {
                        "courier_id": row[0],
                        "courier_name": row[1]
                    })
            # Выгружаем последнюю дату из STG
            cur.execute("""
                        SELECT TO_CHAR(max(update_ts), 'YYYY-MM-DD HH24:MM:SS')
                        FROM stg.api_couriers
                        """)
            new_update = cur.fetchone()[0]
            new_update_dict = {}
            new_update_dict['last_update_ts'] = new_update
            new_update_json_str = json.dumps(new_update_dict)
            # Записываем ее как дату обновления DDS
            cur.execute("""
                        UPDATE dds.srv_wf_settings SET
                        workflow_settings = %(new_update_json_str)s
                        WHERE workflow_key = 'stg_couriers_to_dds_worflow'
                        """,
                        {
                            "new_update_json_str": new_update_json_str
                        })
            
def stg_deliveries_to_dds():
    conn = psycopg.connect(host=host,
                           port=port,
                           dbname=dbname,
                           user=user,
                           password=password,
                           sslmode='require')
    with conn:
        with conn.cursor() as cur:
            # Иницируем последнюю дату обновления DDS
            cur.execute("""
                        SELECT TO_timestamp(workflow_settings ->> 'last_update_ts', 'YYYY-MM-DD HH24:MI:SS')
                        FROM dds.srv_wf_settings
                        WHERE workflow_key = 'stg_deliveries_to_dds_worflow'
                        """)
            last_update = cur.fetchone()[0]
            last_update_str = last_update.strftime("%Y-%m-%d %H:%M:%S")
            # ИЗ STG выгружаем данные свежее послденго обновления DDS
            cur.execute("""
                    WITH pre as(
                    SELECT 
                        REPLACE(object_value, '''', '"')::JSON ->> 'order_id' AS order_key,
                        REPLACE(object_value, '''', '"')::JSON ->> 'delivery_id' AS delivery_id,
                        REPLACE(object_value, '''', '"')::JSON ->> 'courier_id' AS courier_key,
                        REPLACE(object_value, '''', '"')::JSON ->> 'rate' AS rate,
                        REPLACE(object_value, '''', '"')::JSON ->> 'tip_sum' AS tip_sum
                        FROM stg.api_deliveries
                        WHERE update_ts > %(last_update_str)s)
                        SELECT
                            o.id AS order_id,
                            pre.delivery_id,
                            c.id AS courier_id,
                            pre.rate,
                            pre.tip_sum
                        FROM pre
                        JOIN dds.dm_couriers c ON c.courier_id = pre.courier_key
                        JOIN dds.dm_orders o ON o.order_key = pre.order_key 
                        """,
                        {
                            "last_update_str": last_update_str
                        })
            data = objs = cur.fetchall()
            # Записываем в DDS
            for row in data:
                cur.execute(
                    """
                        INSERT INTO dds.dm_deliveries(order_id, delivery_id, courier_id, rate, tip_sum)
                        VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s)
                        ON CONFLICT (order_id) DO UPDATE
                        SET
                            delivery_id = EXCLUDED.delivery_id,
                            courier_id = EXCLUDED.courier_id,
                            rate = EXCLUDED.rate,
                            tip_sum = EXCLUDED.tip_sum;                        
                    """,
                    {
                        "order_id": row[0],
                        "delivery_id": row[1],
                        "courier_id": row[2],
                        "rate": row[3],
                        "tip_sum": row[4]
                    })
            # Выгружаем последнюю дату из STG                
            cur.execute("""
                        SELECT TO_CHAR(max(update_ts), 'YYYY-MM-DD HH24:MM:SS')
                        FROM stg.api_deliveries
                        """)
            new_update = cur.fetchone()[0]
            new_update_dict = {}
            new_update_dict['last_update_ts'] = new_update 
            new_update_json_str = json.dumps(new_update_dict)
            
            # Записываем ее как дату обновления DDS
            cur.execute("""
                        UPDATE dds.srv_wf_settings SET
                        workflow_settings = %(new_update_json_str)s
                        WHERE workflow_key = 'stg_deliveries_to_dds_worflow'
                        """,
                        {
                            "new_update_json_str": new_update_json_str
                        })                                   
    conn.close

with DAG(
        'sprint5_project_stg_delivers',
        schedule_interval='0/15 * * * *',
        description='sprin5_project',
        catchup=False,
        start_date=datetime.now()
) as dag:
    get_api_couriers_to_stg = PythonOperator(
        task_id='get_api_couriers_to_stg',
        python_callable=get_api_couriers_to_stg)

    get_api_deliveries_to_stg = PythonOperator(
        task_id='get_api_deliveries_to_stg',
        python_callable=get_api_deliveries_to_stg)

    stg_couriers_to_dds = PythonOperator(
        task_id='stg_couriers_to_dds',
        python_callable=stg_couriers_to_dds)

    stg_deliveries_to_dds = PythonOperator(
        task_id='stg_deliveries_to_dds',
        python_callable=stg_deliveries_to_dds)
    
    update_dm_courier_ledger = PostgresOperator(
        task_id='update_dm_courier_ledger',
        postgres_conn_id='PG_WAREHOUSE_CONNECTION',
        sql="sql/dm_courier_ledger.sql")    
    
    (
        [get_api_couriers_to_stg, get_api_deliveries_to_stg]
        >> stg_couriers_to_dds
        >> stg_deliveries_to_dds
        >> update_dm_courier_ledger
    )
