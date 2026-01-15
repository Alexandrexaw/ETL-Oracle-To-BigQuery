# -*- coding: utf-8 -*-
"""
# ==============================================================================
# SCRIPT ID     : oracle_to_bigquery_etl.py
# FUNCTION      : Extração de dados Oracle (ERP) para o Google BigQuery.
# AUTHOR        : Alexandre Alvino
# VERSION       : 2.1
# DESCRIPTION   : Pipeline de dados via Airflow (Cloud Composer) que realiza a 
#                 carga incremental/full via logs e tratamento de datas legacy.
# ==============================================================================
"""

import logging
import pendulum
import google.cloud.logging
import oracledb as db
from datetime import datetime, timedelta
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
from airflow.models import Variable
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
import pandas as pd
import json
import platform

# Configurações de DAG (Generalizadas)
DAG_NAME = 'ETL_ORACLE_TO_BIGQUERY_STAGING'
SCHEDULE_INTERVAL = "30 20 * * *"

default_args = {
    'owner': 'Data-Engineering',
    'retries': 1,
    'retry_delay': timedelta(seconds=180)
}

# Configuração de Logging Cloud
client_log = google.cloud.logging.Client()
client_log.setup_logging()

# Recuperação de parâmetros via Airflow Variables
# O JSON esperado na variável deve seguir o contrato de infraestrutura definido
parm = Variable.get("PAR_ETL_CONFIG", deserialize_json=True)

project_id = parm['conf']['project_id']
dataset_id = parm['conf']['dataset_id']

credenciais = {}

def busca_senha(token, cofre_type):
    """
    Realiza a busca de credenciais no cofre de senhas
    """
    logging.info(f"Buscando credenciais para: {cofre_type}")
    
    # Construção dinâmica de IDs de variáveis para a API do cofre
    identifier = parm[cofre_type]['identifier'].replace("/", "%2F")
    
    if cofre_type == 'cofre_oracle':
        fields = ['username', 'password', 'address', 'port', 'servicename']
    else:
        fields = ['username', 'password']

    variable_ids = ",".join([f"{identifier}%2F{field}" for field in fields])
    url = f"{parm[cofre_type]['get_url']}{variable_ids}"

    try:
        response = requests.get(
            url=url, 
            headers={'Authorization': f'Token token="{token}"'}, 
            verify=False
        )
        response.raise_for_status()
        credenciais[cofre_type] = response.json()
    except requests.exceptions.HTTPError as err:
        logging.error(f"Erro na requisição ao cofre: {repr(err)}")
        sys.exit(1)

def acessa_cofre(cofre_type):
    """
    Realiza a autenticação inicial no cofre de senhas para obter o token de acesso.
    """
    logging.info(f"Autenticando no cofre: {cofre_type}")
    try:
        response = requests.post(
            url=parm[cofre_type]['post_url'], 
            headers={'Accept-Encoding': 'base64'},
            data=parm[cofre_type]['key'], 
            verify=False
        )
        response.raise_for_status()
        token = response.text
        busca_senha(token, cofre_type)
    except Exception as err:
        logging.error(f"Falha na autenticação do cofre: {err}")
        sys.exit(1)

def conecta_banco(banco_key):
    """
    Estabelece conexão com o Oracle Database utilizando python-oracledb.
    """
    acessa_cofre('cofre_oracle')
    
    base_id = parm['cofre_oracle']['identifier']
    creds = credenciais['cofre_oracle']

    # Extração de parâmetros injetados pelo cofre
    params = db.ConnectParams(
        host=creds[f"{base_id}/address"],
        port=creds[f"{base_id}/port"],
        service_name=creds[f"{base_id}/servicename"]
    )

    try:
        conn = db.connect(
            user=creds[f"{base_id}/username"],
            password=creds[f"{base_id}/password"],
            params=params
        )
        return conn
    except db.DatabaseError as e:
        logging.error(f"Erro de conexão Oracle: {e}")
        sys.exit(1)

def carga_rm2bq_v2(bq_client, credentials, param_tab):
    """
    Lógica principal de ETL => Extrai do Oracle em blocos e carregar no BQ
    """
    block_size = 500000
    conn = conecta_banco('oracle_source')
    
    tabela = param_tab
    query = f"SELECT * FROM {tabela}"
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        fields = [row[0] for row in cursor.description]
        column_dtypes = [row[1] for row in cursor.description]

        rows = cursor.fetchmany(block_size)
        block_count = 0
        total_rows = 0

        while rows:
            df = pd.DataFrame(rows, columns=fields)
            block_count += 1
            total_rows += len(rows)
            
            # Tratamento de Regra de Negócio: Datas inválidas para o BigQuery (>2200)
            for i, dtype in enumerate(column_dtypes):
                if dtype == db.DB_TYPE_DATE:
                    col_name = fields[i]
                    if df[col_name].dtype == object:
                        df[col_name] = df[col_name].apply(
                            lambda x: x if x is None or (isinstance(x, str) and int(x.split('-')[0]) <= 2200) 
                            else '2199-12-31 00:00:00'
                        )
                        df[col_name] = pd.to_datetime(df[col_name])

            # Definição do destino (Mapeia View de Origem para Tabela de Staging)
            dest_table_name = tabela.replace("VW", "STG", 1).lower()
            table_id = f"{project_id}.{dataset_id}.{dest_table_name}"

            # Se for o primeiro bloco, sobrescreve. Os demais, anexa.
            write_disposition = "replace" if block_count == 1 else "append"
            df.to_gbq(table_id, project_id=project_id, credentials=credentials, if_exists=write_disposition)
            
            logging.info(f"Lote {block_count} processado: {len(rows)} linhas.")
            rows = cursor.fetchmany(block_size)

    except Exception as e:
        logging.error(f"Erro no processamento da tabela {tabela}: {e}")
        raise
    finally:
        conn.close()

def create_dynamic_tasks_etl(task_group):
    """
    Lê metadados do Oracle para gerar tasks dinâmicas no Airflow.
    """
    conn = conecta_banco('oracle_source')
    
    query = """
        SELECT object_name FROM all_objects 
        WHERE object_type = 'VIEW' 
        AND owner = 'DATA_OWNER'
        AND status = 'VALID'
        AND object_name LIKE 'VW_EXT_DA_%'
    """
    cursor = conn.cursor()
    cursor.execute(query)
    tabelas = cursor.fetchall()
    conn.close()

    previous_task = None
    for row in tabelas:
        tab_name = row[0]
        task = PythonOperator(
            task_id=f"sync_{tab_name}",
            task_group=task_group,
            python_callable=process_table_wrapper,
            op_kwargs={'param_tab': tab_name},
            trigger_rule="all_done"
        )
        
        # Sequenciamento das tasks para não sobrecarregar o banco de origem
        if previous_task:
            previous_task >> task
        previous_task = task

def process_table_wrapper(param_tab):
    """
    Wrapper para inicializar clientes GCP e disparar o ETL.
    """
    acessa_cofre('cofre_bq')
    credentials = service_account.Credentials.from_service_account_info(credenciais['cofre_bq'])
    bq_client = bigquery.Client(credentials=credentials, project=project_id)
    carga_rm2bq_v2(bq_client, credentials, param_tab)

# Definição da DAG
with DAG(
    DAG_NAME,
    tags=["ETL", "ORACLE", "BIGQUERY"],
    start_date=pendulum.datetime(2025, 1, 1, tz='America/Sao_Paulo'),
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    max_active_runs=1
) as dag:

    begin = DummyOperator(task_id="begin")
    
    with TaskGroup(group_id='dynamic_etl_group') as etl_group:
        create_dynamic_tasks_etl(etl_group)

    end = DummyOperator(task_id="end")

    chain(begin, etl_group, end)