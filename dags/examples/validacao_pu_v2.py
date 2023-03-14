from datetime import datetime, timedelta
import os
from requests import get
from dataclasses import dataclass

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

from airflow.hooks.base_hook import BaseHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import Variable
from airflow.utils.email import send_email

default_args = {
    'owner': 'alexandre.yoshimatsu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dataclass
class Operacao:
    agente_fid: str
    nome_op: str
    dia_pagamento: datetime
    num_serie: int
    codigo_if: str
    validado_motor: bool
    nm_emissor: str


with DAG(
    default_args=default_args,
    dag_id='validacao_pu_v2',
    description='Validação de PU',
    start_date=datetime(2023, 3, 1),
    schedule_interval='@daily'
) as dag:    
    
    @task(task_id="consultar_operacoes_dia")
    def consultar_operacoes_dia(**kwargs):
        
        # Obtendo Conexao
        #conn = BaseHook.get_connection("connzzzz")
        
        # Obtendo Variavel
        #var = Variable.get("varrrrr")
        
        operacoes_dia = []
        operacoes_dia.append(
            Operacao(
                'VORTX SERVICOS FIDUCIARIOS LTDA.',
                'HABITAT QUARESMEIRA',
                '2023-03-20',
                193,
                '21C0822819',
                1,
                'Virgo Companhia de Securitização',
                )
        )
        
        operacoes_dia.append(
            Operacao(
                'PENTAGONO S A DTVM',
                'CONFIDERE',
                '2023-03-21',
                6,
                '12L0022128',
                1,
                'Virgo Companhia de Securitização',
                )
        )
        
        operacoes_dia.append(
            Operacao(
                'OLIVEIRA TRUST DISTRIBUIDORA DE TITULOS E VALORES MOBILIARIO',
                'CORP BELENUS',
                '2023-03-17',
                489,
                '22D1226341',
                1,
                'Virgo Companhia de Securitização',
                )
        )
        
        kwargs["ti"].xcom_push(key="operacoes_dia", value=operacoes_dia)
    
    task_consultar_operacoes_dia = consultar_operacoes_dia()
                
  
    @task(task_id="consultar_pu_galaxia")
    def consultar_pu_galaxia(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_consultar_pu_galaxia = consultar_pu_galaxia()
    
    
    @task(task_id=f"consultar_pu_externo")
    def consultar_pu_externo(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_consultar_pu_externo = consultar_pu_externo()
    
    
    @task(task_id=f"batimento_com_AF")
    def batimento_com_AF(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_batimento_com_AF = batimento_com_AF()
    
    
    @task(task_id=f"enviar_email_analistas")
    def enviar_email_analistas(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_enviar_email_analistas = enviar_email_analistas()
    
    
    task_consultar_operacoes_dia >> [task_consultar_pu_galaxia, task_consultar_pu_externo] >> task_batimento_com_AF >> task_enviar_email_analistas