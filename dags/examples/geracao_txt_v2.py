from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'alexandre.yoshimatsu',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}




with DAG(
    default_args=default_args,
    dag_id='geracao_txt_b3_v2',
    description='Geração txt da B3',
    start_date=datetime(2023, 3, 1),
    schedule_interval='@daily'
) as dag:
    
    @task(task_id="consultar_operacoes_dia")
    def consultar_operacoes_dia(**kwargs):
        
        # Obtendo Conexao
        #conn = BaseHook.get_connection("connzzzz")
        
        # Obtendo Variavel
        #var = Variable.get("varrrrr")
        
        operacoes_dia = ['xyz', 'abc', 'dfg', 'hij', 'opq']
        
        kwargs["ti"].xcom_push(key="operacoes_dia", value=operacoes_dia)
    
    task_consultar_operacoes_dia = consultar_operacoes_dia()
                
  
    @task(task_id="consultar_pu_galaxia")
    def consultar_pu_galaxia(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_consultar_pu_galaxia = consultar_pu_galaxia()

    
    
    @task(task_id=f"enviar_email_analistas")
    def enviar_email_analistas(**kwargs):
        operacoes_dia = kwargs["ti"].xcom_pull(key="operacoes_dia")
        print(operacoes_dia)
        
    task_enviar_email_analistas = enviar_email_analistas()
    
    
    task_consultar_operacoes_dia >> task_consultar_pu_galaxia >> task_enviar_email_analistas