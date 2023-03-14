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
    dag_id='geracao_txt_b3',
    description='Geração txt da B3',
    start_date=datetime(2023, 3, 1),
    schedule_interval='@daily'
) as dag:
    
    operacoes_dia = ['xyz', 'abc', 'dfg', 'hij', 'opq']
    
    @task(task_id="task_get_operacoes_dia")
    def get_operacoes_dia():
        operacoes_dia.append('def')
    
    operacoes_dia_task = get_operacoes_dia()
    
    
    for operacao in operacoes_dia:
        
        @task(task_id=f"task_print_operacao_{operacao}")
        def print_operacao(operacao):
            print("Operação: {}".format(operacao))
            
        print_operacao_task = print_operacao(operacao)
        
        
        @task(task_id=f"task_calcular_pu_virgo_{operacao}")
        def calcular_pu_virgo(operacao):
            print("Operação: {}".format(operacao))
            
        calcular_pu_virgo_task = calcular_pu_virgo(operacao)
        
        @task(task_id=f"task_enviar_email_{operacao}")
        def enviar_email(operacao):
            print("Operação: {}".format(operacao))
            
        enviar_email_task = enviar_email(operacao)
        
        
        operacoes_dia_task >> print_operacao_task >> calcular_pu_virgo_task >> enviar_email_task