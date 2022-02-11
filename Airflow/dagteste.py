from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from importando import importando_zip
from transformaçoes import tratamento_dados
from banco import criando_banco

##DEFININDO ARGUMENTOS BÁSICOS##
default_args = {
    'owner': 'Fernanda_Castro',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['fecaastro_@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

##CRIANDO DAG NO AIRFLOW##
with DAG(
        dag_id='pagseguro',
        default_args=default_args,
        tags=['airflow', 'mysql' 'csv'],
        description='gravar no SQL local via DAG airflow'
) as dag:
    t1 = PythonOperator(
        task_id='banco_de_dados',
        python_callable=criando_banco,
    )
    t2 = PythonOperator(
        task_id='importando_csv',
        python_callable=importando_zip,
    )
    t3 = PythonOperator(
        task_id='inserido_csv',
        python_callable=tratamento_dados,
    )

    t1 >> t2 >> t3
