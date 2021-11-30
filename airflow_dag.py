from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'ITversity, Inc'
    'start_date': days_ago(3)
}

dag = DAG(
    dag_id = 'HS_ENERGY_DL',
    default_args = default_args,
    schedule_interval = '00***'
    catchup=False
)

create_processed_data = BashOperator(
    task_id='create_processed_data',
    bash_command='hs_dl',
    dag=dag
)

create_processed_data

if __name__ = "__main__":
    dag_cli()