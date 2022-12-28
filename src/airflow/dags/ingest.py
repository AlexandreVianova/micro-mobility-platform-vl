# Basic python modules
from datetime import timedelta, datetime

# Airflow modules
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id=f'ingest-mobility-data',
    default_args=default_args,
    start_date=datetime.strptime('2017-01-01', '%Y-%m-%d'),
    schedule_interval='*/5 * * * *',
    max_active_runs=1,
    catchup=False,
    tags=['ingest', 'mobility_data', 'bikeshare']
) as dag:
    system = 'capital-bikeshare'
    ingest_task = SimpleHttpOperator(
        task_id=f'ingest_{system}'.replace('-', '_'),
        endpoint=f'ingest/systems/{system}',
        method='GET',
        http_conn_id='http_default',
        log_response=True
    )


if __name__ == '__main__':
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run(start_date=dag.start_date, end_date=dag.start_date, verbose=True)