# Basic python modules
from datetime import timedelta, datetime

# Airflow modules
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id=f'ingest-mobility-data',
    default_args=default_args,
    start_date=datetime.strptime('2022-12-30', '%Y-%m-%d'),
    schedule_interval='*/5 * * * *',
    max_active_runs=1,
    catchup=False,
    tags=['ingest', 'mobility_data', 'bikeshare']
) as dag:

    # Define ingestions to perform
    ingestions = []
    systems = ['velib', 'capital-bikeshare']
    for system in systems:
        ingestions.append(SimpleHttpOperator(
            task_id=f'ingest_{system}'.replace('-', '_'),
            endpoint=f'ingest',
            method='GET',
            http_conn_id='http_default',
            log_response=True,
            response_filter=lambda response: response.json(),
            data={
                'system_name': system
            })
        )

    fan_in = EmptyOperator(task_id='fan_in')

    ingestions >> fan_in

if __name__ == '__main__':
    from airflow.utils.state import State

    dag.clear(dag_run_state=State.NONE)
    dag.run(start_date=dag.start_date, end_date=dag.start_date, verbose=True)
