from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook


import logging

log = logging.getLogger(__name__)


def update_global_metrics(sql_path, **kwargs):
    try:
        log.info('Start global_metrics update')
        yesterday_ds = str(kwargs['yesterday_ds'])
        with open(sql_path) as f:
            sql = f.read()

        sql = sql.replace('{{ yesterday_ds }}', yesterday_ds)

        with VerticaHook(vertica_conn_id='vertica_conn').get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                conn.commit()
    except Exception as e:
        log.error(f"Error executing SQL: {e}")
        raise

    log.info('Update is done')


with DAG(
        dag_id='cdm_loader',
        start_date=datetime(2022, 10, 1),
        end_date=datetime(2022, 11, 1),
        catchup = True,
        schedule_interval='@daily'
) as dag:

    update_global_metrics = PythonOperator(
        task_id='update_global_metrics',
        python_callable=update_global_metrics,
        op_kwargs={'sql_path': '/lessons/dags/sql/update_global_metrics.sql'},
        provide_context=True,
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    (
            update_global_metrics
            >> end
    )
