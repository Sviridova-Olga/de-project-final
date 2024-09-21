from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from stg.s3_loader import S3loader
from stg.vertica_stg_loader import VerticaStgLoader
import logging

log = logging.getLogger(__name__)


def get_s3_key(ti):
    s3 = S3loader(conn_id='s3')
    s3_keys = s3.get_key(bucket_name='final-project')
    log.info('Got s3 keys {}'.format(s3_keys))
    ti.xcom_push(key='s3_keys', value=s3_keys)


def upload_data_from_s3(local_path, ti):
    keys = ti.xcom_pull(key='s3_keys', task_ids='get_s3_key')
    log.info('Start uploading transactions')
    s3 = S3loader(conn_id='s3')
    for key in keys:
        s3.download_from_s3(bucket_name='final-project', key=key, local_path=local_path)
    log.info('Uploading transactions done')


def upload_currencies_to_vertica(local_path, ti):
    log.info('Start loading currencies to vertica')
    vertica = VerticaStgLoader()
    vertica.load_currencies(local_path=local_path)
    log.info('Loading currencies is done')


def upload_transactions_to_vertica(local_path, ti):
    log.info('Start loading transactions to vertica')
    keys = ti.xcom_pull(key='s3_keys', task_ids='get_s3_key')
    vertica = VerticaStgLoader()
    vertica.load_transactions(keys=keys, local_path=local_path)
    log.info('Loading transactions is done')



with DAG(
        dag_id='stg_loader',
        start_date=datetime(2022, 1, 1),
        schedule_interval=None,
) as dag:

    get_s3_key = PythonOperator(
        task_id='get_s3_key',
        python_callable=get_s3_key,
        provide_context=True,
        dag=dag)

    upload_data_from_s3 = PythonOperator(
        task_id='upload_data_from_s3',
        python_callable=upload_data_from_s3,
        op_kwargs={'local_path': '/lessons/dags/data'},
        trigger_rule='all_success',
        dag=dag
    )

    upload_currencies_to_vertica = PythonOperator(
        task_id='upload_currencies_to_vertica',
        python_callable=upload_currencies_to_vertica,
        op_kwargs={'local_path': '/lessons/dags/data'},
        trigger_rule='all_success',
        dag=dag
    )

    upload_transactions_to_vertica = PythonOperator(
        task_id='upload_transactions_to_vertica',
        python_callable=upload_transactions_to_vertica,
        op_kwargs={'local_path': '/lessons/dags/data'},
        trigger_rule='all_success',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag,
    )

    (
            get_s3_key
            >> upload_data_from_s3
            >> upload_currencies_to_vertica
            >> upload_transactions_to_vertica
            >> end
    )
