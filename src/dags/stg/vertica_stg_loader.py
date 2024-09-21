import logging
import pandas as pd
from airflow.contrib.hooks.vertica_hook import VerticaHook

log = logging.getLogger(__name__)


class VerticaStgLoader:
    def __init__(self) -> None:
        self.log = log

    def load_transactions(self, keys: list, local_path: str) -> None:
        for key in keys:
            if key != 'currencies_history.csv':
                filepath = f"{local_path}/{key}"
                df = pd.read_csv(filepath, delimiter=',')
                num_rows = len(df)
                size = num_rows // 10

                sql = """
                                    COPY STV2024031238__STAGING.transactions
                                    (operation_id, account_number_from,account_number_to,currency_code,country,status,
                                    transaction_type,amount,transaction_dt)
                                    FROM LOCAL '/lessons/dags/data/chunk.csv'                
                                    DELIMITER ',' ENCLOSED BY '"' 
                                    REJECTED DATA AS TABLE STV2024031238__STAGING.transactions_rej;
                              """
                start = 0
                vertica_conn = VerticaHook(vertica_conn_id='vertica_conn').get_conn()
                with vertica_conn as connection:
                    while start <= num_rows:
                        end = min(start + size, num_rows)
                        df.loc[start: end].to_csv('/lessons/dags/data/chunk.csv', index=False, mode='w')
                        with connection.cursor() as cur:
                            cur.execute(sql)

                        start += size + 1

    def load_currencies(self, local_path: str) -> None:
        filepath = f"{local_path}/currencies_history.csv"
        df = pd.read_csv(filepath, delimiter=',')
        num_rows = len(df)
        size = num_rows // 10

        sql = """
                    COPY STV2024031238__STAGING.currencies
                    (currency_code,currency_code_with,date_update,currency_with_div)
                    FROM LOCAL '/lessons/dags/data/chunk.csv'                
                    DELIMITER ',' ENCLOSED BY '"' 
                    REJECTED DATA AS TABLE STV2024031238__STAGING.currencies_rej;
              """
        start = 0
        vertica_conn = VerticaHook(vertica_conn_id='vertica_conn').get_conn()
        with vertica_conn as connection:
            while start <= num_rows:
                end = min(start + size, num_rows)
                df.loc[start: end].to_csv('/lessons/dags/data/chunk.csv', index=False, mode='w')
                with connection.cursor() as cur:
                    cur.execute(sql)

                start += size + 1
