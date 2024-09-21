CREATE TABLE IF NOT EXISTS STV2024031238__DWH.global_metrics (	

	date_update date NOT NULL,
	currency_from int NOT NULL,
	amount_total int NOT NULL,
	cnt_transactions int NOT NULL,
    	avg_transactions_per_account float NOT NULL,
    	cnt_accounts_make_transactions int NOT NULL
)
order by currency_from, date_update
SEGMENTED BY hash(currency_from, date_update) all nodes
PARTITION BY date_update;
