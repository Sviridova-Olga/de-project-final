CREATE TABLE IF NOT EXISTS STV2024031238__STAGING.currencies (	
	currency_code int NOT NULL,
	currency_code_with int NOT NULL,
	date_update date NOT NULL,
	currency_with_div numeric(19, 2) NULL
)
order by currency_code, date_update
SEGMENTED BY hash(currency_code, date_update) all nodes
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);


CREATE TABLE IF NOT EXISTS STV2024031238__STAGING.transactions (
	operation_id varchar(50) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(50) NULL,
	status varchar(50) NULL,
	transaction_type varchar(50) NULL,
	amount int NULL,
	transaction_dt TIMESTAMP(3) NULL
)
order by operation_id, transaction_dt
SEGMENTED BY hash(operation_id, transaction_dt) all nodes
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);