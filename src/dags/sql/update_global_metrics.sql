MERGE INTO STV2024031238__DWH.global_metrics gm
USING ((
	with tr as (
	Select distinct t.operation_id, t.account_number_from, t.account_number_to, t.currency_code , t.transaction_dt,	t.amount, isnull(c.currency_with_div,1) as currency_with_div
	FROM STV2024031238__STAGING.transactions t left JOIN STV2024031238__STAGING.currencies c
	ON  t.currency_code = c.currency_code AND t.transaction_dt::DATE = c.date_update and c.currency_code_with = 420
	WHERE t.transaction_dt::DATE = '{{ yesterday_ds }}' and account_number_from >=0 and account_number_to >=0 and t.status = 'done')
	SELECT
	    transaction_dt::DATE AS date_update,
	    currency_code AS currency_from,
	    SUM(amount * currency_with_div) AS amount_total,
	    COUNT(DISTINCT operation_id) AS cnt_transactions,
	    COUNT(DISTINCT operation_id)/COUNT(DISTINCT account_number_from) AS avg_transactions_per_account,
	    COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
	 from tr
	 GROUP BY
	    transaction_dt::DATE, currency_code) 
 ) as dayly_view
ON gm.date_update = dayly_view.date_update AND gm.currency_from=dayly_view.currency_from
WHEN MATCHED THEN UPDATE
SET amount_total = dayly_view.amount_total,
	cnt_transactions = dayly_view.cnt_transactions,
	avg_transactions_per_account = dayly_view.avg_transactions_per_account,
	cnt_accounts_make_transactions = dayly_view.cnt_accounts_make_transactions
WHEN NOT MATCHED THEN
INSERT (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
VALUES (dayly_view.date_update, dayly_view.currency_from, dayly_view.amount_total,
		dayly_view.cnt_transactions, dayly_view.avg_transactions_per_account, dayly_view.cnt_accounts_make_transactions);

