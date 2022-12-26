DROP TABLE IF EXISTS ANLOKHANOVYANDEXRU__DWH.global_metrics;
CREATE TABLE ANLOKHANOVYANDEXRU__DWH.global_metrics (
     date_update timestamp NOT NULL,
     currency_from int NOT NULL,
     amount_total numeric(15, 3) NULL,
     cnt_transactions int NULL,
     avg_transactions_per_account int NULL,
     cnt_accounts_make_transactions numeric(15, 3) NULL)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES KSAFE 1;