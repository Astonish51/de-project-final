DROP TABLE IF EXISTS ANLOKHANOVYANDEXRU__STAGING.transactions;
CREATE TABLE ANLOKHANOVYANDEXRU__STAGING.transactions ( operation_id varchar(60) NULL,
    account_number_from int NULL,
    account_number_to int NULL,
    currency_code int NULL,
    country varchar(30) NULL,
    status varchar(30) NULL,
    transaction_type varchar(30) NULL,
    amount int NULL,
    transaction_dt timestamp NULL);
    
CREATE PROJECTION  ANLOKHANOVYANDEXRU__STAGING.transactions_1
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT *
 FROM ANLOKHANOVYANDEXRU__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES KSAFE 1;