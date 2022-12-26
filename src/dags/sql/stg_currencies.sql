DROP TABLE IF EXISTS ANLOKHANOVYANDEXRU__STAGING.currencies;
CREATE TABLE ANLOKHANOVYANDEXRU__STAGING.currencies ( date_update timestamp NOT NULL,
currency_code int NOT NULL,
currency_code_with int NOT NULL,
currency_with_div numeric(5,3) NOT NULL);

CREATE PROJECTION  ANLOKHANOVYANDEXRU__STAGING.currencies_1
(
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div
)
AS
 SELECT *
 FROM ANLOKHANOVYANDEXRU__STAGING.currencies
ORDER BY date_update, currency_code
SEGMENTED BY hash(date_update, currency_code) ALL NODES KSAFE 1;