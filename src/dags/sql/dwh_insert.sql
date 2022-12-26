insert into  ANLOKHANOVYANDEXRU__DWH.global_metrics (
with joined_tab  as (
select t.currency_code, currency_code_with, currency_with_div, account_number_from, t.date_day,	
	   case 
			when t.currency_code='420' then amount
			when t.currency_code!='420' then amount*currency_with_div 
	  end as 'amount_usd'
from (
select *, DATE_TRUNC('day', transaction_dt)::date as date_day
from ANLOKHANOVYANDEXRU__STAGING.transactions
where status='done') as t
left join (select currency_code, currency_code_with, currency_with_div, date_update::date as date_day
		   from ANLOKHANOVYANDEXRU__STAGING.currencies) as c 
on t.currency_code=c.currency_code and t.date_day=c.date_day
where transaction_dt>{{params.transactions_max_dt}} and date_update>{{params.currencies_max_dt}})

select date_day, 
	   currency_code,
	   sum(amount_usd),
	   count(*) as cnt_transactions,
	   round(avg(avg_account),2) as avg_transactions_per_account,
	   count(distinct account_number_from) as cnt_accounts_make_transactions
from (
select *, avg(amount_usd) over (partition by date_day, currency_code,account_number_from) as  avg_account
from joined_tab) as t
group by date_day,
		 currency_code
)




