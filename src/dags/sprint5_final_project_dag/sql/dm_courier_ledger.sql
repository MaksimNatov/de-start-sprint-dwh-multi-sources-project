--Заполняем витрину 
-- В условиях задачи сказано за последнюю неделю, беру даты от '2025-06-20 00:00:00'

INSERT INTO cdm.dm_courier_ledger (courier_id,
courier_name,
settlement_year,
settlement_month,
orders_count,
orders_total_sum,
rate_avg,
order_processing_fee,
courier_order_sum,
courier_tips_sum,
courier_reward_sum)
WITH ts as(
	SELECT
		id,
		"month",
		"year"
	FROM dds.dm_timestamps
	WHERE ts >= to_date('2025-06-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
),
ord as(
	SELECT 
		o.id,
		ts.month,
		ts.year,
		SUM(fps.total_sum) AS total_sum
	FROM dds.dm_orders o
	JOIN ts ON o.timestamp_id = ts.id
	JOIN dds.fct_product_sales fps ON fps.order_id = o.id
	WHERE o.order_status = 'CLOSED'
	GROUP BY 1, 2, 3
),
pay as(
	SELECT
		d.courier_id,
		dn.courier_name,
		ord.YEAR AS settlement_year,
		ord.MONTH AS settlement_month,
		(ord.id),
		(ord.total_sum),
		AVG(d.rate) OVER (PARTITION BY d.courier_id, dn.courier_name, ord.YEAR, ord.MONTH) AS rate_avg,
		d.tip_sum
	FROM ord
	JOIN dds.dm_deliveries d ON d.order_id = ord.id
	JOIN dds.dm_couriers dn ON d.courier_id = dn.id
),
pre AS (
	SELECT
		pay.courier_id,
		pay.courier_name,
		pay.settlement_year,
		pay.settlement_month,
		pay.id,
		pay.total_sum,
		pay.rate_avg,
		CASE 
			WHEN pay.rate_avg < 4 THEN
				CASE
					WHEN pay.total_sum*0.05 < 100 THEN 100
					ELSE pay.total_sum*0.05
				END
			WHEN pay.rate_avg <= 4.5 THEN
				CASE
					WHEN pay.total_sum*0.07 < 150 THEN 150
					ELSE pay.total_sum*0.07
				END
			WHEN pay.rate_avg < 4.9 THEN
				CASE 
					WHEN pay.total_sum*0.08 < 175 THEN 175
					ELSE pay.total_sum*0.08
				END
			ELSE 
				CASE 
					WHEN pay.total_sum*0.10 < 200 THEN 200
					ELSE pay.total_sum*0.10
				END
			END AS courier_order_sum,
		pay.tip_sum
	FROM pay
	),
fin as(
	SELECT 	
		courier_id,
		courier_name,
		settlement_year,
		settlement_month,
		COUNT(id) AS orders_count,
		SUM(total_sum) AS orders_total_sum,
		AVG(rate_avg) AS rate_avg,
		SUM(total_sum) * 0.25 AS order_processing_fee,
		SUM(courier_order_sum) AS courier_order_sum,
		SUM(tip_sum) AS courier_tips_sum,
		(SUM(courier_order_sum) + SUM(tip_sum)) * 0.95 AS courier_reward_sum
	FROM pre
	GROUP BY 1, 2, 3, 4	
)
SELECT * FROM fin
ON CONFLICT ON CONSTRAINT unique_courier_date --задаю униальность колонок курьера и дат
DO UPDATE SET 
	orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum;