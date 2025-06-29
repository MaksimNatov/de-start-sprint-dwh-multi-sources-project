--Слой CDM
-- Cоздание пользовательской витрины
DROP TABLE IF EXISTS cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger(
	id serial PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int NOT NULL,
	settlement_month int NOT NULL,
	orders_count int NOT NULL,
	orders_total_sum numeric(14, 2) DEFAULT 0 check(orders_total_sum>=0) NOT NULL,
	rate_avg numeric(2, 1) NOT NULL,
	order_processing_fee numeric(14, 2) DEFAULT 0 check(orders_total_sum>=0) NOT NULL,
	courier_order_sum numeric(14, 2) DEFAULT 0 check(orders_total_sum>=0) NOT NULL,
	courier_tips_sum numeric(14, 2) DEFAULT 0 NOT NULL,
	courier_reward_sum numeric(14, 2) DEFAULT 0 check(courier_tips_sum>=0) NOT NULL
);

-- Добавляем ограничение уникальности колонок. Оно понадобится для обновления таблицы при конфликтах
ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT unique_courier_date UNIQUE (courier_id, settlement_year, settlement_month);

-- Слой DDS
-- Таблица измерений курьеров
DROP TABLE IF EXISTS dds.dm_couriers CASCADE  ;
CREATE TABLE dds.dm_couriers(
	id serial PRIMARY KEY,
	courier_id varchar NOT NULL UNIQUE,
	courier_name varchar NOT NULL	
);
-- Таблица доставок
DROP TABLE IF EXISTS dds.dm_deliveries;
CREATE TABLE dds.dm_deliveries(
	id serial PRIMARY KEY,
	order_id int REFERENCES dds.dm_orders(id) ON UPDATE CASCADE NOT NULL UNIQUE,
	delivery_id varchar NOT NULL,
	courier_id int REFERENCES dds.dm_couriers(id) ON UPDATE CASCADE NOT NULL,
	rate int check(rate BETWEEN 1 AND 5) NOT NULL,
	tip_sum numeric(14, 2) DEFAULT 0 check(tip_sum>=0) NOT NULL
);

--Слой STG
--Витрина метода /couriers
DROP TABLE IF EXISTS stg.api_couriers CASCADE;
CREATE TABLE stg.api_couriers (
	id serial PRIMARY KEY NOT NULL,
	object_value text UNIQUE NOT NULL,
	update_ts timestamp NOT NULL
);

--Витрина метода /deliveries
DROP TABLE IF EXISTS stg.api_deliveries;
CREATE TABLE stg.api_deliveries (
	id serial PRIMARY KEY NOT NULL,
	object_value text UNIQUE NOT NULL,
	update_ts timestamp NOT NULL
);

-- Дополнительно нужно добавить записи в служебные таблицы
-- Добавляем записи last_ofset для для инкрементальной загрузки из API в STG
INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('api_couriers_to_stg_worflow', '{"last_offset": 0}'),
('api_deliveries_to_stg_worflow', '{"last_offset": 0}');
-- Добавляем записи last_ofset для инкрементальной загрузки из STG в DDS
INSERT	INTO dds.srv_wf_settings (workflow_key, workflow_settings)
VALUES ('stg_couriers_to_dds_worflow', '{"last_update_ts": "2025-01-01 00:00:00"}'),
('stg_deliveries_to_dds_worflow', '{"last_update_ts": "2025-01-01 00:00:00"}')


