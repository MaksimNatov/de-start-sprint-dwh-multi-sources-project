/couriers
courier_id - нужен
courier_name - нужен

/deliveries
order_id - нужен
order_ts - нужен
delivery_id - нужен
courier_id- нужен
address - не нужен, нет таких бизнес требований
delivery_ts - не нужен, ориентируемся на дату заказа
rate - нужен
tip_sum - нужен
sum - не нужен, берем из MongoDB

Что из DDS будет нужно:
dm_timstamp - для агрегации по датам,
fct_product_sale -  для агренации суммы в заказе, 
dm_order - для связи по времени и для выборки закрытых заказов

Создаем еще две таблицы couriers, deliveries

