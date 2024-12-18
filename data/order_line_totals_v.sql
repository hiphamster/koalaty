CREATE OR REPLACE VIEW order_line_totals AS
SELECT
	date (orders.order_date),
	customers.first_name,
	customers.last_name,
	products.name,
	products.unit_price,
	order_lines.quantity,
	products.unit_price * order_lines.quantity AS 'order_line_total'
FROM
	customers
	JOIN orders ON customers.id = orders.customer_id
	JOIN order_lines ON orders.id = order_lines.order_id
	JOIN products ON products.id = order_lines.product_id
ORDER BY
	orders.order_date;
