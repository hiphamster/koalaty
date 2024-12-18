CREATE VIEW order_totals1 AS
select
  o.id AS order_id,
  SUM(ol.quantity * p.unit_price) AS total
FROM orders o
JOIN order_lines ol ON o.id = ol.order_id
JOIN products p ON p.id = ol.product_id
GROUP BY o.id

