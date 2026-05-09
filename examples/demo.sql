LOAD CSV 'examples/customers.csv' INTO customers;
LOAD CSV 'examples/products.csv' INTO products;
LOAD CSV 'examples/orders.csv' INTO orders;
LOAD CSV 'examples/order_items.csv' INTO order_items;

SELECT customers.name, customers.city, orders.order_id, orders.status
FROM customers
JOIN orders ON customers.customer_id = orders.customer_id
WHERE orders.status = 'shipped';

SELECT customers.name, products.product_name, order_items.quantity, products.unit_price
FROM customers
JOIN orders ON customers.customer_id = orders.customer_id
JOIN order_items ON orders.order_id = order_items.order_id
JOIN products ON order_items.product_id = products.product_id
WHERE orders.status != 'cancelled';

SELECT orders.order_id, customers.name, products.category, products.product_name, order_items.quantity
FROM orders
JOIN customers ON orders.customer_id = customers.customer_id
JOIN order_items ON orders.order_id = order_items.order_id
JOIN products ON order_items.product_id = products.product_id
WHERE products.category = 'Electronics';
