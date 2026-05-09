# Examples

This directory contains normalized CSV tables for exercising JOIN support.

Files:
- customers.csv (10 rows)
- products.csv (10 rows)
- orders.csv (10 rows)
- order_items.csv (10 rows)
- demo.sql (loads all CSVs and runs JOIN queries)

Run the demo from the repository root:

```powershell
julia --project=. bin/jsql.jl --file examples/demo.sql
```

Expected output:

```text
OK
OK
OK
OK
customers.name | customers.city | orders.order_id | orders.status
---------------+----------------+-----------------+--------------
Ada Lovelace   | London         | 1001            | shipped
Alan Turing    | Manchester     | 1003            | shipped
Donald Knuth   | Milwaukee      | 1005            | shipped
Barbara Liskov | Los Angeles    | 1007            | shipped
Ken Thompson   | New Orleans    | 1009            | shipped
(5 rows)
customers.name    | products.product_name | order_items.quantity | products.unit_price
------------------+-----------------------+----------------------+--------------------
Ada Lovelace      | Notebook              | 2                    | 12
Ada Lovelace      | USB-C Cable           | 1                    | 9
Grace Hopper      | Wireless Mouse        | 1                    | 24
Grace Hopper      | Coffee Beans          | 2                    | 15
Alan Turing       | Mechanical Pencil     | 3                    | 4
Donald Knuth      | Keyboard              | 1                    | 58
Edsger Dijkstra   | Desk Lamp             | 1                    | 32
Barbara Liskov    | Monitor Stand         | 1                    | 39
Margaret Hamilton | Water Bottle          | 2                    | 14
(9 rows)
orders.order_id | customers.name | products.category | products.product_name | order_items.quantity
----------------+----------------+-------------------+-----------------------+---------------------
1001            | Ada Lovelace   | Electronics       | USB-C Cable           | 1
1002            | Grace Hopper   | Electronics       | Wireless Mouse        | 1
1005            | Donald Knuth   | Electronics       | Keyboard              | 1
(3 rows)
```
