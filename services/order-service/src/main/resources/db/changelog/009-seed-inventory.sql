--liquibase formatted sql

--changeset denis:009-seed-inventory
INSERT INTO inventory (product_id, warehouse_id, quantity)
SELECT 
  (random() * 99 + 1)::int,
  (random() * 99 + 1)::int,
  (random() * 500)::int
FROM generate_series(1, 1000);
