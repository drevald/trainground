--liquibase formatted sql

--changeset denis:006-seed-products
INSERT INTO product (name, price)
SELECT 
  (ARRAY['Laptop','Smartphone','Tablet','Monitor','Keyboard','Mouse','Headphones','Webcam','Speaker','Charger',
         'Backpack','Watch','Camera','Drone','Printer','Router','SSD','GPU','CPU','RAM'])[floor(random()*20+1)]
  || ' Model ' || i,
  (random() * 1900 + 100)::numeric(10,2)
FROM generate_series(1, 100) AS i;
