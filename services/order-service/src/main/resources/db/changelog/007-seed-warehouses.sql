--liquibase formatted sql

--changeset denis:007-seed-warehouses
INSERT INTO warehouse (name, location)
SELECT 
  'Warehouse ' || i,
  (ARRAY['Moscow','SPb','Kazan','Novosibirsk','Ekaterinburg','Nizhny Novgorod','Samara','Omsk','Rostov','Ufa'])[floor(random()*10+1)]
FROM generate_series(1, 100) AS i;
