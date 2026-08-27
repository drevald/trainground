--liquibase formatted sql

--changeset denis:008-seed-customers
INSERT INTO customer (name, city)
SELECT 
  (ARRAY['Ivan','Anna','Petr','Maria','Sergey','Elena','Dmitry','Olga','Andrey','Tatiana'])[floor(random()*10+1)]
  || ' ' ||
  (ARRAY['Petrov','Ivanova','Sidorov','Kuznetsova','Smirnov','Popova','Volkov','Sokolova','Lebedev','Novikova'])[floor(random()*10+1)],
  (ARRAY['Moscow','SPb','Kazan','Novosibirsk','Ekaterinburg','Nizhny Novgorod','Samara','Omsk','Rostov','Ufa'])[floor(random()*10+1)]
FROM generate_series(1, 100000) AS i;
