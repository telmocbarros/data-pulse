INSERT INTO users (first_name, last_name, email, username, password, country, phone, date_of_birth)
SELECT
  (ARRAY['James','Maria','John','Ana','Pedro','Sofia','Liam','Emma','Noah','Olivia'])[1 + (g % 10)],
  (ARRAY['Smith','Silva','Johnson','Garcia','Brown','Martinez','Jones','Lopez','Davis','Wilson'])[1 + (g % 10)],
  'user' || g || '@example.com',
  'user' || g,
  'pass' || g,
  (ARRAY['US','UK','PT','FR','DE','ES','IT','BR','JP','CA'])[1 + (g % 10)],
  '+1' || LPAD((1000000000 + g)::text, 10, '0'),
  DATE '1986-01-01' + (g % 7300 || ' days')::interval
FROM generate_series(1, 100000) AS g;
