CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
CREATE TABLE prices (sku text PRIMARY KEY, price integer NOT NULL, version integer NOT NULL);
INSERT INTO prices VALUES ('phone-x', 69999, 10);
INSERT INTO prices SELECT 'sku-' || lpad(g::text, 5, '0'), 1000 + g, 1 FROM generate_series(0, 4999) g;
