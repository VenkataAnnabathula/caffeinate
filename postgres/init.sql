CREATE TABLE IF NOT EXISTS sales_data (
    id SERIAL PRIMARY KEY,
    date DATE,
    product VARCHAR(100),
    customer VARCHAR(100),
    revenue NUMERIC
);
