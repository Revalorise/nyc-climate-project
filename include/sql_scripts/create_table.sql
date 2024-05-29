CREATE TABLE IF NOT EXISTS 'electronic_purchase' (
    event_time TIMESTAMP,
    order_id BIGINT PRIMARY KEY,
    product_id BIGINT,
    category_id BIGINT,
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price DECIMAL(10, 2),
    user_id BIGINT
)