CREATE TABLE IF NOT EXISTS transactions (
    event_id UUID PRIMARY KEY,
    transaction_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL,
    merchant_id VARCHAR(100) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(10) NOT NULL,
    payment_method VARCHAR(50),
    transaction_type VARCHAR(50),
    status VARCHAR(30),
    location VARCHAR(100),
    device_id VARCHAR(100),
    event_timestamp TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_customer_time
ON transactions (customer_id, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_transactions_event_time
ON transactions (event_timestamp DESC);
