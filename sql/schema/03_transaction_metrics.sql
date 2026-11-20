CREATE TABLE IF NOT EXISTS transaction_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    currency VARCHAR(10) NOT NULL,
    transaction_count BIGINT NOT NULL CHECK (transaction_count >= 0),
    total_amount NUMERIC(20, 2) NOT NULL,
    average_amount NUMERIC(20, 6) NOT NULL,
    maximum_amount NUMERIC(18, 2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_transaction_metrics_window
        UNIQUE (window_start, window_end, currency),

    CONSTRAINT chk_metrics_window
        CHECK (window_end > window_start)
);

CREATE INDEX IF NOT EXISTS idx_metrics_window_start
ON transaction_metrics (window_start DESC);
