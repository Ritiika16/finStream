CREATE TABLE IF NOT EXISTS anomaly_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    event_id UUID,
    customer_id VARCHAR(100) NOT NULL,
    anomaly_type VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    transaction_count BIGINT,
    amount NUMERIC(18, 2),
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_alert_severity
        CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),

    CONSTRAINT chk_alert_type
        CHECK (anomaly_type IN (
            'HIGH_VALUE_TRANSACTION',
            'HIGH_FREQUENCY_ACTIVITY'
        )),

    CONSTRAINT uq_anomaly_alert
        UNIQUE NULLS NOT DISTINCT (
            event_id,
            customer_id,
            anomaly_type,
            window_start,
            window_end
        )
);

CREATE INDEX IF NOT EXISTS idx_alerts_customer_time
ON anomaly_alerts (customer_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_alerts_type
ON anomaly_alerts (anomaly_type);
