CREATE TABLE IF NOT EXISTS daily_order_summary (
    summary_date DATE NOT NULL,
    city VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    event_count BIGINT NOT NULL,
    total_amount BIGINT NOT NULL,
    avg_delivery_sec DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (summary_date, city, event_type)
);

CREATE TABLE IF NOT EXISTS data_quality_checks (
    id SERIAL PRIMARY KEY,
    check_date DATE NOT NULL,
    check_name VARCHAR(128) NOT NULL,
    status VARCHAR(16) NOT NULL,
    metric_value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    details TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
