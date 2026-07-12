-- CREATE TABLE IF NOT EXISTS processedPayments(
    
-- )

CREATE TABLE IF NOT EXISTS fraud_requests(
    account_id BIGINT,
    interval_since TIMESTAMPTZ
);