-- CREATE TABLE IF NOT EXISTS processedPayments(
    
-- )

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fraud_requests(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id BIGINT NOT NULL,
    merchant_id BIGINT NOT NULL,
    interaction TEXT,
    interval_since TIMESTAMPTZ,
    timestamp_req TIMESTAMPTZ NOT NULL,
    executed BOOLEAN NOT NULL DEFAULT FALSE
);