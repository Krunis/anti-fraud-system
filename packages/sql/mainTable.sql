-- CREATE TABLE IF NOT EXISTS processedPayments(
    
-- )

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fraud_requests(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id BIGINT,
    merchant_id BIGINT,
    interval_since TIMESTAMPTZ,
    timestamp_req TIMESTAMPTZ
    executed BOOLEAN DEFAULT FALSE
);