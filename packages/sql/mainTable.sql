-- CREATE TABLE IF NOT EXISTS processedPayments(
    
-- )

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS fraud_requests(
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id TEXT NOT NULL,
    merchant_id TEXT NOT NULL,
    interaction TEXT,
    interval_since TIMESTAMPTZ,
    timestamp_req TIMESTAMPTZ NOT NULL,
    executed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS payment_events(
    event_id TEXT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    direction TEXT,
    amount REAL,
    currency TEXT DEFAULT 'USD',
    transaction_type TEXT DEFAULT 'unknown',
    account_id TEXT NOT NULL DEFAULT '',
    merchant_id TEXT NOT NULL DEFAULT '',
    merchant_name TEXT,
    country TEXT NOT NULL,
	channel TEXT NOT NULL,
	device_id TEXT NOT NULL,
	ip TEXT NOT NULL, 
	user_agent TEXT
);