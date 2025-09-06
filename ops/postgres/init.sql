-- ops/postgres/init.sql
-- PostgreSQL initialization script for Polymarket indexer

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- Create main database (if not exists)
-- This runs as postgres user, database already created via env vars

-- Connect to the polymarket_indexer database
\c polymarket_indexer;

-- Core blockchain tracking
CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash VARCHAR(66) NOT NULL UNIQUE,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    gas_used BIGINT,
    gas_limit BIGINT,
    indexed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Contract addresses and their types
CREATE TABLE IF NOT EXISTS contracts (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL UNIQUE,
    contract_type VARCHAR(50) NOT NULL, -- 'conditional_tokens', 'ctf_exchange', 'neg_risk_adapter'
    name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    first_seen_block BIGINT REFERENCES blocks(number)
);

-- Market conditions (the core prediction markets)
CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL UNIQUE, -- The bytes32 condition ID
    oracle VARCHAR(42) NOT NULL,
    question_id VARCHAR(66) NOT NULL,
    outcome_slot_count INTEGER NOT NULL DEFAULT 2,
    created_at_block BIGINT REFERENCES blocks(number),
    created_at_tx VARCHAR(66) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Market metadata (enriched from external sources)
    question TEXT,
    description TEXT,
    end_date TIMESTAMP WITH TIME ZONE,
    resolution_source TEXT,
    category VARCHAR(100),
    image_url TEXT,

    -- Current state
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at_block BIGINT REFERENCES blocks(number),
    resolved_at_tx VARCHAR(66),
    resolved_at TIMESTAMP WITH TIME ZONE,
    payout_numerators INTEGER[],

    -- Indexes
    CONSTRAINT unique_condition_id UNIQUE (condition_id)
);

-- Position tokens for each condition
CREATE TABLE IF NOT EXISTS position_tokens (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    collection_id VARCHAR(66) NOT NULL,
    position_id VARCHAR(78) NOT NULL UNIQUE, -- The full token ID
    outcome_index INTEGER NOT NULL,

    -- Current supply metrics
    total_supply DECIMAL(36,18) DEFAULT 0,
    last_supply_update_block BIGINT REFERENCES blocks(number),

    CONSTRAINT unique_position_id UNIQUE (position_id)
);

-- Trading pairs on the exchange
CREATE TABLE IF NOT EXISTS trading_pairs (
    id SERIAL PRIMARY KEY,
    token0 VARCHAR(78) NOT NULL, -- Position token ID
    token1 VARCHAR(78) NOT NULL, -- Usually USDC or other collateral
    exchange_address VARCHAR(42) NOT NULL,
    created_at_block BIGINT REFERENCES blocks(number),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Current market state
    is_active BOOLEAN DEFAULT TRUE,
    last_price DECIMAL(18,6),
    last_trade_block BIGINT REFERENCES blocks(number),

    CONSTRAINT unique_trading_pair UNIQUE(token0, token1, exchange_address)
);

-- Individual trades/orders
CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL REFERENCES blocks(number),
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Trade details
    exchange_address VARCHAR(42) NOT NULL,
    trader VARCHAR(42) NOT NULL,
    token_id VARCHAR(78) NOT NULL, -- Position token being traded
    collateral_token VARCHAR(42) NOT NULL, -- USDC, etc.

    -- Trade amounts
    token_amount DECIMAL(36,18) NOT NULL,
    collateral_amount DECIMAL(36,18) NOT NULL,
    price DECIMAL(18,6) NOT NULL, -- Calculated price

    -- Trade type
    is_buy BOOLEAN NOT NULL,
    order_id VARCHAR(66), -- If from order book

    CONSTRAINT unique_trade UNIQUE(tx_hash, log_index)
);

-- Order book state (for CLOB)
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(66) NOT NULL UNIQUE,
    trader VARCHAR(42) NOT NULL,
    token_id VARCHAR(78) NOT NULL,

    -- Order details
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DECIMAL(18,6) NOT NULL,
    size DECIMAL(36,18) NOT NULL,
    filled DECIMAL(36,18) DEFAULT 0,

    -- Blockchain tracking
    created_at_block BIGINT NOT NULL REFERENCES blocks(number),
    created_at_tx VARCHAR(66) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Order state
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'FILLED', 'CANCELLED', 'PARTIAL')),
    cancelled_at_block BIGINT REFERENCES blocks(number)
);

-- User balances for position tokens
CREATE TABLE IF NOT EXISTS balances (
    id SERIAL PRIMARY KEY,
    user_address VARCHAR(42) NOT NULL,
    token_id VARCHAR(78) NOT NULL,
    balance DECIMAL(36,18) NOT NULL DEFAULT 0,
    last_updated_block BIGINT NOT NULL REFERENCES blocks(number),
    last_updated_tx VARCHAR(66) NOT NULL,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_user_token UNIQUE(user_address, token_id)
);

-- Aggregate market metrics (computed/materialized)
CREATE TABLE IF NOT EXISTS market_metrics (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    computed_at_block BIGINT NOT NULL REFERENCES blocks(number),

    -- Volume metrics
    volume_24h DECIMAL(36,18) DEFAULT 0,
    volume_7d DECIMAL(36,18) DEFAULT 0,
    volume_30d DECIMAL(36,18) DEFAULT 0,
    volume_all_time DECIMAL(36,18) DEFAULT 0,

    -- Liquidity metrics
    total_liquidity DECIMAL(36,18) DEFAULT 0,
    best_bid DECIMAL(18,6),
    best_ask DECIMAL(18,6),
    mid_price DECIMAL(18,6),
    spread DECIMAL(18,6),

    -- Market activity
    trade_count_24h INTEGER DEFAULT 0,
    trade_count_7d INTEGER DEFAULT 0,
    unique_traders_24h INTEGER DEFAULT 0,

    -- Outcome probabilities (for binary markets)
    yes_price DECIMAL(18,6),
    no_price DECIMAL(18,6),

    CONSTRAINT unique_condition_metrics UNIQUE(condition_id)
);

-- Price history for charting (time series)
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    outcome_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    block_number BIGINT NOT NULL REFERENCES blocks(number),

    -- OHLCV data
    open_price DECIMAL(18,6) NOT NULL,
    high_price DECIMAL(18,6) NOT NULL,
    low_price DECIMAL(18,6) NOT NULL,
    close_price DECIMAL(18,6) NOT NULL,
    volume DECIMAL(36,18) NOT NULL,

    -- Time interval (1m, 5m, 1h, 1d, etc.)
    interval_type VARCHAR(10) NOT NULL DEFAULT '1m'
);

-- Indexer state tracking
CREATE TABLE IF NOT EXISTS indexer_state (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE, -- 'conditional_tokens', 'ctf_exchange', etc.
    last_processed_block BIGINT NOT NULL,
    last_processed_tx_index INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'PAUSED', 'ERROR')),
    error_message TEXT,
    total_events_processed BIGINT DEFAULT 0,
    processing_rate_per_minute DECIMAL(10,2) DEFAULT 0
);

-- Event log for debugging and reprocessing
CREATE TABLE IF NOT EXISTS event_logs (
    id SERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(number),
    tx_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    event_data JSONB NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP WITH TIME ZONE,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,

    CONSTRAINT unique_event UNIQUE(tx_hash, log_index)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX IF NOT EXISTS idx_blocks_number ON blocks(number);

CREATE INDEX IF NOT EXISTS idx_conditions_condition_id ON conditions(condition_id);
CREATE INDEX IF NOT EXISTS idx_conditions_oracle ON conditions(oracle);
CREATE INDEX IF NOT EXISTS idx_conditions_created_block ON conditions(created_at_block);
CREATE INDEX IF NOT EXISTS idx_conditions_end_date ON conditions(end_date);
CREATE INDEX IF NOT EXISTS idx_conditions_resolved ON conditions(resolved);
CREATE INDEX IF NOT EXISTS idx_conditions_category ON conditions(category);

CREATE INDEX IF NOT EXISTS idx_position_tokens_condition ON position_tokens(condition_id);
CREATE INDEX IF NOT EXISTS idx_position_tokens_position_id ON position_tokens(position_id);

CREATE INDEX IF NOT EXISTS idx_trades_condition_id ON trades(token_id);
CREATE INDEX IF NOT EXISTS idx_trades_trader ON trades(trader);
CREATE INDEX IF NOT EXISTS idx_trades_block_timestamp ON trades(block_timestamp);
CREATE INDEX IF NOT EXISTS idx_trades_block_number ON trades(block_number);
CREATE INDEX IF NOT EXISTS idx_trades_exchange ON trades(exchange_address);
CREATE INDEX IF NOT EXISTS idx_trades_tx_hash ON trades(tx_hash);

CREATE INDEX IF NOT EXISTS idx_balances_user ON balances(user_address);
CREATE INDEX IF NOT EXISTS idx_balances_token ON balances(token_id);
CREATE INDEX IF NOT EXISTS idx_balances_updated_block ON balances(last_updated_block);

CREATE INDEX IF NOT EXISTS idx_market_metrics_condition ON market_metrics(condition_id);
CREATE INDEX IF NOT EXISTS idx_market_metrics_computed_at ON market_metrics(computed_at);
CREATE INDEX IF NOT EXISTS idx_market_metrics_volume_24h ON market_metrics(volume_24h DESC);

CREATE INDEX IF NOT EXISTS idx_price_history_condition_time ON price_history(condition_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_price_history_interval ON price_history(interval_type, timestamp);

CREATE INDEX IF NOT EXISTS idx_event_logs_block ON event_logs(block_number);
CREATE INDEX IF NOT EXISTS idx_event_logs_contract ON event_logs(contract_address);
CREATE INDEX IF NOT EXISTS idx_event_logs_event_name ON event_logs(event_name);
CREATE INDEX IF NOT EXISTS idx_event_logs_processed ON event_logs(processed);

-- Insert initial contract addresses
INSERT INTO contracts (address, contract_type, name) VALUES
('0x4D97DCd97eC945f40cF65F87097ACe5EA0476045', 'conditional_tokens', 'ConditionalTokens'),
('0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E', 'ctf_exchange', 'CTFExchange'),
('0xC5d563A36AE78145C45a50134d48A1215220f80a', 'neg_risk_adapter', 'NegRiskAdapter')
ON CONFLICT (address) DO NOTHING;

-- Insert initial indexer state
INSERT INTO indexer_state (name, last_processed_block) VALUES
('conditional_tokens', 50000000),
('ctf_exchange', 50000000),
('neg_risk_adapter', 50000000)
ON CONFLICT (name) DO NOTHING;

-- Create function to refresh market metrics
CREATE OR REPLACE FUNCTION refresh_market_metrics(condition_id_param VARCHAR(66))
RETURNS VOID AS $$
BEGIN
    INSERT INTO market_metrics (
        condition_id,
        computed_at_block,
        volume_24h,
        volume_7d,
        volume_30d,
        volume_all_time,
        trade_count_24h,
        unique_traders_24h
    )
    SELECT
        condition_id_param,
        MAX(t.block_number),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '7 days' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '30 days' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(t.collateral_amount), 0),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN 1 END),
        COUNT(DISTINCT CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN t.trader END)
    FROM trades t
    JOIN position_tokens pt ON t.token_id = pt.position_id
    WHERE pt.condition_id = condition_id_param
    ON CONFLICT (condition_id)
    DO UPDATE SET
        computed_at = NOW(),
        computed_at_block = EXCLUDED.computed_at_block,
        volume_24h = EXCLUDED.volume_24h,
        volume_7d = EXCLUDED.volume_7d,
        volume_30d = EXCLUDED.volume_30d,
        volume_all_time = EXCLUDED.volume_all_time,
        trade_count_24h = EXCLUDED.trade_count_24h,
        unique_traders_24h = EXCLUDED.unique_traders_24h;
END;
$$ LANGUAGE plpgsql;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;