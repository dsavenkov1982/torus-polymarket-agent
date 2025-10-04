-- ops/postgres/init.sql
-- Enhanced PostgreSQL schema for Polymarket indexer with advanced analytics

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

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

CREATE TABLE IF NOT EXISTS contracts (
    id SERIAL PRIMARY KEY,
    address VARCHAR(42) NOT NULL UNIQUE,
    contract_type VARCHAR(50) NOT NULL,
    name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    first_seen_block BIGINT REFERENCES blocks(number)
);

-- Market conditions
CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL UNIQUE,
    oracle VARCHAR(42) NOT NULL,
    question_id VARCHAR(66) NOT NULL,
    outcome_slot_count INTEGER NOT NULL DEFAULT 2,
    created_at_block BIGINT REFERENCES blocks(number),
    created_at_tx VARCHAR(66) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    question TEXT,
    description TEXT,
    end_date TIMESTAMP WITH TIME ZONE,
    resolution_source TEXT,
    category VARCHAR(100),
    image_url TEXT,

    resolved BOOLEAN DEFAULT FALSE,
    resolved_at_block BIGINT REFERENCES blocks(number),
    resolved_at_tx VARCHAR(66),
    resolved_at TIMESTAMP WITH TIME ZONE,
    payout_numerators INTEGER[]
);

CREATE TABLE IF NOT EXISTS position_tokens (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    collection_id VARCHAR(66) NOT NULL,
    position_id VARCHAR(78) NOT NULL UNIQUE,
    outcome_index INTEGER NOT NULL,
    total_supply DECIMAL(36,18) DEFAULT 0,
    last_supply_update_block BIGINT REFERENCES blocks(number)
);

CREATE TABLE IF NOT EXISTS trading_pairs (
    id SERIAL PRIMARY KEY,
    token0 VARCHAR(78) NOT NULL,
    token1 VARCHAR(78) NOT NULL,
    exchange_address VARCHAR(42) NOT NULL,
    created_at_block BIGINT REFERENCES blocks(number),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE,
    last_price DECIMAL(18,6),
    last_trade_block BIGINT REFERENCES blocks(number),
    CONSTRAINT unique_trading_pair UNIQUE(token0, token1, exchange_address)
);

CREATE TABLE IF NOT EXISTS trades (
    id SERIAL PRIMARY KEY,
    tx_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL REFERENCES blocks(number),
    block_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    exchange_address VARCHAR(42) NOT NULL,
    trader VARCHAR(42) NOT NULL,
    token_id VARCHAR(78) NOT NULL,
    collateral_token VARCHAR(42) NOT NULL,
    token_amount DECIMAL(36,18) NOT NULL,
    collateral_amount DECIMAL(36,18) NOT NULL,
    price DECIMAL(18,6) NOT NULL,
    is_buy BOOLEAN NOT NULL,
    order_id VARCHAR(66),
    CONSTRAINT unique_trade UNIQUE(tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(66) NOT NULL UNIQUE,
    trader VARCHAR(42) NOT NULL,
    token_id VARCHAR(78) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    price DECIMAL(18,6) NOT NULL,
    size DECIMAL(36,18) NOT NULL,
    filled DECIMAL(36,18) DEFAULT 0,
    created_at_block BIGINT NOT NULL REFERENCES blocks(number),
    created_at_tx VARCHAR(66) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'OPEN' CHECK (status IN ('OPEN', 'FILLED', 'CANCELLED', 'PARTIAL')),
    cancelled_at_block BIGINT REFERENCES blocks(number)
);

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

-- Enhanced market metrics with multi-timeframe data
CREATE TABLE IF NOT EXISTS market_metrics (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    computed_at_block BIGINT NOT NULL REFERENCES blocks(number),

    -- Multi-timeframe volumes
    volume_1h DECIMAL(36,18) DEFAULT 0,
    volume_4h DECIMAL(36,18) DEFAULT 0,
    volume_12h DECIMAL(36,18) DEFAULT 0,
    volume_24h DECIMAL(36,18) DEFAULT 0,
    volume_7d DECIMAL(36,18) DEFAULT 0,
    volume_30d DECIMAL(36,18) DEFAULT 0,
    volume_all_time DECIMAL(36,18) DEFAULT 0,

    -- Volume change percentages
    volume_1h_change_pct DECIMAL(10,4) DEFAULT 0,
    volume_4h_change_pct DECIMAL(10,4) DEFAULT 0,
    volume_12h_change_pct DECIMAL(10,4) DEFAULT 0,
    volume_24h_change_pct DECIMAL(10,4) DEFAULT 0,

    -- Open Interest tracking
    open_interest DECIMAL(36,18) DEFAULT 0,
    open_interest_1h_ago DECIMAL(36,18),
    open_interest_4h_ago DECIMAL(36,18),
    open_interest_12h_ago DECIMAL(36,18),
    open_interest_24h_ago DECIMAL(36,18),

    -- OI change percentages
    oi_1h_change_pct DECIMAL(10,4),
    oi_4h_change_pct DECIMAL(10,4),
    oi_12h_change_pct DECIMAL(10,4),
    oi_24h_change_pct DECIMAL(10,4),

    -- Liquidity metrics
    total_liquidity DECIMAL(36,18) DEFAULT 0,
    best_bid DECIMAL(18,6),
    best_ask DECIMAL(18,6),
    mid_price DECIMAL(18,6),
    spread DECIMAL(18,6),
    bid_ask_spread_pct DECIMAL(10,4),

    -- Market activity
    trade_count_1h INTEGER DEFAULT 0,
    trade_count_4h INTEGER DEFAULT 0,
    trade_count_12h INTEGER DEFAULT 0,
    trade_count_24h INTEGER DEFAULT 0,
    trade_count_7d INTEGER DEFAULT 0,
    unique_traders_24h INTEGER DEFAULT 0,

    -- Outcome probabilities
    yes_price DECIMAL(18,6),
    no_price DECIMAL(18,6),
    yes_price_12h_ago DECIMAL(18,6),
    yes_price_24h_ago DECIMAL(18,6),
    price_12h_change_pct DECIMAL(10,4),
    price_24h_change_pct DECIMAL(10,4),

    -- Advanced metrics
    turnover_ratio DECIMAL(10,4),
    liq_vol_ratio DECIMAL(10,4),
    price_momentum DECIMAL(10,4),
    volume_momentum DECIMAL(10,4),
    normalized_spread DECIMAL(10,4),
    adjusted_volatility DECIMAL(10,4),
    trend_strength DECIMAL(10,4),
    reversal_rate DECIMAL(10,4),

    CONSTRAINT unique_condition_metrics UNIQUE(condition_id)
);

-- User position tracking for PnL calculations
CREATE TABLE IF NOT EXISTS user_market_positions (
    id SERIAL PRIMARY KEY,
    user_address VARCHAR(42) NOT NULL,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    outcome_index INTEGER NOT NULL,

    -- Position tracking
    total_shares_bought DECIMAL(36,18) DEFAULT 0,
    total_shares_sold DECIMAL(36,18) DEFAULT 0,
    current_shares DECIMAL(36,18) DEFAULT 0,
    average_buy_price DECIMAL(18,6),
    average_sell_price DECIMAL(18,6),

    -- Cost basis
    total_cost_basis DECIMAL(36,18) DEFAULT 0,
    total_proceeds DECIMAL(36,18) DEFAULT 0,

    -- PnL
    realized_pnl DECIMAL(36,18) DEFAULT 0,
    unrealized_pnl DECIMAL(36,18) DEFAULT 0,

    first_trade_at TIMESTAMP WITH TIME ZONE,
    last_trade_at TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    CONSTRAINT unique_user_position UNIQUE(user_address, condition_id, outcome_index)
);

CREATE INDEX idx_user_positions_user ON user_market_positions(user_address);
CREATE INDEX idx_user_positions_condition ON user_market_positions(condition_id);
CREATE INDEX idx_user_positions_pnl ON user_market_positions(realized_pnl DESC);

-- User aggregate statistics
CREATE TABLE IF NOT EXISTS user_stats (
    user_address VARCHAR(42) PRIMARY KEY,
    total_volume DECIMAL(36,18) DEFAULT 0,
    total_trades INTEGER DEFAULT 0,
    total_realized_pnl DECIMAL(36,18) DEFAULT 0,
    total_unrealized_pnl DECIMAL(36,18) DEFAULT 0,
    portfolio_value DECIMAL(36,18) DEFAULT 0,
    markets_traded INTEGER DEFAULT 0,
    active_positions INTEGER DEFAULT 0,
    win_count INTEGER DEFAULT 0,
    loss_count INTEGER DEFAULT 0,
    avg_trade_size DECIMAL(36,18),
    largest_win DECIMAL(36,18),
    largest_loss DECIMAL(36,18),
    first_trade_at TIMESTAMP WITH TIME ZONE,
    last_trade_at TIMESTAMP WITH TIME ZONE,
    last_updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_user_stats_pnl ON user_stats(total_realized_pnl DESC);
CREATE INDEX idx_user_stats_volume ON user_stats(total_volume DESC);

-- Order book snapshots
CREATE TABLE IF NOT EXISTS order_book_snapshots (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    outcome_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    best_bid DECIMAL(18,6),
    best_ask DECIMAL(18,6),
    bid_size DECIMAL(36,18),
    ask_size DECIMAL(36,18),
    spread DECIMAL(18,6),
    spread_pct DECIMAL(10,4),
    mid_price DECIMAL(18,6)
);

CREATE INDEX idx_order_book_condition_time ON order_book_snapshots(condition_id, timestamp DESC);

-- Price history for charting
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    condition_id VARCHAR(66) NOT NULL REFERENCES conditions(condition_id),
    outcome_index INTEGER NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    block_number BIGINT NOT NULL REFERENCES blocks(number),
    open_price DECIMAL(18,6) NOT NULL,
    high_price DECIMAL(18,6) NOT NULL,
    low_price DECIMAL(18,6) NOT NULL,
    close_price DECIMAL(18,6) NOT NULL,
    volume DECIMAL(36,18) NOT NULL,
    trade_count INTEGER DEFAULT 0,
    interval_type VARCHAR(10) NOT NULL DEFAULT '1m'
);

CREATE INDEX idx_price_history_condition_time ON price_history(condition_id, timestamp DESC);
CREATE INDEX idx_price_history_interval ON price_history(interval_type, timestamp DESC);

-- Indexer state tracking
CREATE TABLE IF NOT EXISTS indexer_state (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    last_processed_block BIGINT NOT NULL,
    last_processed_tx_index INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'PAUSED', 'ERROR')),
    error_message TEXT,
    total_events_processed BIGINT DEFAULT 0,
    processing_rate_per_minute DECIMAL(10,2) DEFAULT 0
);

-- Event log for debugging
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
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX idx_blocks_number ON blocks(number);
CREATE INDEX idx_conditions_condition_id ON conditions(condition_id);
CREATE INDEX idx_conditions_oracle ON conditions(oracle);
CREATE INDEX idx_conditions_created_block ON conditions(created_at_block);
CREATE INDEX idx_conditions_end_date ON conditions(end_date);
CREATE INDEX idx_conditions_resolved ON conditions(resolved);
CREATE INDEX idx_conditions_category ON conditions(category);
CREATE INDEX idx_position_tokens_condition ON position_tokens(condition_id);
CREATE INDEX idx_position_tokens_position_id ON position_tokens(position_id);
CREATE INDEX idx_trades_condition_id ON trades(token_id);
CREATE INDEX idx_trades_trader ON trades(trader);
CREATE INDEX idx_trades_block_timestamp ON trades(block_timestamp);
CREATE INDEX idx_trades_block_number ON trades(block_number);
CREATE INDEX idx_trades_exchange ON trades(exchange_address);
CREATE INDEX idx_trades_tx_hash ON trades(tx_hash);
CREATE INDEX idx_balances_user ON balances(user_address);
CREATE INDEX idx_balances_token ON balances(token_id);
CREATE INDEX idx_balances_updated_block ON balances(last_updated_block);
CREATE INDEX idx_market_metrics_condition ON market_metrics(condition_id);
CREATE INDEX idx_market_metrics_computed_at ON market_metrics(computed_at);
CREATE INDEX idx_market_metrics_volume_24h ON market_metrics(volume_24h DESC);
CREATE INDEX idx_event_logs_block ON event_logs(block_number);
CREATE INDEX idx_event_logs_contract ON event_logs(contract_address);
CREATE INDEX idx_event_logs_event_name ON event_logs(event_name);
CREATE INDEX idx_event_logs_processed ON event_logs(processed);

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

-- Enhanced refresh_market_metrics function
CREATE OR REPLACE FUNCTION refresh_market_metrics(condition_id_param VARCHAR(66))
RETURNS VOID AS $$
DECLARE
    current_block_num BIGINT;
BEGIN
    SELECT MAX(block_number) INTO current_block_num FROM trades;

    INSERT INTO market_metrics (
        condition_id,
        computed_at_block,
        volume_1h, volume_4h, volume_12h, volume_24h, volume_7d, volume_all_time,
        trade_count_1h, trade_count_4h, trade_count_12h, trade_count_24h, trade_count_7d,
        unique_traders_24h,
        open_interest
    )
    SELECT
        condition_id_param,
        current_block_num,
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 hour' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '4 hours' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '12 hours' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(CASE WHEN t.block_timestamp > NOW() - INTERVAL '7 days' THEN t.collateral_amount ELSE 0 END), 0),
        COALESCE(SUM(t.collateral_amount), 0),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 hour' THEN 1 END),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '4 hours' THEN 1 END),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '12 hours' THEN 1 END),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN 1 END),
        COUNT(CASE WHEN t.block_timestamp > NOW() - INTERVAL '7 days' THEN 1 END),
        COUNT(DISTINCT CASE WHEN t.block_timestamp > NOW() - INTERVAL '1 day' THEN t.trader END),
        (SELECT COALESCE(SUM(balance), 0) FROM balances b
         JOIN position_tokens pt2 ON b.token_id = pt2.position_id
         WHERE pt2.condition_id = condition_id_param)
    FROM trades t
    JOIN position_tokens pt ON t.token_id = pt.position_id
    WHERE pt.condition_id = condition_id_param
    ON CONFLICT (condition_id)
    DO UPDATE SET
        computed_at = NOW(),
        computed_at_block = EXCLUDED.computed_at_block,
        volume_1h = EXCLUDED.volume_1h,
        volume_4h = EXCLUDED.volume_4h,
        volume_12h = EXCLUDED.volume_12h,
        volume_24h = EXCLUDED.volume_24h,
        volume_7d = EXCLUDED.volume_7d,
        volume_all_time = EXCLUDED.volume_all_time,
        trade_count_1h = EXCLUDED.trade_count_1h,
        trade_count_4h = EXCLUDED.trade_count_4h,
        trade_count_12h = EXCLUDED.trade_count_12h,
        trade_count_24h = EXCLUDED.trade_count_24h,
        trade_count_7d = EXCLUDED.trade_count_7d,
        unique_traders_24h = EXCLUDED.unique_traders_24h,
        open_interest = EXCLUDED.open_interest;
END;
$$ LANGUAGE plpgsql;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO postgres;