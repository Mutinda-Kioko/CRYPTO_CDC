-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS binance 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE binance;

-- Drop existing tables if you need to recreate
DROP TABLE IF EXISTS symbols;
DROP TABLE IF EXISTS latest_prices;
DROP TABLE IF EXISTS ticker_24h;
DROP TABLE IF EXISTS klines;
DROP TABLE IF EXISTS recent_trades;

-- Symbols table (matching PostgreSQL structure)
CREATE TABLE symbols (
    id int,
    symbol text,
    base_asset text,
    quote_asset text,
    created_at timestamp,
    updated_at timestamp,
    cdc_timestamp timestamp,
    PRIMARY KEY (symbol, cdc_timestamp)
) WITH CLUSTERING ORDER BY (cdc_timestamp DESC);

-- Latest prices table (matching PostgreSQL structure)
CREATE TABLE latest_prices (
    id int,
    symbol text,
    price double,
    timestamp timestamp,
    cdc_timestamp timestamp,
    PRIMARY KEY (symbol, cdc_timestamp)
) WITH CLUSTERING ORDER BY (cdc_timestamp DESC);

-- 24h ticker stats table (matching PostgreSQL structure)
CREATE TABLE ticker_24h (
    id int,
    symbol text,
    price_change double,
    price_change_percent double,
    volume double,
    high_price double,
    low_price double,
    timestamp timestamp,
    cdc_timestamp timestamp,
    PRIMARY KEY (symbol, cdc_timestamp)
) WITH CLUSTERING ORDER BY (cdc_timestamp DESC);

-- Create index for top performers query
CREATE INDEX IF NOT EXISTS idx_ticker_percent ON ticker_24h (price_change_percent);

-- Klines (candlesticks) table (matching PostgreSQL structure)
CREATE TABLE klines (
    id int,
    symbol text,
    open_time timestamp,
    open_price double,
    high_price double,
    low_price double,
    close_price double,
    volume double,
    close_time timestamp,
    interval text,
    timestamp timestamp,
    cdc_timestamp timestamp,
    PRIMARY KEY ((symbol, interval), open_time, cdc_timestamp)
) WITH CLUSTERING ORDER BY (open_time DESC, cdc_timestamp DESC);

-- Recent trades table (matching PostgreSQL structure)
CREATE TABLE recent_trades (
    id int,
    symbol text,
    trade_id bigint,
    price double,
    qty double,
    is_buyer_maker boolean,
    timestamp timestamp,
    cdc_timestamp timestamp,
    PRIMARY KEY (symbol, timestamp, trade_id)
) WITH CLUSTERING ORDER BY (timestamp DESC, trade_id DESC);