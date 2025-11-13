docker exec -it cassandra cqlsh << 'EOF'
USE binance;

-- Create materialized view for top performers (last 1 hour)
DROP MATERIALIZED VIEW IF EXISTS top_performers_1h;
CREATE MATERIALIZED VIEW top_performers_1h AS
    SELECT symbol, price_change_percent, timestamp, cdc_timestamp
    FROM ticker_24h
    WHERE symbol IS NOT NULL 
        AND timestamp IS NOT NULL 
        AND price_change_percent IS NOT NULL
        AND cdc_timestamp IS NOT NULL
    PRIMARY KEY (symbol, timestamp, cdc_timestamp)
    WITH CLUSTERING ORDER BY (timestamp DESC, cdc_timestamp DESC);

-- Create table for aggregated latest data (we'll populate this from the consumer)
DROP TABLE IF EXISTS latest_ticker_summary;
CREATE TABLE latest_ticker_summary (
    symbol text PRIMARY KEY,
    price_change_percent decimal,
    high_price decimal,
    low_price decimal,
    volume decimal,
    last_updated timestamp
);

-- Verify
DESCRIBE TABLES;
EOF





