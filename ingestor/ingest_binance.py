
import time
import os
from binance.client import Client
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, IntegrityError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Config
API_KEY = 'BINANCE_API_KEY'
API_SECRET = 'BINANCE_SECRET_KEY'
DB_URL = 'postgresql://username:password@localhost:5432/binance_data'

client = Client(API_KEY, API_SECRET)
engine = create_engine(DB_URL)

def create_tables():
    """Ensure required tables, triggers, and publication exist."""
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS symbols (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) UNIQUE NOT NULL,
            base_asset VARCHAR(20) NOT NULL,
            quote_asset VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE OR REPLACE FUNCTION update_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        DROP TRIGGER IF EXISTS update_symbols_timestamp ON symbols;
        CREATE TRIGGER update_symbols_timestamp
            BEFORE UPDATE ON symbols
            FOR EACH ROW
            EXECUTE FUNCTION update_timestamp();
        """,
        """
        CREATE TABLE IF NOT EXISTS latest_prices (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL REFERENCES symbols(symbol),
            price DECIMAL(18,8) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_latest_prices_symbol_time ON latest_prices(symbol, timestamp);
        """,
        """
        CREATE TABLE IF NOT EXISTS ticker_24h (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL REFERENCES symbols(symbol),
            price_change DECIMAL(18,8),
            price_change_percent DECIMAL(6,2),
            volume DECIMAL(18,8),
            high_price DECIMAL(18,8),
            low_price DECIMAL(18,8),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, timestamp)
        );
        CREATE INDEX IF NOT EXISTS idx_24h_percent ON ticker_24h(price_change_percent DESC, timestamp DESC);
        """,
        """
        CREATE TABLE IF NOT EXISTS klines (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL REFERENCES symbols(symbol),
            open_time TIMESTAMP NOT NULL,
            open_price DECIMAL(18,8),
            high_price DECIMAL(18,8),
            low_price DECIMAL(18,8),
            close_price DECIMAL(18,8),
            volume DECIMAL(24,8),
            close_time TIMESTAMP NOT NULL,
            interval VARCHAR(10) DEFAULT '1m',
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, open_time)
        );
        CREATE INDEX IF NOT EXISTS idx_klines_symbol_time ON klines(symbol, open_time);
        """,
        """
        CREATE TABLE IF NOT EXISTS recent_trades (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) NOT NULL REFERENCES symbols(symbol),
            trade_id BIGINT NOT NULL,
            price DECIMAL(18,8),
            qty DECIMAL(18,8),
            is_buyer_maker BOOLEAN,
            timestamp TIMESTAMP NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON recent_trades(symbol, timestamp);
        """
        # """
        # CREATE PUBLICATION  crypto_pub FOR ALL TABLES;
        # """
    ]
    try:
        with engine.connect() as conn:
            for sql in sql_statements:
                conn.execute(text(sql))
                conn.commit()
                logger.info("Executed SQL: %s", sql.strip()[:50] + "...")
            logger.info("All tables, triggers, and publication created or verified.")
    except OperationalError as e:
        logger.error(f"Failed to create table/trigger/publication: {e}")
        raise

def get_valid_symbols():
    """Fetch all symbols from the symbols table."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT symbol FROM symbols"))
            return {row[0] for row in result.fetchall()}
    except Exception as e:
        logger.error(f"Error fetching valid symbols: {e}")
        raise

def populate_symbols():
    """Fetch and upsert all USDT pairs."""
    try:
        tickers = client.get_all_tickers()
        usdt_symbols = [t['symbol'] for t in tickers if t['symbol'].endswith('USDT')]
        with engine.connect() as conn:
            for sym in usdt_symbols:
                try:
                    conn.execute(
                        text("""
                            INSERT INTO symbols (symbol, base_asset, quote_asset)
                            VALUES (:sym, :base, :quote)
                            ON CONFLICT (symbol) DO UPDATE
                            SET base_asset = EXCLUDED.base_asset,
                                quote_asset = EXCLUDED.quote_asset,
                                updated_at = CURRENT_TIMESTAMP
                        """),
                        {'sym': sym, 'base': sym[:-4], 'quote': 'USDT'}
                    )
                    conn.commit()
                    logger.info(f"Processed symbol: {sym}")
                except Exception as e:
                    logger.error(f"Error processing {sym}: {e}")
                    conn.rollback()
                    raise
            logger.info(f"Populated/updated {len(usdt_symbols)} symbols.")
    except Exception as e:
        logger.error(f"Failed to populate symbols: {e}")
        raise

def upsert_latest_prices(valid_symbols):
    """Fetch and upsert latest prices for valid symbols."""
    try:
        tickers = client.get_all_tickers()
        with engine.connect() as conn:
            for t in tickers:
                if t['symbol'].endswith('USDT') and t['symbol'] in valid_symbols:
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO latest_prices (symbol, price, timestamp)
                                VALUES (:symbol, :price, CURRENT_TIMESTAMP)
                                ON CONFLICT (symbol, timestamp) DO NOTHING
                            """),
                            {'symbol': t['symbol'], 'price': float(t['price'])}
                        )
                        conn.commit()
                    except IntegrityError as e:
                        logger.warning(f"Skipped {t['symbol']} due to integrity error: {e}")
                        conn.rollback()
            logger.info("Latest prices updated.")
    except Exception as e:
        logger.error(f"Error in upsert_latest_prices: {e}")
        raise

def upsert_24h_ticker(valid_symbols):
    """Fetch 24h stats for valid symbols."""
    try:
        stats = client.get_ticker()
        with engine.connect() as conn:
            for s in stats:
                if s['symbol'].endswith('USDT') and s['symbol'] in valid_symbols:
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO ticker_24h (symbol, price_change, price_change_percent, volume, high_price, low_price, timestamp)
                                VALUES (:symbol, :change, :change_pct, :volume, :high, :low, CURRENT_TIMESTAMP)
                                ON CONFLICT (symbol, timestamp) DO UPDATE SET
                                    price_change = EXCLUDED.price_change,
                                    price_change_percent = EXCLUDED.price_change_percent,
                                    volume = EXCLUDED.volume,
                                    high_price = EXCLUDED.high_price,
                                    low_price = EXCLUDED.low_price
                            """),
                            {
                                'symbol': s['symbol'],
                                'change': float(s['priceChange']),
                                'change_pct': float(s['priceChangePercent']),
                                'volume': float(s['quoteVolume']),
                                'high': float(s['highPrice']),
                                'low': float(s['lowPrice'])
                            }
                        )
                        conn.commit()
                    except IntegrityError as e:
                        logger.warning(f"Skipped {s['symbol']} due to integrity error: {e}")
                        conn.rollback()
            logger.info("24h ticker updated.")
    except Exception as e:
        logger.error(f"Error in upsert_24h_ticker: {e}")
        raise

def fetch_and_upsert_klines(symbols, interval='1m', limit=1):
    """Fetch latest klines for valid symbols."""
    try:
        for sym in symbols:
            if sym in symbols:  # Already validated
                klines = client.get_klines(symbol=sym, interval=interval, limit=limit)
                with engine.connect() as conn:
                    for k in klines:
                        try:
                            conn.execute(
                                text("""
                                    INSERT INTO klines (symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time)
                                    VALUES (:sym, to_timestamp(:open_time/1000), :open, :high, :low, :close, :vol, to_timestamp(:close_time/1000))
                                    ON CONFLICT (symbol, open_time) DO NOTHING
                                """),
                                {
                                    'sym': sym, 'open_time': k[0], 'open': float(k[1]), 'high': float(k[2]),
                                    'low': float(k[3]), 'close': float(k[4]), 'vol': float(k[5]), 'close_time': k[6]
                                }
                            )
                            conn.commit()
                        except IntegrityError as e:
                            logger.warning(f"Skipped kline for {sym} due to integrity error: {e}")
                            conn.rollback()
                    logger.info(f"Klines updated for {sym}.")
    except Exception as e:
        logger.error(f"Error in fetch_and_upsert_klines: {e}")
        raise

def fetch_recent_trades(symbol, limit=10):
    """Fetch recent trades for valid symbols."""
    try:
        trades = client.get_recent_trades(symbol=symbol, limit=limit)
        with engine.connect() as conn:
            for t in trades:
                try:
                    conn.execute(
                        text("""
                            INSERT INTO recent_trades (symbol, trade_id, price, qty, is_buyer_maker, timestamp)
                            VALUES (:sym, :tid, :price, :qty, :is_bm, to_timestamp(:time/1000))
                        """),
                        {
                            'sym': symbol, 'tid': t['id'], 'price': float(t['price']), 'qty': float(t['qty']),
                            'is_bm': t['isBuyerMaker'], 'time': t['time']
                        }
                    )
                    conn.commit()
                except IntegrityError as e:
                    logger.warning(f"Skipped trade for {symbol} due to integrity error: {e}")
                    conn.rollback()
            logger.info(f"Recent trades updated for {symbol}.")
    except Exception as e:
        logger.error(f"Error in fetch_recent_trades: {e}")
        raise

if __name__ == "__main__":
    try:
        # Create tables and publication
        create_tables()
        
        # Populate symbols
        populate_symbols()
        
        # Get valid symbols for other operations
        valid_symbols = get_valid_symbols()
        logger.info(f"Loaded {len(valid_symbols)} valid symbols.")
        
        while True:
            try:
                upsert_latest_prices(valid_symbols)
                upsert_24h_ticker(valid_symbols)
                fetch_and_upsert_klines(valid_symbols)
                for sym in list(valid_symbols)[:5]:  # Limit trades for demo
                    fetch_recent_trades(sym)
                logger.info("Cycle complete; sleeping 30s")
                time.sleep(30)
            except Exception as e:
                logger.error(f"Main loop error: {e}; retrying in 30s")
                time.sleep(30)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        raise







