from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement, ConsistencyLevel
import json
import logging
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
TOPICS = [
    'localhost.public.symbols',
    'localhost.public.latest_prices',
    'localhost.public.ticker_24h',
    'localhost.public.klines',
    'localhost.public.recent_trades'
]

# Cassandra configuration
CASSANDRA_HOSTS = ['cassandra']
CASSANDRA_KEYSPACE = 'binance'

# -------------------------------------------------------------
# WAIT FOR SERVICES
# -------------------------------------------------------------
def wait_for_services():
    """Wait for Kafka and Cassandra to be ready"""
    logger.info("Waiting for services to be ready...")
    time.sleep(10)

# -------------------------------------------------------------
# CONNECTIONS
# -------------------------------------------------------------
def connect_cassandra():
    """Connect to Cassandra cluster"""
    logger.info("Connecting to Cassandra...")
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            cluster = Cluster(CASSANDRA_HOSTS)
            session = cluster.connect(CASSANDRA_KEYSPACE)
            logger.info("Connected to Cassandra successfully")
            return session
        except Exception as e:
            logger.warning(f"Cassandra connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

def connect_kafka():
    """Connect to Kafka"""
    logger.info(f"Connecting to Kafka at {KAFKA_BROKER}...")
    max_retries = 10
    retry_delay = 5

    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='cassandra-sink-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            logger.info(f"Connected to Kafka successfully")
            return consumer
        except Exception as e:
            logger.warning(f"Kafka connection attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

# -------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------
def to_decimal(val):
    """Convert value to Decimal safely"""
    if val is None:
        return None
    try:
        return Decimal(str(val).strip())
    except (InvalidOperation, ValueError, TypeError) as e:
        logger.warning(f"Invalid decimal value '{val}': {e}")
        return None

def to_timestamp(val):
    """Convert value to timezone-aware datetime"""
    if val is None:
        return None
    try:
        if isinstance(val, str):
            return datetime.fromisoformat(val.replace("Z", "+00:00"))
        if isinstance(val, (int, float)):
            if val > 1e10:  # milliseconds
                val = val / 1000
            return datetime.fromtimestamp(val, tz=timezone.utc)
        return None
    except Exception as e:
        logger.warning(f"Invalid timestamp '{val}': {e}")
        return None

# -------------------------------------------------------------
# PREPARED STATEMENTS
# -------------------------------------------------------------
def prepare_statements(session):
    """Prepare all CQL statements"""
    statements = {}

    statements['symbols'] = session.prepare("""
        INSERT INTO symbols (id, symbol, base_asset, quote_asset, created_at, updated_at, cdc_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    statements['latest_prices'] = session.prepare("""
        INSERT INTO latest_prices (id, symbol, price, timestamp, cdc_timestamp)
        VALUES (?, ?, ?, ?, ?)
    """)

    statements['ticker_24h'] = session.prepare("""
        INSERT INTO ticker_24h (id, symbol, price_change, price_change_percent, volume, high_price, low_price, timestamp, cdc_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    statements['ticker_summary'] = session.prepare("""
        INSERT INTO latest_ticker_summary (symbol, price_change_percent, high_price, low_price, volume, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
    """)
    
    statements['klines'] = session.prepare("""
        INSERT INTO klines (id, symbol, open_time, open_price, high_price, low_price, close_price, volume, close_time, interval, timestamp, cdc_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    statements['recent_trades'] = session.prepare("""
        INSERT INTO recent_trades (id, symbol, trade_id, price, qty, is_buyer_maker, timestamp, cdc_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    logger.info("All prepared statements created successfully")
    return statements

# -------------------------------------------------------------
# INSERT FUNCTIONS
# -------------------------------------------------------------
def safe_execute(session, stmt, params, context=""):
    """Execute statement safely with debug logging"""
    try:
        session.execute(stmt, params)
        return True
    except Exception as e:
        logger.error(f"Error inserting {context}: {e}")
        logger.error(f"Values: {params}")
        logger.error(f"Types: {[type(x).__name__ for x in params]}")
        return False

def insert_latest_prices(session, stmt, data):
    """Insert into latest_prices table"""
    params = [
        data.get('id'),
        data.get('symbol'),
        to_decimal(data.get('price')),
        to_timestamp(data.get('timestamp')),
        datetime.now(tz=timezone.utc),
    ]
    return safe_execute(session, stmt, params, "latest_prices")

def insert_symbols(session, stmt, data):
    params = [
        data.get('id'),
        data.get('symbol'),
        data.get('base_asset'),
        data.get('quote_asset'),
        to_timestamp(data.get('created_at')),
        to_timestamp(data.get('updated_at')),
        datetime.now(tz=timezone.utc),
    ]
    return safe_execute(session, stmt, params, "symbols")

def insert_ticker_24h(session, stmt_ticker, stmt_summary, data):
    ok1 = safe_execute(session, stmt_ticker, [
        data.get('id'),
        data.get('symbol'),
        to_decimal(data.get('price_change')),
        to_decimal(data.get('price_change_percent')),
        to_decimal(data.get('volume')),
        to_decimal(data.get('high_price')),
        to_decimal(data.get('low_price')),
        to_timestamp(data.get('timestamp')),
        datetime.now(tz=timezone.utc),
    ], "ticker_24h")

    ok2 = safe_execute(session, stmt_summary, [
        data.get('symbol'),
        to_decimal(data.get('price_change_percent')),
        to_decimal(data.get('high_price')),
        to_decimal(data.get('low_price')),
        to_decimal(data.get('volume')),
        datetime.now(tz=timezone.utc),
    ], "ticker_summary")

    return ok1 and ok2

def insert_klines(session, stmt, data):
    return safe_execute(session, stmt, [
        data.get('id'),
        data.get('symbol'),
        to_timestamp(data.get('open_time')),
        to_decimal(data.get('open_price')),
        to_decimal(data.get('high_price')),
        to_decimal(data.get('low_price')),
        to_decimal(data.get('close_price')),
        to_decimal(data.get('volume')),
        to_timestamp(data.get('close_time')),
        data.get('interval', '1m'),
        to_timestamp(data.get('timestamp')),
        datetime.now(tz=timezone.utc),
    ], "klines")

def insert_recent_trades(session, stmt, data):
    return safe_execute(session, stmt, [
        data.get('id'),
        data.get('symbol'),
        data.get('trade_id'),
        to_decimal(data.get('price')),
        to_decimal(data.get('qty')),
        data.get('is_buyer_maker'),
        to_timestamp(data.get('timestamp')),
        datetime.now(tz=timezone.utc),
    ], "recent_trades")

# -------------------------------------------------------------
# MESSAGE PROCESSING
# -------------------------------------------------------------
def process_message(session, statements, topic, message):
    """Process Kafka message and insert into appropriate Cassandra table"""
    if message.get('__deleted') in ['true', True]:
        return

    table_name = topic.split('.')[-1]

    if table_name == 'symbols':
        insert_symbols(session, statements['symbols'], message)
    elif table_name == 'latest_prices':
        insert_latest_prices(session, statements['latest_prices'], message)
    elif table_name == 'ticker_24h':
        insert_ticker_24h(session, statements['ticker_24h'], statements['ticker_summary'], message)
    elif table_name == 'klines':
        insert_klines(session, statements['klines'], message)
    elif table_name == 'recent_trades':
        insert_recent_trades(session, statements['recent_trades'], message)
    else:
        logger.warning(f"Unknown table {table_name}, skipping message.")

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Starting Kafka to Cassandra CDC Consumer")
    logger.info("=" * 60)

    wait_for_services()
    session = connect_cassandra()
    statements = prepare_statements(session)
    consumer = connect_kafka()

    logger.info(f"Subscribed to topics: {TOPICS}")
    logger.info("Listening for messages...")

    message_count = success_count = 0

    try:
        for message in consumer:
            message_count += 1
            process_message(session, statements, message.topic, message.value)
            success_count += 1
            if message_count % 1000 == 0:
                logger.info(f"Processed {message_count} messages (Success: {success_count})")
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
    finally:
        logger.info(f"Total messages processed: {message_count}")
        logger.info(f"Successful inserts: {success_count}")
        consumer.close()
        session.shutdown()
        logger.info("Consumer shut down successfully")

if __name__ == "__main__":
    main()
