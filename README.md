# CRYPTO_CDC:🪙 Binance Real-Time Data Pipeline with CDC, Cassandra & Grafana

A production-ready, end-to-end data pipeline that ingests live market data from Binance, streams changes via PostgreSQL CDC, and delivers real-time insights through Kafka → Cassandra → Grafana.

A **real-time cryptocurrency analytics pipeline** that ingests live Binance market data, stores it in **PostgreSQL**, replicates it to **Cassandra** via **Change Data Capture (CDC)** using **Debezium**, and visualizes insights in **Grafana**.

This setup allows you to track and analyze market metrics such as the **top 5 performing cryptocurrencies by 24h gain**, **candlestick trends**, and **live price movements** — all in real time.

---

## 🚀 Architecture Overview

```
    +---------------------+
    |    Binance API      |
    | (WebSocket & REST)  |
    +----------+----------+
               |
               | JSON Streams (Trades, Tickers, Klines)
               v
    +---------------------+
    |  Python Ingest App  |
    | (asyncio / aiohttp) |
    +----------+----------+
               |
               | INSERT INTO
               v
    +---------------------+
    |   PostgreSQL (OLTP) |
    +----------+----------+
               |
               | CDC via Debezium
               v
    +---------------------+
    |   Kafka + Connect   |
    +----------+----------+
               |
               | Cassandra Sink Connector
               v
    +---------------------+
    |     Cassandra DB    |
    +----------+----------+
               |
               | SQL Queries
               v
    +---------------------+
    |      Grafana        |
    +---------------------+


```

---

## 🧰 Tech Stack

| Component                    | Purpose                                                          |
| ---------------------------- | ---------------------------------------------------------------- |
| **Binance API**              | Real-time crypto data source (prices, trades, klines, 24h stats) |
| **Python (asyncio)**         | Data ingestion and transformation                                |
| **PostgreSQL**               | Structured staging database                                      |
| **Debezium + Kafka Connect** | Change Data Capture (CDC) replication layer                      |
| **Cassandra**                | Scalable, time-series optimized datastore                        |
| **Grafana**                  | Real-time visualization dashboards                               |
| **Docker Compose**           | Container orchestration and local development                    |

---

## ⚙️ Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/Mutinda-Kioko/CRYPTO_CDC.git
cd CRYPTO_CDC
```

### 2. Environment Configuration

Adjust credentials and endpoints in `.env` or directly in `docker-compose.yml` if needed.

### 3. Start the Infrastructure

```bash
docker-compose up -d
```

This will launch:

- Zookeeper & Kafka
- PostgreSQL
- Debezium Connect
- Cassandra
- Grafana

### 4. Verify Services

- **PostgreSQL** → `localhost:5432`
- **Kafka Connect API** → `http://localhost:8083`
- **Grafana** → `http://localhost:3000` (default login: `admin` / `admin`)
- **Cassandra** → `localhost:9042`

---

## 🐍 Python Ingestion Service

### Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Run Ingest App

```bash
cd ingestor

pip install -r requirements.txt

python binance_ingest.py
```

This will:

- Connect to Binance’s REST endpoints
- Insert real-time market and trade data into PostgreSQL tables

---

## 🔄 Change Data Capture (CDC)

### Register Debezium Connector

```bash
bash scripts/register_debezium_connector.sh
```

### Register Cassandra Sink Connector

```bash
bash scripts/register_cassandra_sink_connector.sh

```

Kafka Connect will now stream changes from PostgreSQL into Cassandra in near real-time.

---

## Initialize Cassandra & Create Tables / Views

Run the CQL below after Cassandra is running to create the keyspace, primary time-series tables, and a lightweight top5_24h table as well as an example materialized view . To execute these, you can paste them into cqlsh running inside the Cassandra container or use a heredoc.

Run with docker-compose:

```bash
docker-compose exec -T cassandra cqlsh -e "SOURCE 'cassandra/init_cassandra.cql';"

```

Or run the commands directly:

```bash
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

```

## 📊 Grafana Visualization

1. Go to **Grafana** → `http://localhost:3000`
2. Add a **PostgreSQL datasource**:
   - Host: `host.docker.internal:5432`
   - Database: `binance`
   - User: `postgres`
   - Password: `postgres`
3. Import the dashboard:
   - **Dashboard → Import → Upload `grafana/binance-dashboard.json`**

You’ll now see:

- 📈 Top 5 coins by 24h % gain
- 🕯️ Candlestick charts (klines)
- 💹 Real-time ticker metrics

---

## 📁 Project Structure

```
CRYPTO_CDC/
├── docker-compose.yml
├── cassandra/
│   ├── create_views.cql
│   └── init_cassandra.cql
├── cassandra_sink/
│   ├── kafka_to_cassandra.py
├── grafana/
│   └── dashboards/
|       ├── price_trends.json
|       └── top5_gainers.json
├── ingestor/
│   ├── ingest_binance.py
│   └── requirements.txt
├── connect_plugins/
├── scripts/
│   ├── register_cassandra_sink_connector.sh
│   └── register_debezium_connector.sh
└── README.md
```

---

## 🧪 Validation Steps

**Check PostgreSQL Data**

```bash
docker-compose exec postgres psql -U postgres -d binance -c "SELECT * FROM trades LIMIT 10;"
```

**Check Kafka Topics**

```bash
docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

**Check Cassandra Data**

```bash
docker-compose exec cassandra cqlsh -e "SELECT * FROM binance.trades_by_symbol_day LIMIT 5;"
```

**Check Connectors**

```bash
curl http://localhost:8083/connectors | jq
```

---

## 🧭 Dashboard Example

The Grafana dashboard includes panels for:

- **Top 5 Performing Cryptocurrencies (24h Gain)**
- **Candlestick (Kline) Chart**
- **Price & Volume Trend**
- **Recent Trades Stream**
- **Market Summary**

---

## 🧑‍💻 Author

**John Kioko**  
💼 Data Engineer / Fullstack Developer  
🌐 [LinkedIn](https://www.linkedin.com/in/john-kioko-a25182174/) | [GitHub](https://github.com/Mutinda-Kioko)

---

## 📝 License

This project is licensed under the **MIT License** — feel free to fork, modify, and adapt for your own use.

---

Happy Trading & Streaming! 🚀
