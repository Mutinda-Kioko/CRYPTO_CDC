# Delete old connector if exists
# curl -X DELETE http://localhost:8083/connectors/postgres-binance-source

# Create new connector with correct publication name
curl -X POST -H "Content-Type: application/json" http://localhost:8083/connectors \
  -d '{
    "name": "postgres-binance-source",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "host.docker.internal",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "2275",
      "database.dbname": "binance_data",
      "database.server.name": "localhost",
      "topic.prefix": "localhost",
      "table.include.list": "public.symbols,public.latest_prices,public.ticker_24h,public.klines,public.recent_trades",
      "plugin.name": "pgoutput",
      "publication.name": "crypto_pub",
      "slot.name": "binance_slot",
      "snapshot.mode": "initial",
      "publication.autocreate.mode": "disabled",
      "transforms": "unwrap",
      "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
      "transforms.unwrap.drop.tombstones": "false",
      "transforms.unwrap.delete.handling.mode": "rewrite",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      "decimal.handling.mode": "string",
      "time.precision.mode": "connect"
    }
  }'

# Check status
# curl http://localhost:8083/connectors/postgres-binance-source/status | jq